package util

import (
	"fmt"
	"hash/maphash"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/internal/util"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow"
)

var hashSeed = maphash.MakeSeed()

// XXX add a HashEq<ArrowType> struct with both hash and compare functions?

// ----------------------------------------------------------------------
// An open-addressing insert-only hash table (no deletes)

const kSentinel = uint64(0)
const kLoadFactor = uint64(2)

// type interface{} interface{}

type HashTableEntry struct {
	h       uint64
	payload interface{}
}

// An entry is valid if the hash is different from the sentinel value
func (e HashTableEntry) bool() bool { return e.h != kSentinel }

type HashTableCmpFunc func(interface{}) bool

type HashTable interface {
	// Lookup with non-linear probing
	// cmp_func should have signature bool(const Payload*).
	// Return a (Entry*, found) pair.
	Lookup(h uint64, cmpFunc HashTableCmpFunc) (*HashTableEntry, bool)
	Insert(entry *HashTableEntry, h uint64, payload interface{}) error
	Size() uint64
	VisitEntries(visitFunc func(*HashTableEntry))
}

type CompareKind int

const (
	_ CompareKind = iota
	HashTable_CompareKind_DoCompare
	HashTable_CompareKind_NoCompare
)

type hashTable struct {
	// The number of slots available in the hash table array.
	capacity     uint64
	capacityMask uint64
	// The number of used slots in the hash table array.
	size uint64

	entriesBuilder entryBufferBuilder
}

func NewHashTable(pool memory.Allocator, capacity uint64) *hashTable {
	debug.Assert(pool != nil, "Assert: pool != nil")
	ht := &hashTable{}
	// Minimum of 32 elements
	capacity = util.MaxUint64(capacity, uint64(32))
	ht.capacity = uint64(bitutil.NextPowerOf2(int(capacity)))
	ht.capacityMask = ht.capacity - 1
	ht.size = 0

	ht.UpsizeBuffer(ht.capacity)
	return ht
}

func (ht *hashTable) Size() uint64 {
	return ht.size
}

func (ht *hashTable) NeedUpsizing() bool {
	// Keep the load factor <= 1/2
	return ht.size*kLoadFactor >= ht.capacity
}

func (ht *hashTable) UpsizeBuffer(capacity uint64) {
	ht.entriesBuilder.Resize(int(capacity))

	// TODO(nickpoorman): Do we need to memset this to zero?
	// entries := ht.entriesBuilder.Values()
	// for i := range entries {
	// 	entries[i] = &HashTableEntry{}
	// }
}

func (ht *hashTable) Upsize(newCapacity uint64) error {
	if newCapacity <= ht.capacity {
		return fmt.Errorf("newCapacity must be greater than current capacity")
	}
	newMask := newCapacity - 1
	if (newCapacity & newMask) != 0 {
		return fmt.Errorf("must be a power of two")
	}

	// Stash old entries and seal builder, effectively resetting the Buffer
	oldEntries := ht.entries()
	_ = ht.entriesBuilder.Finish()
	// Allocate new buffer
	ht.UpsizeBuffer(newCapacity)

	for _, entry := range oldEntries {
		if entry.bool() {
			// Dummy compare function will not be called
			pFirst, pSecond := ht.lookup(HashTable_CompareKind_NoCompare, entry.h, ht.entries(), newMask,
				func(payload interface{}) bool { return false })
			// Lookup<NoCompare> (and CompareEntry<NoCompare>) ensure that an
			// empty slots is always returned
			if pSecond {
				return fmt.Errorf("emply slot was not returned")
			}
			fmt.Printf("setting entry: %v\n", entry)
			ht.entries()[pFirst] = entry
		}
	}
	ht.capacity = newCapacity
	ht.capacityMask = newMask

	return nil
}

func (ht *hashTable) entries() []HashTableEntry {
	return ht.entriesBuilder.Values()
}

func (ht *hashTable) Lookup(h uint64, cmpFunc HashTableCmpFunc) (*HashTableEntry, bool) {
	pFirst, pSecond := ht.lookup(HashTable_CompareKind_DoCompare, h, ht.entries(), ht.capacityMask, cmpFunc)
	return &ht.entries()[pFirst], pSecond
}

// The workhorse lookup function
func (ht *hashTable) lookup(cKind CompareKind, h uint64, entries []HashTableEntry, sizeMask uint64, cmpFunc HashTableCmpFunc) (uint64, bool) {
	const perturbShift uint8 = 5

	var index uint64
	var perturb uint64

	h = ht.fixHash(h)
	index = uint64(h) & sizeMask
	perturb = (uint64(h) >> perturbShift) + uint64(1)

	for {
		entry := entries[index]
		if ht.CompareEntry(cKind, h, entry, cmpFunc) {
			// Found
			return index, true
		}
		if entry.h == kSentinel {
			// Empty slot
			return index, false
		}

		// Perturbation logic inspired from CPython's set / dict object.
		// The goal is that all 64 bits of the unmasked hash value eventually
		// participate in the probing sequence, to minimize clustering.
		index = (index + perturb) & sizeMask
		perturb = (perturb >> perturbShift) + 1
	}
}

func (*hashTable) fixHash(h uint64) uint64 {
	if h == uint64(kSentinel) {
		return uint64(42)
	}
	return h
}

func (*hashTable) CompareEntry(cKind CompareKind, h uint64, entry HashTableEntry, cmpFunc HashTableCmpFunc) bool {
	if cKind == HashTable_CompareKind_NoCompare {
		return false
	}
	return entry.h == h && cmpFunc(entry.payload)
}

// Insert takes a pointer to an entry that it will set.
func (ht *hashTable) Insert(entry *HashTableEntry, h uint64, payload interface{}) error {
	if entry.bool() {
		return fmt.Errorf("Insert: Entry must be empty before inserting: %+v", entry)
	}
	entry.h = ht.fixHash(h)
	entry.payload = payload
	ht.size++

	if ht.NeedUpsizing() {
		// Resize less frequently since it is expensive
		return ht.Upsize(ht.capacity * kLoadFactor * 2)
	}
	return nil
}

// Visit all non-empty entries in the table
// The visit_func should have signature func(*HashTableEntry)
func (ht *hashTable) VisitEntries(visitFunc func(*HashTableEntry)) {
	entries := ht.entries()
	for i := uint64(0); i < ht.capacity; i++ {
		entry := entries[i]
		if entry.bool() {
			visitFunc(&entry)
		}
	}
}

// Type specific class for a buffer builder to hold Entry structs.
type entryBufferBuilder struct {
	// TODO(nickpoorman): Use Arrow TypedBufferBuilder instead of native array.
	// array.BufferBuilder
	entries []HashTableEntry
}

func (b *entryBufferBuilder) Resize(elements int) {
	enteries := make([]HashTableEntry, elements)
	copy(enteries, b.entries)
	b.entries = enteries
}

func (b *entryBufferBuilder) Values() []HashTableEntry {
	return b.entries
}

// Reset and return the buffer
func (b *entryBufferBuilder) Finish() *memory.Buffer {
	b.entries = make([]HashTableEntry, 0)
	return nil
}

const kKeyNotFound = -1

func OnFoundNoOp(memoIndex int32)    {}
func OnNotFoundNoOp(memoIndex int32) {}

// ----------------------------------------------------------------------
// A base class for memoization table.

type MemoTable interface {
	Size() int32
	Get(value arrow.Scalar) (int32, error)
	GetOrInsert(value arrow.Scalar, onFound func(int32), onNotFound func(int32)) (int32, error)
	GetNull() int32
	GetOrInsertNull(onFound func(int32), onNotFound func(int32)) int32
	CopyValues(start int32, outSize int64, outData interface{})
}

// ----------------------------------------------------------------------
// A memoization table for memory-cheap scalar values.

// The memoization table remembers and allows to look up the insertion
// index for each key.

type scalarPayload struct {
	value     arrow.Scalar
	memoIndex int32
}

func newScalarPayload(value arrow.Scalar, memoIndex int32) *scalarPayload {
	return &scalarPayload{
		value:     value,
		memoIndex: memoIndex,
	}
}

type ScalarMemoTable struct {
	hashTable HashTable
	nullIndex int32 // default: kKeyNotFound
}

func NewScalarMemoTable(pool memory.Allocator, entries int /* default: 0 */) *ScalarMemoTable {
	return &ScalarMemoTable{
		hashTable: NewHashTable(pool, uint64(entries)),
		nullIndex: kKeyNotFound,
	}
}

// The number of entries in the memo table +1 if null was added.
// (which is also 1 + the largest memo index)
func (s *ScalarMemoTable) Size() int32 {
	nullAdded := int32(0)
	if s.GetNull() != kKeyNotFound {
		nullAdded = 1
	}
	return int32(s.hashTable.Size()) + nullAdded
}

func (s *ScalarMemoTable) Get(value arrow.Scalar) (int32, error) {
	cmpFunc := func(payload interface{}) bool {
		return CompareScalars(payload.(*scalarPayload).value, value)
	}
	h, err := s.ComputeHash(value)
	if err != nil {
		return 0, err
	}
	hashTableEntry, ok := s.hashTable.Lookup(h, cmpFunc)
	if ok {
		return hashTableEntry.payload.(*scalarPayload).memoIndex, nil
	} else {
		return kKeyNotFound, nil
	}
}

func (s *ScalarMemoTable) GetOrInsert(
	value arrow.Scalar, onFound func(int32), onNotFound func(int32)) (int32, error) {

	cmpFunc := func(payload interface{}) bool {
		return CompareScalars(value, payload.(*scalarPayload).value)
	}
	h, err := s.ComputeHash(value)
	if err != nil {
		return 0, err
	}
	hashTableEntry, ok := s.hashTable.Lookup(h, cmpFunc)
	var memoIndex int32
	if ok {
		memoIndex = hashTableEntry.payload.(*scalarPayload).memoIndex
		if onFound != nil {
			onFound(memoIndex)
		}
	} else {
		memoIndex = s.Size()
		if err := s.hashTable.Insert(
			hashTableEntry, h, newScalarPayload(value, memoIndex)); err != nil {
			return memoIndex, err
		}
		if onNotFound != nil {
			onNotFound(memoIndex)
		}
	}
	return memoIndex, nil
}

func (s *ScalarMemoTable) GetNull() int32 {
	return s.nullIndex
}

func (s *ScalarMemoTable) GetOrInsertNull(
	onFound func(int32), onNotFound func(int32)) int32 {

	memoIndex := s.GetNull()
	if memoIndex != kKeyNotFound {
		if onFound != nil {
			onFound(memoIndex)
		}
	} else {
		memoIndex = s.Size()
		s.nullIndex = memoIndex
		if onNotFound != nil {
			onNotFound(memoIndex)
		}
	}
	return memoIndex
}

func (*ScalarMemoTable) ComputeHash(value arrow.Scalar) (uint64, error) {
	return ScalarComputeHash(value)
}

// Copy values starting from index `start` into `outData`.
// Check the size in debug mode.
func (s *ScalarMemoTable) CopyValues(start int32, outSize int64, outData interface{}) {
	outDataScalars := outData.([]arrow.Scalar)
	s.copyValues(start, outSize, outDataScalars)
}

// Copy values starting from index `start` into `outData`.
// Check the size in debug mode.
func (s *ScalarMemoTable) copyValues(start int32, outSize int64, outData []arrow.Scalar) {
	s.hashTable.VisitEntries(func(entry *HashTableEntry) {
		index := entry.payload.(*scalarPayload).memoIndex - start
		if index >= 0 {
			outData[index] = entry.payload.(*scalarPayload).value
		}
	})
}

// ----------------------------------------------------------------------
// A memoization table for small scalar values, using direct indexing

const smallScalarMemoTableMaxCardinality = 256

type SmallScalarMemoTable struct {
	hashTable HashTable

	// The last index is reserved for the null element.
	valueToIndex [smallScalarMemoTableMaxCardinality + 1]int32 // max cardinality is 256
	indexToValue []arrow.Scalar
}

func NewSmallScalarMemoTable(pool memory.Allocator, entries int /* default: 0 */) *SmallScalarMemoTable {
	table := &SmallScalarMemoTable{
		hashTable:    NewHashTable(pool, uint64(entries)),
		indexToValue: make([]arrow.Scalar, 0, smallScalarMemoTableMaxCardinality),
	}
	for i := range table.valueToIndex {
		table.valueToIndex[i] = kKeyNotFound
	}

	return table
}

func (s *SmallScalarMemoTable) Get(value arrow.Scalar) (int32, error) {
	return s.valueToIndex[s.AsIndex(value)], nil
}

func (*SmallScalarMemoTable) AsIndex(value arrow.Scalar) uint32 {
	switch v := value.(type) {
	case interface{ ValueUint32() uint32 }:
		return v.ValueUint32()
	default:
		// only integral types are supported
		panic(fmt.Errorf("AsIndex ValueUint32() unsupported type: %T", value))
	}
}

func (s *SmallScalarMemoTable) GetNull() int32 {
	return s.valueToIndex[smallScalarMemoTableMaxCardinality]
}

func (s *SmallScalarMemoTable) GetOrInsert(
	value arrow.Scalar, onFound func(int32), onNotFound func(int32)) (int32, error) {

	valueIndex := s.AsIndex(value)
	memoIndex := s.valueToIndex[valueIndex]
	if memoIndex == kKeyNotFound {
		memoIndex = int32(len(s.indexToValue))
		s.indexToValue = append(s.indexToValue, value)
		s.valueToIndex[valueIndex] = memoIndex
		debug.Assert(memoIndex < smallScalarMemoTableMaxCardinality+1,
			fmt.Sprintf(
				"Assert: %d < %d", memoIndex, smallScalarMemoTableMaxCardinality+1))
		if onNotFound != nil {
			onNotFound(memoIndex)
		}
	} else {
		if onFound != nil {
			onFound(memoIndex)
		}
	}
	return memoIndex, nil
}

func (s *SmallScalarMemoTable) GetOrInsertNull(
	onFound func(int32), onNotFound func(int32)) int32 {

	memoIndex := s.GetNull()
	if memoIndex == kKeyNotFound {
		memoIndex = s.Size()
		s.valueToIndex[smallScalarMemoTableMaxCardinality] = memoIndex
		s.indexToValue = append(s.indexToValue, arrow.NewNullScalar(nil))
		if onNotFound != nil {
			onNotFound(memoIndex)
		}
	} else {
		if onFound != nil {
			onFound(memoIndex)
		}
	}
	return memoIndex
}

// The number of entries in the memo table
// (which is also 1 + the largest memo index)
func (s *SmallScalarMemoTable) Size() int32 {
	return int32(len(s.indexToValue))
}

// Copy values starting from index `start` into `out_data`
// Check the size in debug mode.
func (s *SmallScalarMemoTable) CopyValues(start int32, outSize int64, outData interface{}) {
	debug.AssertGE(int(start), int(0))
	debug.AssertLE(int(start), len(s.indexToValue))
	outDataScalars := outData.([]arrow.Scalar)
	copy(outDataScalars, s.indexToValue[start:])
}

func ScalarComputeHash(scalar arrow.Scalar) (uint64, error) {
	h1 := new(maphash.Hash)
	h1.SetSeed(hashSeed)
	_, err := h1.Write(scalar.ValueBytes())
	if err != nil {
		return 0, err
	}
	return h1.Sum64(), nil
}

func ScalarComputeStringHash(data []byte) (uint64, error) {
	// TODO(nickpoorman): Try implementing the C++ algorithm and benchmark for speed
	h1 := new(maphash.Hash)
	h1.SetSeed(hashSeed)
	_, err := h1.Write(data)
	if err != nil {
		return 0, err
	}
	return h1.Sum64(), nil
}

func CompareScalars(left, right arrow.Scalar) bool {
	if arrow.ScalarIsNaN(left) {
		// XXX should we do a bit-precise comparison?
		return arrow.ScalarIsNaN(right)
	}
	return left.Equals(right)
}

var (
	_ MemoTable = (*ScalarMemoTable)(nil)
	_ MemoTable = (*SmallScalarMemoTable)(nil)
)
