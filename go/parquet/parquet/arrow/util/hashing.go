package util

import (
	"fmt"
	"hash/maphash"
	"math"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/internal/util"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow"
)

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

func (e HashTableEntry) bool() bool { return uint64(e.h) != kSentinel }

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

func NewHashTable(pool *memory.Allocator, capacity uint64) *hashTable {
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
		if entry != nil {
			// Dummy compare function will not be called
			pFirst, pSecond := ht.lookup(HashTable_CompareKind_NoCompare, entry.h, ht.entries(), newMask,
				func(payload interface{}) bool { return false })
			// Lookup<NoCompare> (and CompareEntry<NoCompare>) ensure that an
			// empty slots is always returned
			if pSecond {
				return fmt.Errorf("emply slot was not returned")
			}
			ht.entries()[pFirst] = entry
		}
	}
	ht.capacity = newCapacity
	ht.capacityMask = newMask

	return nil
}

func (ht *hashTable) entries() []*HashTableEntry {
	return ht.entriesBuilder.Values()
}

func (ht *hashTable) Lookup(h uint64, cmpFunc HashTableCmpFunc) (*HashTableEntry, bool) {
	pFirst, pSecond := ht.lookup(HashTable_CompareKind_DoCompare, h, ht.entries(), ht.capacityMask, cmpFunc)
	return ht.entries()[pFirst], pSecond
}

// The workhorse lookup function
func (ht *hashTable) lookup(cKind CompareKind, h uint64, entries []*HashTableEntry, sizeMask uint64, cmpFunc HashTableCmpFunc) (uint64, bool) {
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
		if uint64(entry.h) == kSentinel {
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

func (*hashTable) CompareEntry(cKind CompareKind, h uint64, entry *HashTableEntry, cmpFunc HashTableCmpFunc) bool {
	if cKind == HashTable_CompareKind_NoCompare {
		return false
	}
	return entry.h == h && cmpFunc(entry.payload)
}

func (ht *hashTable) Insert(entry *HashTableEntry, h uint64, payload interface{}) error {
	if entry == nil {
		return fmt.Errorf("Insert: entry is nil")
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
	for i := uint64(0); i < ht.capacity; i++ {
		entry := ht.entries()[i]
		if entry != nil {
			visitFunc(entry)
		}
	}
}

// Type specific class for a buffer builder to hold Entry structs.
type entryBufferBuilder struct {
	// TODO(nickpoorman): Use Arrow TypedBufferBuilder instead of native array.
	// array.BufferBuilder
	entries []*HashTableEntry
}

func (b *entryBufferBuilder) Resize(elements int) {
	enteries := make([]*HashTableEntry, elements)
	copy(enteries, b.entries)
	b.entries = enteries
}

func (b *entryBufferBuilder) Values() []*HashTableEntry {
	return b.entries
}

// Reset and return the buffer
func (b *entryBufferBuilder) Finish() *memory.Buffer {
	b.entries = make([]*HashTableEntry, 0)
	return nil
}

const kKeyNotFound = -1

func OnFoundNoOp(memoIndex int32)    {}
func OnNotFoundNoOp(memoIndex int32) {}

// ----------------------------------------------------------------------
// A base class for memoization table.

type MemoTable interface {
	Size() int32
	GetOrInsert(value interface{}, onFound func(memoIndex int32), onNotFound func(memoIndex int32), outMemoIndex *int32) error
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

func NewScalarMemoTable() *ScalarMemoTable {
	return &ScalarMemoTable{
		nullIndex: kKeyNotFound,
	}
}

// The number of entries in the memo table +1 if null was added.
// (which is also 1 + the largest memo index)
func (s ScalarMemoTable) size() int32 {
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
	value arrow.Scalar, onFound func(int32), onNotFound func(int32), outMemoIndex int32,
) (int32, error) {
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
		onFound(memoIndex)
	} else {
		memoIndex = s.size()
		if err := s.hashTable.Insert(
			hashTableEntry, h, newScalarPayload(value, memoIndex)); err != nil {
			return memoIndex, err
		}
		onNotFound(memoIndex)
	}
	return memoIndex, nil
}

func (s *ScalarMemoTable) GetNull() int32 {
	return s.nullIndex
}

func (ScalarMemoTable) ComputeHash(value arrow.Scalar) (uint64, error) {
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

func ScalarComputeHash(scalar arrow.Scalar) (uint64, error) {
	h1 := new(maphash.Hash)
	_, err := h1.Write(scalar.ValueBytes())
	if err != nil {
		return 0, err
	}
	return h1.Sum64(), nil
}

func CompareScalars(left, right arrow.Scalar) bool {
	if isFloatingPoint(left) && isFloatingPoint(right) {
		if math.IsNaN(scalarToFloat64(left)) {
			// XXX should we do a bit-precise comparison?
			return math.IsNaN(scalarToFloat64(right))
		}
	}
	return left.Equals(right)
}

func isFloatingPoint(scalar arrow.Scalar) bool {
	switch scalar.(type) {
	case *arrow.Float16Scalar, *arrow.Float32Scalar, *arrow.Float64Scalar:
		return true
	default:
		return false
	}
}

func scalarToFloat64(scalar arrow.Scalar) float64 {
	switch v := scalar.(type) {
	case *arrow.Float16Scalar:
		return float64(v.Value().Float32())
	case *arrow.Float32Scalar:
		return float64(v.Value())
	case *arrow.Float64Scalar:
		return v.Value()
	default:
		panic(fmt.Sprintf("scalarToFloat64: unknown Scalar type: %T", scalar))
	}
}
