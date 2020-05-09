package util

import (
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/internal/util"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

type hash_t uint64

// XXX would it help to have a 32-bit hash value on large datasets?
// type hash_t uint64

// Notes about the choice of a hash function.
// - XXH3 is extremely fast on most data sizes, from small to huge;
//   faster even than HW CRC-based hashing schemes
// - our custom hash function for tiny values (< 16 bytes) is still
//   significantly faster (~30%), at least on this machine and compiler

func ComputeStringHash(data interface{}, length int64) hash_t {
	if length <= 16 {
		// Specialize for small hash strings, as they are quite common as
		// hash table keys.  Even XXH3 isn't quite as fast.
		// p := data.([]byte)
	}

	panic("not implemented")
}

// XXX add a HashEq<ArrowType> struct with both hash and compare functions?

// ----------------------------------------------------------------------
// An open-addressing insert-only hash table (no deletes)

const kSentinel = uint64(0)
const kLoadFactor = uint64(2)

// type interface{} interface{}

type HashTableEntry struct {
	h       hash_t
	payload interface{}
}

func (e HashTableEntry) bool() bool { return uint64(e.h) != kSentinel }

type HashTableCmpFunc func(interface{}) bool

type HashTable interface {
	// Lookup with non-linear probing
	// cmp_func should have signature bool(const Payload*).
	// Return a (Entry*, found) pair.
	Lookup(h hash_t, cmpFunc HashTableCmpFunc) (*HashTableEntry, bool)
	Insert(entry *HashTableEntry, h hash_t, payload interface{}) error
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

func (ht *hashTable) Lookup(h hash_t, cmpFunc HashTableCmpFunc) (*HashTableEntry, bool) {
	pFirst, pSecond := ht.lookup(HashTable_CompareKind_DoCompare, h, ht.entries(), ht.capacityMask, cmpFunc)
	return ht.entries()[pFirst], pSecond
}

// The workhorse lookup function
func (ht *hashTable) lookup(cKind CompareKind, h hash_t, entries []*HashTableEntry, sizeMask uint64, cmpFunc HashTableCmpFunc) (uint64, bool) {
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

func (*hashTable) fixHash(h hash_t) hash_t {
	if h == hash_t(kSentinel) {
		return hash_t(42)
	}
	return h
}

func (*hashTable) CompareEntry(cKind CompareKind, h hash_t, entry *HashTableEntry, cmpFunc HashTableCmpFunc) bool {
	if cKind == HashTable_CompareKind_NoCompare {
		return false
	}
	return entry.h == h && cmpFunc(entry.payload)
}

func (ht *hashTable) Insert(entry *HashTableEntry, h hash_t, payload interface{}) error {
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
	enteries := make([]*HashTableEntry, elements, elements)
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

type ScalarHelperTemplate struct {
	Scalar
}

// var ScalarHelpers map[]

type Scalar interface{}

func ScalarToUint64(s Scalar) uint64 {
	switch v := s.(type) {
	case bool:
		if v {
			return 1
		}
		return 0
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	default:
		panic(fmt.Sprintf("ScalarToUint64: unknown Scalar type: %T", s))
	}
}

func ScalarToFloat64(s Scalar) float64 {
	switch v := s.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		panic(fmt.Sprintf("ScalarToFloat64: unknown Scalar type: %T", s))
	}
}

type ScalarHelperBase struct {
	Scalar Scalar
	AlgNum int
}

func (s *ScalarHelperBase) CompareScalars(u Scalar, v Scalar) bool {
	// switch s.Scalar.(type) {
	// 	// ScalarHelper specialization for reals
	// if math.IsNaN(ScalarToFloat64(u)) {
	// 	// XXX should we do a bit-precise comparison?
	// 	return math.IsNaN(ScalarToFloat64(v))
	// }
	// return u == v
	// }
	// return u == v
	panic("not implemented")
}

func (s *ScalarHelperBase) ComputeHash(value *Scalar) hash_t {
	// Generic hash computation for scalars.  Simply apply the string hash
	// to the bit representation of the value.

	// XXX in the case of FP values, we'd like equal values to have the same hash,
	// even if they have different bit representations...
	return ComputeStringHash(value, int64(binary.Size(value)))
}

type ScalarHelper struct {
	ScalarHelperBase
}

func NewScalarHelper(algNum int, scalar Scalar) *ScalarHelper {
	sh := &ScalarHelper{
		ScalarHelperBase: ScalarHelperBase{
			Scalar: scalar,
			AlgNum: algNum,
		},
	}
	return sh
}

func (s *ScalarHelper) ComputeHash(value *Scalar) hash_t {
	if isIntegral(*value) {
		// Faster hash computation for integers.
		return s.computeHashIntegral(*value)
	}
	if isFloatingPoint(value) {
		// ScalarHelper specialization for reals
		// return s.computeHashReals(*value)
		panic("not implemented... is this even a thing?")
	}

	// default and string type
	return s.ScalarHelperBase.ComputeHash(value)
}

func (s *ScalarHelper) computeHashIntegral(value Scalar) hash_t {
	// Faster hash computation for integers.
	// Two of xxhash's prime multipliers (which are chosen for their
	// bit dispersion properties)
	var multipliers = [...]uint64{11400714785074694791, 14029467366897019727}

	// Multiplying by the prime number mixes the low bits into the high bits,
	// then byte-swapping (which is a single CPU instruction) allows the
	// combined high and low bits to participate in the initial hash table index.
	h := ScalarToUint64(value)
	return hash_t(bitutilext.ByteSwap64(multipliers[s.AlgNum] * h))
}

func isIntegral(value interface{}) bool {
	switch value.(type) {
	case bool, uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return true
	default:
		return false
	}
}

func (s *ScalarHelper) computeHashReals(u Scalar, v Scalar) bool {
	panic("not implemented")
}

func isFloatingPoint(value interface{}) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

type SCALAR_TYPE_PLACEHOLDER interface{}

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
	value     Scalar
	memoIndex int32
}

func newScalarPayload(value Scalar, memoIndex int32) *scalarPayload {
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

func (s *ScalarMemoTable) Get(value Scalar) int32 {
	cmpFunc := func(payload interface{}) bool {
		return NewScalarHelperSCALAR_TYPE_PLACEHOLDER().
			CompareScalars(payload.(*scalarPayload).value, value)
	}
	h := s.ComputeHash(value)
	hashTableEntry, ok := s.hashTable.Lookup(h, cmpFunc)
	if ok {
		return hashTableEntry.payload.(*scalarPayload).memoIndex
	} else {
		return kKeyNotFound
	}
}

func (s *ScalarMemoTable) GetOrInsert(value Scalar, onFound Func1, onNotFound Func2, outMemoIndex int32) (int32, error) {
	cmpFunc := func(payload interface{}) bool {
		return NewScalarHelperSCALAR_TYPE_PLACEHOLDER().
			CompareScalars(value, payload.(*scalarPayload).value)
	}
	h := s.ComputeHash(value)
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

func (ScalarMemoTable) ComputeHash(value Scalar) hash_t {
	return NewScalarHelperSCALAR_TYPE_PLACEHOLDER().ComputeHash(value)
}

// Copy values starting from index `start` into `outData`.
// Check the size in debug mode.
func (s *ScalarMemoTable) CopyValues(start int32, outSize int64, outData interface{}) {
	outDataScalars := outData.([]SCALAR_TYPE_PLACEHOLDER)
	s.copyValues(start, outSize, outDataScalars)
}

// Copy values starting from index `start` into `outData`.
// Check the size in debug mode.
func (s *ScalarMemoTable) copyValues(start int32, outSize int64, outData []SCALAR_TYPE_PLACEHOLDER) {
	s.hashTable.VisitEntries(func(entry *HashTableEntry) {
		index := entry.payload.(*scalarPayload).memoIndex - start
		if index >= 0 {
			outData[index] = entry.payload.(*scalarPayload).value
		}
	})
}
