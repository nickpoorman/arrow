package util

import (
	"bytes"
	"fmt"
	"hash/maphash"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/internal/util"
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

func NewMemoTable(pool memory.Allocator, entries int /* default: 0 */, dataType arrow.DataType) MemoTable {
	switch dt := dataType.(type) {
	case arrow.BinaryDataType:
		valueSize := -1
		switch dt := dataType.(type) {
		case interface{ BitWidth() int }:
			valueSize = dt.BitWidth() >> 3
		}
		return NewBinaryMemoTable(pool, entries, valueSize, dt)
	case arrow.DataType:
		bitWidth := -1
		switch dt := dataType.(type) {
		case interface{ BitWidth() int }:
			bitWidth = dt.BitWidth()
		}
		if bitWidth != -1 && bitWidth <= 8 {
			return NewSmallScalarMemoTable(pool, entries)
		} else {
			return NewScalarMemoTable(pool, entries)
		}
	default:
		panic(fmt.Sprintf("unknown DataType: %T", dataType))
	}
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
	var nullAdded int32
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
	byteOffset := 0
	s.hashTable.VisitEntries(func(entry *HashTableEntry) {
		index := entry.payload.(*scalarPayload).memoIndex - start
		if index >= 0 {
			switch outTyped := outData.(type) {
			case []byte:
				scalar := entry.payload.(*scalarPayload).value
				byteOffset += scalar.PutValue(outTyped[int(index)*scalar.ValueSize():])
			case []arrow.Scalar:
				outTyped[index] = entry.payload.(*scalarPayload).value
			default:
				panic(fmt.Errorf("ScalarMemoTable.CopyValues() unsupported outData type: %T", outData))
			}
		}
	})
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
	switch outTyped := outData.(type) {
	case []byte:
		arrow.ScalarCopyValues(s.indexToValue[start:], outTyped)
	case []arrow.Scalar:
		copy(outTyped, s.indexToValue[start:])
	default:
		panic(fmt.Errorf("SmallScalarMemoTable.CopyValues() unsupported outData type: %T", outData))
	}
}

// ----------------------------------------------------------------------
// A memoization table for variable-sized binary data.

type binaryPayload struct {
	memoIndex int32
}

func newBinaryPayload(memoIndex int32) *binaryPayload {
	return &binaryPayload{
		memoIndex: memoIndex,
	}
}

type BinaryMemoTable struct {
	hashTable HashTable

	nullIndex     int32
	binaryBuilder *array.BinaryBuilder
}

func NewBinaryMemoTable(pool memory.Allocator, entries int, /* default: 0 */
	valuesSize int, /* default: -1 */
	dataType arrow.BinaryDataType) *BinaryMemoTable {

	table := &BinaryMemoTable{
		hashTable:     NewHashTable(pool, uint64(entries)),
		binaryBuilder: array.NewBinaryBuilder(pool, dataType),
		nullIndex:     kKeyNotFound,
	}
	dataSize := valuesSize
	if valuesSize < 0 {
		dataSize = entries * 4
	}
	table.binaryBuilder.Resize(entries)
	table.binaryBuilder.ReserveData(dataSize)
	return table
}

func (t *BinaryMemoTable) Get(value arrow.Scalar) (int32, error) {
	data := value.ValueBytes()
	h, err := ScalarComputeStringHash(data)
	if err != nil {
		return 0, err
	}
	hashTableEntry, ok := t.lookup(h, data)
	if ok {
		return hashTableEntry.payload.(*binaryPayload).memoIndex, nil
	} else {
		return kKeyNotFound, nil
	}
}

func (t *BinaryMemoTable) GetOrInsert(
	value arrow.Scalar, onFound func(int32), onNotFound func(int32)) (int32, error) {

	data := value.ValueBytes()
	h, err := ScalarComputeStringHash(data)
	if err != nil {
		return 0, err
	}
	hashTableEntry, ok := t.lookup(h, data)
	var memoIndex int32
	if ok {
		memoIndex = hashTableEntry.payload.(*binaryPayload).memoIndex
		if onFound != nil {
			onFound(memoIndex)
		}
	} else {
		memoIndex = t.Size()
		// Insert string value
		t.binaryBuilder.Append(data)
		// Insert hash entry
		t.hashTable.Insert(hashTableEntry, h, newBinaryPayload(memoIndex))

		if onNotFound != nil {
			onNotFound(memoIndex)
		}
	}
	return memoIndex, nil
}

func (t *BinaryMemoTable) lookup(h uint64, data []byte) (*HashTableEntry, bool) {
	cmpFunc := func(payload interface{}) bool {
		lhs := t.binaryBuilder.Value(int(payload.(*binaryPayload).memoIndex))
		return bytes.Equal(lhs, data)
	}
	return t.hashTable.Lookup(h, cmpFunc)
}

func (t *BinaryMemoTable) GetNull() int32 {
	return t.nullIndex
}

func (t *BinaryMemoTable) GetOrInsertNull(
	onFound func(int32), onNotFound func(int32)) int32 {

	memoIndex := t.GetNull()
	if memoIndex != kKeyNotFound {
		if onFound != nil {
			onFound(memoIndex)
		}
	} else {
		memoIndex = t.Size()
		t.nullIndex = memoIndex
		t.binaryBuilder.AppendNull()
		if onNotFound != nil {
			onNotFound(memoIndex)
		}
	}
	return memoIndex
}

// The number of entries in the memo table
// (which is also 1 + the largest memo index)
func (t *BinaryMemoTable) Size() int32 {
	var nullAdded int32
	if t.GetNull() != kKeyNotFound {
		nullAdded = 1
	}
	return int32(t.hashTable.Size()) + nullAdded
}

func (t *BinaryMemoTable) ValuesSize() int {
	return t.binaryBuilder.DataLen()
}

// Copy (n + 1) offsets starting from index `start` into `out_data`
func (t *BinaryMemoTable) CopyOffsets(start int32, outSize int64, outData interface{}) {
	debug.AssertLT(start, t.Size())
	switch v := outData.(type) {
	case []int32:
		t.copyOffsetsInt32(start, outSize, v)
	default:
		panic(fmt.Errorf("BinaryMemoTable.CopyOffsets() unsupported outData type: %T", outData))
	}
}

func (t *BinaryMemoTable) copyOffsetsInt32(start int32, outSize int64, outData []int32) {
	outDataOffset := 0
	delta := t.binaryBuilder.Offset(int(start))
	for i := start; i < t.Size(); i++ {
		adjOffset := t.binaryBuilder.Offset(int(i)) - delta
		outData[outDataOffset] = adjOffset
		outDataOffset++
	}

	// Copy last value since BinaryBuilder only materializes it on in Finish()
	outData[outDataOffset] = int32(t.binaryBuilder.DataLen() - int(delta))
}

// Copy values starting from index `start` into `outData`.
// Check the output size in debug mode.
func (t *BinaryMemoTable) CopyValues(start int32, outSize int64, outData interface{}) {
	switch v := outData.(type) {
	case []byte:
		t.copyValuesBytes(start, outSize, v)
	default:
		panic(fmt.Errorf("BinaryMemoTable.CopyValues() unsupported outData type: %T", outData))
	}
}

// Copy values starting from index `start` into `outData`.
// Check the size in debug mode.
func (t *BinaryMemoTable) copyValuesBytes(start int32, outSize int64, outData []byte) {
	debug.AssertLE(start, t.Size())

	// The absolute byte offset of `start` value in the binary buffer.
	offset := t.binaryBuilder.Offset(int(start))
	length := t.binaryBuilder.DataLen() - int(offset)

	if outSize != -1 {
		debug.AssertLE(int64(length), outSize)
	}

	view := t.binaryBuilder.Value(int(start))
	copy(outData, view[:length])
}

func (t *BinaryMemoTable) CopyFixedWidthValues(start int32, widthSize int32, outSize int64, outData []byte) {
	panic("not yet tested") // TODO(nickpoorman): Need to test this because it probably isn't correct

	// This method exists to cope with the fact that the BinaryMemoTable does
	// not know the fixed width when inserting the null value. The data
	// buffer hold a zero length string for the null value (if found).
	//
	// Thus, the method will properly inject an empty value of the proper width
	// in the output buffer.
	//
	if start >= t.Size() {
		return
	}

	nullIndex := t.GetNull()
	if nullIndex < start {
		// Nothing to skip, proceed as usual.
		t.CopyValues(start, outSize, outData)
		return
	}

	leftOffset := t.binaryBuilder.Offset(int(start))

	// Ensure that the data length is exactly missing width_size bytes to fit
	// in the expected output (n_values * width_size).
	debug.Assert(int64((t.ValuesSize()-int(leftOffset))+int(widthSize)) == outSize,
		fmt.Sprintf("Assert: %d == %d", int64((t.ValuesSize()-int(leftOffset))+int(widthSize)), outSize))

	inData := t.binaryBuilder.Value(int(start))
	// The null use 0-length in the data, slice the data in 2 and skip by
	// width_size in out_data. [part_1][width_size][part_2]
	nullDataOffset := t.binaryBuilder.Offset(int(nullIndex))
	leftSize := nullDataOffset - leftOffset
	if leftSize > 0 {
		copy(outData, inData[:leftSize])
	}

	rightSize := t.ValuesSize() - int(nullDataOffset)
	if rightSize > 0 {
		// skip the null fixed size value.
		outOffset := int(leftSize + widthSize)
		debug.Assert(&outData[outOffset+rightSize:][0] == &outData[outSize:][0],
			fmt.Sprintf("Assert: %p == %p", &outData[outOffset+rightSize:][0], &outData[outSize:][0]))
		copy(outData[outOffset:], inData[:rightSize])
	}
}

// Visit the stored values in insertion order.
// The visitor function should have the signature `func([]byte))`.
func (t *BinaryMemoTable) VisitValues(start int32, visit func([]byte)) {
	size := int(t.Size())
	for i := int(start); i < size; i++ {
		visit(t.binaryBuilder.Value(i))
	}
}

var (
	_ MemoTable = (*ScalarMemoTable)(nil)
	_ MemoTable = (*SmallScalarMemoTable)(nil)
	_ MemoTable = (*BinaryMemoTable)(nil)
)
