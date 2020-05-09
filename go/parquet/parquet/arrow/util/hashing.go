package util

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
		p := data.([]byte)

	}
}


// XXX add a HashEq<ArrowType> struct with both hash and compare functions?

// ----------------------------------------------------------------------
// An open-addressing insert-only hash table (no deletes)
type HashTable interface {
	// Lookup with non-linear probing
	// cmp_func should have signature bool(const Payload*).
	// Return a (Entry*, found) pair.
	Lookup(h hash_t, cmpFunc CmpFunc) (Entry, bool)
	Insert(entry Entry, h hash_t, payload Payload) 
}

type hashTable struct {

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
	switch s.Scalar.(type) {
		// ScalarHelper specialization for reals
	if math.IsNaN(ScalarToFloat64(u)) {
		// XXX should we do a bit-precise comparison?
		return math.IsNaN(ScalarToFloat64(v))
	}
	return u == v
	}
	return u == v
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
		return s.computeHashReals(*value)
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
	return bitutilext.ByteSwap64(multipliers[s.algNum] * h)
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
	CopyValues(start int32, outSize int64, outData []byte)
}

// ----------------------------------------------------------------------
// A memoization table for memory-cheap scalar values.

// The memoization table remembers and allows to look up the insertion
// index for each key.

type Payload struct {
	value Scalar
	memoIndex int32
}

type ScalarMemoTable struct {
	hashTable HashTableType
	nullIndex int32 // default: kKeyNotFound
}

func NewScalarMemoTable() *ScalarMemoTable {
	return &ScalarMemoTable{
		nullIndex: kKeyNotFound,
	}
}

// The number of entries in the memo table +1 if null was added.
// (which is also 1 + the largest memo index)
func (s ScalarMemoTable) size(value Scalar) {
	return int32(s.hashTable.size()) + (s.GetNull() != kKeyNotFound)
}

func (ScalarMemoTable) Get(value Scalar) int32 {
	cmpFunc := func(payload *Payload) bool {
		return NewScalarHelperSCALAR_TYPE_PLACEHOLDER().CompareScalars(payload.value, value)
	}
	h := ComputeHash(value)
	payload, ok := hashTable.Lookup(h, cmpFunc)
	if ok {
		return payload.memoIndex
	} else {
		return kKeyNotFound
	}
}

func (s *ScalarMemoTable) GetOrInsert(value Scalar, onFound Func1, onNotFound Func2, outMemoIndex int32) error {
	cmpFunc := func(payload *Payload) bool {
		return NewScalarHelperSCALAR_TYPE_PLACEHOLDER().CompareScalars(value, payload.value)
	}
	h := ComputeHash(value)
	payload, ok := s.hashTable.Lookup(h, cmpFunc)
	var memoIndex int32
	if ok {
		memoIndex = payload.memoIndex
		onFound(memoIndex)
	} else {
		memoIndex = size()
		onNotFound(memoIndex)
	}
	return memoIndex
}

func (s *ScalarMemoTable) GetNull() int32 {
	return s.nullIndex
}

func (ScalarMemoTable) ComputeHash(value Scalar) hash_t {
	return NewScalarHelperSCALAR_TYPE_PLACEHOLDER().ComputeHash(value)
}

// Copy values starting from index `start` into `outData`.
// Check the size in debug mode.
func (s *ScalarMemoTable) CopyValues(start int32, outSize int64, outData []byte) {
	s.hashTable.VisitEntries(func (entry *HashTableEntry) {
		index := entry.payload.memoIndex - start
		if index >= 0 {
			outData[index] = entry.payload.value
		}
	})
}