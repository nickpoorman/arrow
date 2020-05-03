package parquet

import (
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
)

// func TestVectorBooleanTest_TestEncodeDecode() {
// 	// TODO: Implement....
// }

// ----------------------------------------------------------------------
// test data generation

func GenerateDataInt(numValues int, out interface{}) {
	// seed the prng so failure is deterministic
	randomNumbersInt(numValues, 0, math.MaxInt64, math.MinInt64 < 0, out)
}

// ----------------------------------------------------------------------
// Create some column descriptors

func ExampleDescr(t *testing.T, dtype PhysicalType) *ColumnDescriptor {
	t.Helper()
	node, err := NewPrimitiveNodeFromConvertedType(
		"name", RepetitionType_OPTIONAL, dtype.typeNum(),
		ConvertedType_NONE, -1, -1, -1, -1,
	)
	testutil.AssertNil(t, err)
	cd, err := NewColumnDescriptor(node, 0, 0, nil)
	testutil.AssertNil(t, err)
	return cd
}

// ----------------------------------------------------------------------
// Plain encoding tests

type testEncodingBaseInt64 struct {
	t            *testing.T
	dtype        PhysicalType
	allocator    memory.Allocator
	numValues    int
	typeLength   int
	draws        interface{} // T*
	decodeBuf    interface{} // T*
	inputBytes   []byte      // std::vector<uint8_t>
	outputBytes  []byte      // std::vector<uint8_t>
	dataBuffer   []byte      // std::vector<uint8_t>
	encodeBuffer *memory.Buffer
	descr        *ColumnDescriptor
}

func newTestEncodingBaseInt64(t *testing.T, dtype PhysicalType) *testEncodingBaseInt64 {
	t.Helper()

	return &testEncodingBaseInt64{
		t:     t,
		dtype: dtype,
	}
}

func (t *testEncodingBaseInt64) Setup() {
	t.t.Helper()

	t.descr = ExampleDescr(t.t, t.dtype)
	t.typeLength = t.descr.typeLength()
	t.allocator = memory.DefaultAllocator
}

func (t *testEncodingBaseInt64) TearDown() {}

func (t *testEncodingBaseInt64) InitData(nvalues int, repeats int) {
	t.t.Helper()

	t.numValues = nvalues * repeats

	switch t.dtype.typeNum() {
	// case Type_BOOLEAN:
	case Type_INT32:
		t.initDataInt32(nvalues, repeats)
	case Type_INT64:
		t.initDataInt64(nvalues, repeats)
	// case Type_INT96: // TODO: implement
	case Type_FLOAT:
		t.initDataFloat32(nvalues, repeats)
	case Type_DOUBLE:
		t.initDataFloat64(nvalues, repeats)
	// case Type_BYTE_ARRAY:
	// case Type_FIXED_LEN_BYTE_ARRAY:
	default:
		t.t.Errorf("unhandled Type: %s\n", TypeToString(t.dtype.typeNum()))
	}

	GenerateDataInt(nvalues, t.draws)

	// add some repeated values
	for j := 1; j < repeats; j++ {
		for i := 0; i < nvalues; i++ {
			// t.draws.([]int64)[nvalues*j+i] = t.draws.([]int64)[i]
			reflect.ValueOf(t.draws).Index(nvalues*j + i).Set(reflect.ValueOf(t.draws).Index(i))
		}
	}
}

func (t *testEncodingBaseInt64) initDataInt32(nvalues int, repeats int) {
	t.t.Helper()
	t.inputBytes = arrow.Int32Traits.CastToBytes(make([]int32, t.numValues))
	t.outputBytes = arrow.Int32Traits.CastToBytes(make([]int32, t.numValues))
	t.draws = arrow.Int32Traits.CastFromBytes(t.inputBytes)
	t.decodeBuf = arrow.Int32Traits.CastFromBytes(t.outputBytes)
}

func (t *testEncodingBaseInt64) initDataInt64(nvalues int, repeats int) {
	t.t.Helper()
	t.inputBytes = arrow.Int64Traits.CastToBytes(make([]int64, t.numValues))
	t.outputBytes = arrow.Int64Traits.CastToBytes(make([]int64, t.numValues))
	t.draws = arrow.Int64Traits.CastFromBytes(t.inputBytes)
	t.decodeBuf = arrow.Int64Traits.CastFromBytes(t.outputBytes)
}

func (t *testEncodingBaseInt64) initDataFloat32(nvalues int, repeats int) {
	t.t.Helper()
	t.inputBytes = arrow.Float32Traits.CastToBytes(make([]float32, t.numValues))
	t.outputBytes = arrow.Float32Traits.CastToBytes(make([]float32, t.numValues))
	t.draws = arrow.Float32Traits.CastFromBytes(t.inputBytes)
	t.decodeBuf = arrow.Float32Traits.CastFromBytes(t.outputBytes)
}

func (t *testEncodingBaseInt64) initDataFloat64(nvalues int, repeats int) {
	t.t.Helper()
	t.inputBytes = arrow.Float64Traits.CastToBytes(make([]float64, t.numValues))
	t.outputBytes = arrow.Float64Traits.CastToBytes(make([]float64, t.numValues))
	t.draws = arrow.Float64Traits.CastFromBytes(t.inputBytes)
	t.decodeBuf = arrow.Float64Traits.CastFromBytes(t.outputBytes)
}

type testPlainEncodingInt64 struct {
	testEncodingBaseInt64
}

func newTestPlainEncodingInt64(t *testing.T, dtype PhysicalType) *testPlainEncodingInt64 {
	t.Helper()
	return &testPlainEncodingInt64{
		testEncodingBaseInt64: *newTestEncodingBaseInt64(t, dtype),
	}
}

func (t *testPlainEncodingInt64) Execute(nvalues int, repeats int) {
	t.t.Helper()
	t.InitData(nvalues, repeats)
	t.CheckRoundtrip()
}

func (t *testPlainEncodingInt64) CheckRoundtrip() {
	t.t.Helper()

	// TODO: Use a checked allocator here...
	encoder, err := NewTypedEncoder(t.dtype, EncodingType_PLAIN, false, t.descr, memory.DefaultAllocator)
	testutil.AssertNil(t.t, err)

	decoder, err := NewTypedDecoder(t.dtype, EncodingType_PLAIN, t.descr)
	testutil.AssertNil(t.t, err)

	// encode the []int64 into []bytes
	testutil.AssertNil(t.t, encoder.Put(t.draws, t.numValues))
	t.encodeBuffer = encoder.FlushValues()

	// decode the []bytes back to []int64
	decoder.SetData(t.numValues, t.encodeBuffer.Bytes(), t.encodeBuffer.Len())
	valuesDecoded, err := decoder.DecodeBuffer(t.decodeBuf, t.numValues)
	testutil.AssertNil(t.t, err)
	testutil.AssertEqInt(t.t, t.numValues, valuesDecoded)
}

// typedef ::testing::Types<BooleanType, Int32Type, Int64Type, Int96Type, FloatType,
//                          DoubleType, ByteArrayType, FLBAType>
//     ParquetTypes;

var ParquetTypes = []PhysicalType{
	//BooleanType, Int32Type,
	Int64Type,
	// Int96Type, FloatType
}

func TestTypedTestCase_TestPlainEncoding_BasicRoundTrip(t *testing.T) {
	for _, c := range ParquetTypes {
		runner := newTestPlainEncodingInt64(t, c)
		runner.Execute(10000, 1)
	}
}

// ----------------------------------------------------------------------
// Dictionary encoding tests

// typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
//                          ByteArrayType, FLBAType>
//     DictEncodedTypes;

var DictEncodedTypes = []PhysicalType{
	// Int32Type,
	Int64Type,
	// Int96Type, FloatType,
}

type testDictionaryEncodingInt64 struct {
	testEncodingBaseInt64
	dictBuffer *memory.Buffer
}

func newTestDictionaryEncodingInt64(t *testing.T, dtype PhysicalType) *testDictionaryEncodingInt64 {
	return &testDictionaryEncodingInt64{
		testEncodingBaseInt64: *newTestEncodingBaseInt64(t, dtype),
	}
}

func (t *testDictionaryEncodingInt64) Execute(nvalues int, repeats int) {
	t.t.Helper()
	t.InitData(nvalues, repeats)
	t.CheckRoundtrip()
}

func makeBytesWithInitValue(length int, initValue byte) (validBits []byte) {
	validBits = make([]byte, length)
	for i := range validBits {
		validBits[i] = initValue
	}
	return
}

// TODO: Generate this test file
func (t *testDictionaryEncodingInt64) CheckRoundtrip() {
	t.t.Helper()

	validBits := makeBytesWithInitValue(
		int(bitutil.BytesForBits(int64(t.numValues))+1), 255)

	baseEncoder, err := NewEncoder(Int64Type.typeNum(), EncodingType_PLAIN, true, t.descr, memory.DefaultAllocator)
	testutil.AssertNil(t.t, err)
	encoder := baseEncoder.(*Int64DictEncoder)
	dictTraits := encoder

	testutil.AssertNil(t.t, encoder.PutBuffer(t.draws, t.numValues))
	t.dictBuffer = AllocateBuffer(memory.DefaultAllocator, dictTraits.DictEncodedSize())
	dictTraits.WriteDict(t.dictBuffer.Bytes())
	indicies := encoder.FlushValues()

	baseSpacedEncoder, err := NewEncoder(Int64Type.typeNum(), EncodingType_PLAIN, true, t.descr, memory.DefaultAllocator)
	testutil.AssertNil(t.t, err)
	spacedEncoder := baseSpacedEncoder.(*Int64DictEncoder)

	// PustSpaced should lead to the same results
	testutil.AssertNil(t.t, spacedEncoder.PutSpaced(t.draws, t.numValues, validBits, 0))
	indiciesFromSpaced := spacedEncoder.FlushValues()
	testutil.AssertTrue(t.t, indiciesFromSpaced.Equals(indicies))

	dictDecoder, err := NewTypedDecoder(Int64Type, EncodingType_PLAIN, t.descr)
	testutil.AssertNil(t.t, err)
	dictDecoder.SetData(dictTraits.numEntries(), t.dictBuffer.Bytes(), t.dictBuffer.Len())

	decoder, err := NewDictDecoder(Int64Type.typeNum(), t.descr, t.allocator)
	testutil.AssertNil(t.t, err)
	decoder.SetDict(dictDecoder)

	decoder.SetData(t.numValues, indicies.Bytes(), indicies.Len())
	valuesDecoded := decoder.Decode(t.decodeBuf, t.numValues)
	testutil.AssertEqInt(t.t, valuesDecoded, t.numValues)

	// TODO: The DictionaryDecoder must stay alive because the decoded
	// values' data is owned by a buffer inside the DictionaryEncoder. We
	// should revisit when data lifetime is reviewed more generally.
	testutil.AssertNil(t.t, VerifyResults(t.t, t.decodeBuf, t.draws, t.numValues))

	// Also test spaced decoding
	decoder.SetData(t.numValues, indicies, len(indicies))
	valuesDecoded := decoder.DecodeSpaced(t.decodeBuf, t.numValues, 0, validBits, 0)
	testutil.AssertEqInt(t.t, t.numValues, valuesDecoded)
	testutil.AssertNil(t.t, VerifyResults(t.t, t.decodeBuf, t.draws, t.numValues))
}

func TestDictionaryEncoding_BasicRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	for _, c := range DictEncodedTypes {
		runner := newTestDictionaryEncodingInt64(t, c)
		runner.allocator = pool
		runner.Execute(10000, 1)
	}
}
