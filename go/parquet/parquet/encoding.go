package parquet

import (
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/internal/util"
	utilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

// The ColumnReader that will create these will have a physical type on the ColumnDescriptor.
// Instead of creating one for each possible type, lets build logic off the physical type.

// If it has a specific underlying type then we need an interface for it
// Encoder
//  |_ encoderBase
// TypedEncoder
//  |_ BooleanEncoder < TypedEncoder < encoderBase
// PlainEncoder
//  |_ BooleanPlainEncoder < BooleanEncoder

// ------------ New layout below

// Encoder
//  |_ encoderBase
// PlainEncoder
//  |_ BooleanPlainEncoder < encoderBase

type Encoder interface {
	// From encoderBase
	Encoding() EncodingType
	MemoryPool() memory.Allocator
}

// type TypedEncoder interface {
// 	Encoder

// 	PutBuffer(buffer interface{}, numValues int) error
// 	PutVector(src []interface{}, numValues int) error
// 	PutSpaced(src interface{}, numValues int, validBits []byte, validBitsOffset int64) error
// }

type PlainEncoder interface {
	// TypedEncoder
	Put(src interface{}, numValues int) error
	PutSpaced(src interface{}, validBits []byte, validBitsOffset int) error

	EstimatedDataEncodedSize() int
	FlushValues() *memory.Buffer
	PutArrowArray(values array.Interface) error
}

// TODO: Fix comment below after verify
// TypedEncoder can be a PlainEncoder or a DictEncoder?
type TypedEncoderInterface interface {
	Put(src interface{}, numValues int) error
	PutSpaced(src interface{}, validBits []byte, validBitsOffset int) error

	EstimatedDataEncodedSize() int
	FlushValues() *memory.Buffer
	PutArrowArray(values array.Interface) error
}

type Decoder interface {
	// Sets the data for a new page. This will be called multiple times on the same
	// decoder and should reset all internal state.
	SetData(numValues int, data []byte, len int) error

	// Returns the number of values left (for the last call to SetData()). This is
	// the number of values left in this page.
	valuesLeft() int
	Encoding() EncodingType

	// // Decode values into a buffer
	// //
	// // Subclasses may override the more specialized Decode methods below.
	// //
	// // buffer - destination for decoded values
	// // maxBalues - maximum number of values to decode
	// // returns the number of values decoded. Should be identical to maxValues except
	// // at the end of the current data page.
	// DecodeBuffer(buffer interface{}, makeValues int) (int, error)

	// // Decode the values in this data page but leave spaces for null entries.
	// //
	// // buffer - destination for decoded values
	// // numValues - size of the def_levels and buffer arrays including the number
	// // of null slots
	// // nullCount - number of null slots
	// // validBits - bitmap data indicating position of valid slots
	// // validBitsOffset - offset into valid_bits
	// // returns the number of values decoded, including nulls.
	// DecodeSpaced(buffer interface{}, numValues int, nullCount int,
	// 	validBits []byte, validBitsOffset int) (int, error)
}

type TypedDecoderInterface interface {
	Decoder

	// Decode values into a buffer
	//
	// Subclasses may override the more specialized Decode methods below.
	//
	// buffer - destination for decoded values
	// maxBalues - maximum number of values to decode
	// returns the number of values decoded. Should be identical to maxValues except
	// at the end of the current data page.
	// DecodeBuffer(buffer interface{}, makeValues int) (int, error)
	Decode(buffer interface{}, numValues int) (int, error)

	// Decode the values in this data page but leave spaces for null entries.
	//
	// buffer - destination for decoded values
	// numValues - size of the def_levels and buffer arrays including the number
	// of null slots
	// nullCount - number of null slots
	// validBits - bitmap data indicating position of valid slots
	// validBitsOffset - offset into valid_bits
	// returns the number of values decoded, including nulls.
	DecodeSpaced(buffer interface{}, numValues int, nullCount int,
		validBits []byte, validBitsOffset int64) (int, error)

	// Decode into an ArrayBuilder or other accumulator
	//
	// returns number of values decoded
	// DecodeArrow(numValues int, nullCount int, validBits []byte, validBitsOffset int, out *Accumulator) (int, error)

	// Decode into an ArrayBuilder or other accumulator ignoring nulls
	//
	// returns number of values decoded
	// DecodeArrowNonNull(numValues int, out *Accumulator) (int, error)

	// Decode into a DictionaryBuilder
	//
	// return number of values decoded
	// DecodeArrow(numValues int, nullCount int, validBits []byte, validBitsOffset int, builder *DictAccumulator) (int, error)

	// Decode into a DictionaryBuilder ignoring nulls
	//
	// returns number of values decoded
	// DecodeArrowNonNull(numValues int, builder *DictAccumulator, validBits []byte) (int, error) // return DecodeArrow(num_values, 0, &valid_bits, 0, builder)
}

type DictDecoder interface {
	TypedDecoderInterface

	SetDict(dictionary TypedDecoderInterface) error
}

// ----------------------------------------------------------------------
// Base encoder implementation

type encoderBase struct {
	encodingTraits EncodingTraits

	// For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
	descr *ColumnDescriptor
	enc   EncodingType
	pool  memory.Allocator

	// Type length from descr
	typeLength int
}

func newEncoderBase(
	encodingTraits EncodingTraits, descr *ColumnDescriptor,
	encoding EncodingType, pool memory.Allocator) encoderBase {

	typeLength := -1
	if descr != nil {
		typeLength = descr.typeLength()
	}
	return encoderBase{
		encodingTraits: encodingTraits,
		descr:          descr,
		enc:            encoding,
		pool:           pool,
		typeLength:     typeLength,
	}
}

func (e encoderBase) Encoding() EncodingType       { return e.enc }
func (e encoderBase) MemoryPool() memory.Allocator { return e.pool }

// ----------------------------------------------------------------------
// PlainEncoder<T> implementations

type bufferBuilderInterface interface {
	Len() int
	Finish() *memory.Buffer
	Append([]byte)
}

type Int64PlainEncoder struct {
	encoderBase

	// base
	// sink *array.Int64BufferBuilder
	sink bufferBuilderInterface
}

func NewInt64PlainEncoder(
	encodingTraits EncodingTraits, descr *ColumnDescriptor,
	pool memory.Allocator) (*Int64PlainEncoder, error) {

	return &Int64PlainEncoder{
		encoderBase: newEncoderBase(
			encodingTraits, descr, EncodingType_PLAIN, pool),
		sink: array.NewInt64BufferBuilder(pool),
	}, nil
}

func (e *Int64PlainEncoder) EstimatedDataEncodedSize() int {
	return e.sink.Len()
}

func (e *Int64PlainEncoder) FlushValues() *memory.Buffer {
	return e.sink.Finish()
}

func (e *Int64PlainEncoder) Put(src interface{}, numValues int) error {
	switch v := src.(type) {
	case []int64:
		e.sink.(*array.Int64BufferBuilder).AppendValues(v)
		return nil
	case []byte:
		return e.PutBuffer(v)
	default:
		return fmt.Errorf(
			"Put: to %s from %T not supported",
			arrow.INT64.String(),
			src,
		)
	}
}

func (e *Int64PlainEncoder) PutArrowArray(values array.Interface) error {
	arrayType := arrow.Int64Type{}
	if values.DataType().ID() != arrayType.ID() {
		return fmt.Errorf(
			"PutArrowArray: direct put to %s from %s not supported",
			arrayType.Name(),
			values.DataType().Name(),
		)
	}

	// TODO(nickpoorman): Not sure if we can cast here or if we need to init it from the data. Need to test this...
	// int64Values := array.NewInt64Data(values.Data())
	vals := values.(*array.Int64)
	rawValues := vals.Int64Values()

	if values.NullN() == 0 {
		// no nulls, just dump the data
		e.sink.AppendValues(rawValues)
	} else {
		for i := 0; i < vals.Len(); i++ {
			if vals.IsValid(i) {
				e.sink.AppendValue(vals.Value(i))
			}
		}
	}

	return nil
}

func (e *Int64PlainEncoder) PutBuffer(buffer []byte) error {
	e.sink.Append(buffer)
	return nil
}

func (e *Int64PlainEncoder) PutSpaced(
	src interface{}, validBits []byte, validBitsOffset int) error {

	switch v := src.(type) {
	case []int64:
		return e.putSpacedValuesInt64(v, validBits, validBitsOffset)
	default:
		return fmt.Errorf(
			"PutSpaced: to %s from %T not supported",
			arrow.INT64.String(),
			src,
		)
	}
}

// TODO: Add the other types...
func (e *Int64PlainEncoder) putSpacedValuesInt64(
	src []int64, validBits []byte,
	validBitsOffset int) error {
	numValues := len(src)

	buffer := memory.NewResizableBuffer(e.MemoryPool())
	defer buffer.Release()
	buffer.Resize(arrow.Int64Traits.BytesRequired(numValues))

	numValidValues := 0
	validBitsReader := bitutilext.NewBitmapReader(
		validBits, validBitsOffset, numValues)
	data := arrow.Int64Traits.CastFromBytes(buffer.Bytes())

	for i := 0; i < numValues; i++ {
		if validBitsReader.IsSet() {
			data[numValidValues] = src[i]
			numValidValues++
		}
		validBitsReader.Next()
	}
	return e.PutBuffer(arrow.Int64Traits.CastToBytes(data[:numValidValues]))
}

// ----------------------------------------------------------------------
// DictEncoder<T> implementations

// Initially 1024 elements
const kInitialHashTableSize = 1 << 10

// See the dictionary encoding section of
// https://github.com/Parquet/parquet-format.  The encoding supports
// streaming encoding. Values are encoded as they are added while the
// dictionary is being constructed. At any time, the buffered values
// can be written out with the current dictionary size. More values
// can then be added to the encoder, including new dictionary
// entries.

//+ {{.DType}}DictEncoder
type Int64DictEncoder struct {
	encoderBase

	dataType arrow.DataType

	sink *array.Int64BufferBuilder

	// Indices that have not yet be written out by WriteIndices().
	bufferedIndices []int32 // C++ implementation has this as an ArrowPoolVector

	// The number of bytes needed to encode the dictionary.
	dictEncodedSize int

	memoTable utilext.MemoTable
}

//+ func New{{.DType}}DictEncoder(descr *ColumnDescriptor, pool memory.Allocator) (*{{.DType}}DictEncoder, error) {
func NewInt64DictEncoder(encodingTraits EncodingTraits, descr *ColumnDescriptor,
	pool memory.Allocator) (*Int64DictEncoder, error) {

	return &Int64DictEncoder{
		encoderBase: newEncoderBase(
			encodingTraits, descr, EncodingType_PLAIN_DICTIONARY, pool),
		sink: array.NewInt64BufferBuilder(pool),
		memoTable: utilext.NewMemoTable(
			pool, kInitialHashTableSize, encodingTraits.ArrowType),
	}, nil
}

// Returns a conservative estimate of the number of bytes needed to encode the buffered
// indices. Used to size the buffer passed to WriteIndices().
func (e *Int64DictEncoder) EstimatedDataEncodedSize() int64 {
	// Note: because of the way RleEncoder::CheckBufferFull() is called, we have to
	// reserve
	// an extra "RleEncoder::MinBufferSize" bytes. These extra bytes won't be used
	// but not reserving them would cause the encoder to fail.
	return 1 + int64(utilext.RleEncoderMaxBufferSize(
		e.bitWidth(), len(e.bufferedIndices),
	)) + int64(utilext.RleEncoderMinBufferSize(e.bitWidth()))
}

func (e *Int64DictEncoder) DictEncodedSize() int { return e.dictEncodedSize }

//+ func (e *{{.DType}}DictEncoder) PutBuffer(buffer interface{}, numValues int) error {
func (e *Int64DictEncoder) PutBuffer(buffer interface{}, numValues int) error {
	if numValues > 0 {
		switch v := buffer.(type) {
		case []int64:
			for i := 0; i < numValues; i++ {
				e.PutValue(v[i])
			}
		default:
			return fmt.Errorf(
				"PutBuffer: direct put to %s from %T not supported",
				arrow.INT64.String(),
				buffer,
			)
		}
	}
	return nil
}

// Encode value. Note that this does not actually write any data, just
// buffers the value's index to be written later.
// func (e *Int64DictEncoder) PutValue(v interface{}) error {
// 	// switch v.(type) {
// 	// }
// 	return nil
// }

// TODO: This should really be the native type for speed....
// func (e *Int64DictEncoder) PutValue(v int64) error {
//+ Specialization required here for: FLBAType, ByteArrayType, Int96Type
//+ func (e *{{.DType}}DictEncoder) PutValue(v interface{}) error {
func (e *Int64DictEncoder) PutValue(v int64) error {
	// Put() implementation for primitive types
	onFound := func(memoIndex int32) {}
	onNotFound := func(memoIndex int32) {
		//+ e.dictEncodedSize += {{.Size}}
		e.dictEncodedSize += binary.Size(v) // SIZE_OF_T // TODO: Maybe we generate these?
	}

	memoIndex, err := e.memoTable.GetOrInsert(
		e.dataType.BuildScalar(v), onFound, onNotFound)
	if err != nil {
		return err
	}
	e.bufferedIndices = append(e.bufferedIndices, memoIndex)
	return nil
}

//+ Specialization required here for: FLBAType, ByteArrayType, Int96Type
//+ func (e *{{.DType}}DictEncoder) PutValues(values array.Interface) error {
func (e *Int64DictEncoder) PutValues(values array.Interface) error {
	// data := values.(*{{.ArrayType}})
	data := values.(*array.Int64)
	if data.NullN() == 0 {
		// no nulls, just dump the data
		for i := 0; i < data.Len(); i++ {
			if err := e.PutValue(data.Value(i)); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < data.Len(); i++ {
			if data.IsValid(i) {
				if err := e.PutValue(data.Value(i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (e *Int64DictEncoder) PutSpaced(
	src interface{}, numValues int,
	validBits []byte, validBitsOffset int) error {

	srcInt64, ok := src.([]int64)
	if !ok {
		return fmt.Errorf(
			"PutSpaced: to %s from %T not supported",
			arrow.INT64.String(),
			src,
		)
	}

	validBitsReader := bitutilext.NewBitmapReader(
		validBits, validBitsOffset, numValues)
	for i := 0; i < numValues; i++ {
		if validBitsReader.IsSet() {
			if err := e.PutValue(srcInt64[i]); err != nil {
				return err
			}
		}
		validBitsReader.Next()
	}

	return nil
}

func (e *Int64DictEncoder) WriteIndices(buffer []byte, bufferLen int) int {
	// Write bit width in first byte
	buffer[0] = byte(e.bitWidth())
	buffer = buffer[1:]
	bufferLen--

	encoder := utilext.NewRleEncoder(buffer, bufferLen, e.bitWidth())

	for index := range e.bufferedIndices {
		if !encoder.Put(uint64(index)) {
			return -1
		}
	}
	encoder.Flush()

	e.clearIndicies()
	return 1 + encoder.Len()
}

// The minimum bit width required to encode the currently buffered indices.
func (e *Int64DictEncoder) bitWidth() int {
	if e.numEntries() == 0 {
		return 0
	}
	if e.numEntries() == 1 {
		return 1
	}
	return bitutilext.Log2(uint64(e.numEntries()))
}

// The number of entries in the dictionary.
func (e *Int64DictEncoder) numEntries() int {
	return int(e.memoTable.Size())
}

// Clears all the indices (but leaves the dictionary).
func (e *Int64DictEncoder) clearIndicies() {
	e.bufferedIndices = make([]int32, 0, cap(e.bufferedIndices))
}

//+ func {{.DType}}AssertCanPutDictionary(encoder *{{.DType}}DictEncoder, dict array.Interface) error {
func Int64AssertCanPutDictionary(
	encoder *Int64DictEncoder, dict array.Interface) error {

	if dict.NullN() > 0 {
		fmt.Errorf(
			"Inserted dictionary cannot cannot contain nulls: %w",
			ParquetException,
		)
	}

	if encoder.numEntries() > 0 {
		fmt.Errorf(
			"Can only call PutDictionary on an empty DictEncoder: %w",
			ParquetException,
		)
	}

	return nil
}

//+ func {{.DType}}PutDictionary(values array.Interface) error {
func (e *Int64DictEncoder) PutDictionary(values array.Interface) error {
	//+ {{.DType}}AssertCanPutDictionary
	if err := Int64AssertCanPutDictionary(e, values); err != nil {
		return err
	}

	// data := values.(*{{.ArrayType}})
	data := values.(*array.Int64)

	//+ e.dictEncodedSize += int({{.Size}} * data.Len())
	e.dictEncodedSize += int(binary.Size(int64(0)) * data.Len())
	for i := 0; i < data.Len(); i++ {
		_, err := e.memoTable.GetOrInsert(
			e.dataType.BuildScalar(data.Value(i)),
			utilext.OnFoundNoOp,
			utilext.OnNotFoundNoOp,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Int64DictEncoder) FlushValues() *memory.Buffer {
	buffer := AllocateBuffer(e.pool, int(e.EstimatedDataEncodedSize()))
	resultSize := e.WriteIndices(
		buffer.Buf(), int(e.EstimatedDataEncodedSize()))
	buffer.ResizeNoShrink(resultSize)
	return buffer
}

// Writes out the encoded dictionary to buffer. buffer must be preallocated to
// DictEncodedSize() bytes.
func (e *Int64DictEncoder) WriteDict(buffer []byte) {
	// For primitive types, only a memcpy
	debug.Assert(
		e.dictEncodedSize == binary.Size(int64(0))*int(e.memoTable.Size()),
		fmt.Sprintf(
			"Assert: e.dictEncodedSize == "+
				"binary.Size(int64(0))*int(e.memoTable.Size())"+
				" | %d == %d",
			e.dictEncodedSize, binary.Size(int64(0))*int(e.memoTable.Size()),
		))
	e.memoTable.CopyValues(0 /* startPos */, -1, buffer)
}

// ----------------------------------------------------------------------
// Base decoder implementation

type decoderBase struct {
	encodingTraits EncodingTraits

	// For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
	descr      *ColumnDescriptor
	enc        EncodingType
	numValues  int
	data       []byte
	len        int
	typeLength int
}

func newDecoderBase(
	encodingTraits EncodingTraits, descr *ColumnDescriptor,
	enc EncodingType) *decoderBase {

	return &decoderBase{
		encodingTraits: encodingTraits,
		descr:          descr,
		enc:            enc,
	}
}

// SetData updates decoderBase with the params.
func (d *decoderBase) SetData(numValues int, data []byte, len int) error {
	d.numValues = numValues
	d.data = data
	d.len = len
	return nil
}

func (d *decoderBase) valuesLeft() int        { return d.numValues }
func (d *decoderBase) Encoding() EncodingType { return d.enc }

// ----------------------------------------------------------------------
// Typed plain decoder implementations

type Int64PlainDecoder struct {
	decoderBase
}

func NewInt64PlainDecoder(
	encodingTraits EncodingTraits,
	descr *ColumnDescriptor) (*Int64PlainDecoder, error) {

	return &Int64PlainDecoder{
		decoderBase: *newDecoderBase(encodingTraits, descr, EncodingType_PLAIN),
	}, nil
}

func (d *Int64PlainDecoder) DecodeSpaced(
	buffer interface{}, numValues int, nullCount int,
	validBits []byte, validBitsOffset int64) (int, error) {
	// int values_to_read = num_values - null_count;
	// int values_read = Decode(buffer, values_to_read);
	// if (values_read != values_to_read) {
	//   throw ParquetException("Number of values / definition_levels read did not match");
	// }

	// // Depending on the number of nulls, some of the value slots in buffer may
	// // be uninitialized, and this will cause valgrind warnings / potentially UB
	// memset(static_cast<void*>(buffer + values_read), 0,
	//        (num_values - values_read) * sizeof(T));

	// // Add spacing for null entries. As we have filled the buffer from the front,
	// // we need to add the spacing from the back.
	// int values_to_move = values_read;
	// for (int i = num_values - 1; i >= 0; i--) {
	//   if (BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
	//     buffer[i] = buffer[--values_to_move];
	//   }
	// }
	// return num_values;
	panic("not yet implemented")
}

// numValues is the number of values in it's type,
// i.e. not the number of bytes.
func (d *Int64PlainDecoder) Decode(
	buffer interface{}, maxValues int) (int, error) {

	switch v := buffer.(type) {
	case []int64:
		return d.DecodeValues(v, maxValues)
	case []byte:
		return d.DecodeValues(arrow.Int64Traits.CastFromBytes(v), maxValues)
	default:
		return 0, fmt.Errorf(
			"Int64PlainDecoder: to %T not supported",
			buffer,
		)
	}
}

// numValues is the number of values in it's type,
// i.e. not the number of bytes.
func (d *Int64PlainDecoder) DecodeValues(
	buffer []int64, maxValues int) (int, error) {

	if d.numValues < maxValues {
		maxValues = d.numValues
	}
	bytesConsumed, err := d.DecodePlain(
		d.data, d.len, maxValues, d.typeLength, buffer)
	if err != nil {
		return 0, err
	}
	d.data = d.data[bytesConsumed:] // move the pointer up
	d.len -= bytesConsumed
	d.numValues -= maxValues
	return maxValues, nil
}

func (d *Int64PlainDecoder) DecodePlain(
	data []byte, dataSize int, numValues int,
	typeLength int, out interface{}) (int, error) {

	bytesToDecode := arrow.Int64Traits.BytesRequired(numValues)
	if dataSize < bytesToDecode {
		return 0, ParquetEofException
	}

	// If bytesToDecode == 0, data could be null
	if bytesToDecode > 0 {
		outInt64 := out.([]int64)
		copy(outInt64, arrow.Int64Traits.CastFromBytes(data)[:numValues])
	}
	return bytesToDecode, nil
}

// ----------------------------------------------------------------------
// Dictionary encoding and decoding

//+ {{.DType}}DictDecoder
type Int64DictDecoder struct {
	decoderBase

	// dtype PhysicalType // TODO: Cleanup Remove
	sink *array.Int64BufferBuilder

	// Only one is set.
	dictionary *memory.Buffer

	dictionaryLength int32

	// Data that contains the byte array data (byte_array_dictionary_ just has the
	// pointers).
	byteArrayData *memory.Buffer

	// Arrow-style byte offsets for each dictionary value. We maintain two
	// representations of the dictionary, one as ByteArray* for non-Arrow
	// consumers and this one for Arrow consumers. Since dictionaries are
	// generally pretty small to begin with this doesn't mean too much extra
	// memory use in most cases
	byteArrayOffsets *memory.Buffer

	// Reusable buffer for decoding dictionary indices to be appended to a
	// BinaryDictionary32Builder
	indicesScratchSpace *memory.Buffer

	idxDecoder *utilext.RleDecoder
}

// Initializes the dictionary with values from 'dictionary'. The data in
// dictionary is not guaranteed to persist in memory after this call so the
// dictionary decoder needs to copy the data out if necessary.
//+ func New{{.DType}}DictDecoder(descr *ColumnDescriptor, pool memory.Allocator) (*{{.DType}}DictDecoder, error) {
func NewInt64DictDecoder(
	encodingTraits EncodingTraits, descr *ColumnDescriptor,
	pool memory.Allocator) (*Int64DictDecoder, error) {

	return &Int64DictDecoder{
		decoderBase: *newDecoderBase(
			encodingTraits, descr, EncodingType_RLE_DICTIONARY),

		sink:                array.NewInt64BufferBuilder(pool),
		dictionary:          AllocateBuffer(pool, 0),
		dictionaryLength:    0,
		byteArrayData:       AllocateBuffer(pool, 0),
		byteArrayOffsets:    AllocateBuffer(pool, 0),
		indicesScratchSpace: AllocateBuffer(pool, 0),
	}, nil
}

func (d *Int64DictDecoder) decodeDict(
	dictionary TypedDecoderInterface) (int, error) {

	d.dictionaryLength = int32(dictionary.valuesLeft())
	d.dictionary.ResizeNoShrink(
		// change this to use the type trait for the physical type
		arrow.Int64Traits.BytesRequired(int(d.dictionaryLength)),
	)
	return dictionary.Decode(d.dictionary.Buf(), int(d.dictionaryLength))
}

// Perform type-specific initiatialization
func (d *Int64DictDecoder) SetDict(dictionary TypedDecoderInterface) error {
	// TODO(nickpoorman): This is different for each type we generate
	_, err := d.decodeDict(dictionary)
	return err
}

func (d *Int64DictDecoder) SetData(numValues int, data []byte, len int) error {
	d.numValues = numValues
	if len == 0 {
		// Initialize dummy decoder to avoid crashes later on
		d.idxDecoder = utilext.NewRleDecoder(data, len /* bitWdith */, 1)
		return nil
	}
	bitWidth := data[0]
	if bitWidth >= 64 {
		return fmt.Errorf(
			"Invalid or corrupted bitWidth: %w",
			ParquetNYIException,
		)
	}
	d.idxDecoder = utilext.NewRleDecoder(data[1:], len-1, int(bitWidth))
	return nil
}

// numValues is the number of values in it's type,
// i.e. not the number of bytes.
func (d *Int64DictDecoder) Decode(
	buffer interface{}, numValues int) (int, error) {

	switch v := buffer.(type) {
	// case []int64:
	// 	return d.DecodeValues(v, numValues)
	case []byte:
		return d.DecodeBuffer(v, numValues)
	default:
		return 0, fmt.Errorf(
			"{{.DType}} Decode: to %T not supported",
			buffer,
		)
	}
}

func (d *Int64DictDecoder) DecodeBuffer(
	buffer []byte, numValues int) (int, error) {

	if d.numValues < numValues {
		numValues = d.numValues
	}

	decodedValues := d.idxDecoder.GetBatchWithDict(
		d.dictionary.Bytes(),
		d.dictionaryLength,
		buffer,
		numValues)

	if decodedValues != numValues {
		return 0, ParquetEofException
	}
	d.numValues -= numValues
	return numValues, nil
}

// numValues is the number of values in it's type,
// i.e. not the number of bytes.
func (d *Int64DictDecoder) DecodeSpaced(
	src interface{}, numValues int, nullCount int,
	validBits []byte, validBitsOffset int64) (int, error) {
	switch v := src.(type) {
	// case []int64:
	// 	return d.DecodeSpacedValues(v,
	// 		numValues, nullCount, validBits, validBitsOffset)
	case []byte:
		return d.decodeSpacedBuffer(v,
			numValues, nullCount, validBits, validBitsOffset)
	default:
		return 0, fmt.Errorf(
			"{{.DType}} DecodeSpaced: to %T not supported",
			src,
		)
	}
}

func (d *Int64DictDecoder) decodeSpacedBuffer(
	buffer []byte, numValues int, nullCount int,
	validBits []byte, validBitsOffset int64) (int, error) {
	numValues = util.MinInt(numValues, d.numValues)
	if numValues != d.idxDecoder.GetBatchWithDictSpaced(
		d.dictionary.Bytes(),
		d.dictionaryLength,
		buffer,
		numValues,
		nullCount,
		validBits,
		validBitsOffset,
	) {
		return 0, ParquetEofException
	}
	d.numValues -= numValues
	return numValues, nil
}

// // func (d *Int64DictDecoder) DecodeSpacedValues(buffer {{.CType}}, numValues int, nullCount int,
// func (d *Int64DictDecoder) decodeSpacedValues(buffer []int64, numValues int, nullCount int,
// 	validBits []byte, validBitsOffset int64) (int, error) {
// 	numValues = util.MinInt(numValues, d.numValues)
// 	if numValues != d.idxDecoder.GetBatchWithDictSpaced(
// 		// arrow.{{.DType}}Traits.CastFromBytes(d.dictionary.Bytes()),
// 		// arrow.Int64Traits.CastFromBytes(d.dictionary.Bytes()),
// 		d.dictionary.Bytes(),
// 		d.dictionaryLength,
// 		arrow.Int64Traits.CastFromBytes(buffer),
// 		numValues,
// 		nullCount,
// 		validBits,
// 		validBitsOffset,
// 	) {
// 		return 0, ParquetEofException
// 	}
// 	d.numValues -= numValues
// 	return numValues, nil
// }

func (d *Int64DictDecoder) DecodeArrow(
	numValues int, nullCount int, validBits []byte, validBitsOffset int64) {
	panic("not implemented")
}

func (d *Int64DictDecoder) DecodeArrowDict(
	numValues int, nullCount int, validBits []byte, validBitsOffset int64) {
	panic("not implemented")
}

func (d *Int64DictDecoder) InsertDictionary(builder array.Builder) {
	panic("not implemented")
}

func (d *Int64DictDecoder) DecodeIndicesSpaced(
	numValues int, nullCount int, validBits []byte, validBitsOffset int64,
	builder array.Builder) (int, error) {

	if numValues > 0 {
		d.indicesScratchSpace.Resize(
			arrow.Int32Traits.BytesRequired(numValues),
		)
	}

	indicesBuffer := d.indicesScratchSpace.Buf()

	if numValues != d.idxDecoder.GetBatchSpaced(numValues, nullCount, validBits,
		validBitsOffset, indicesBuffer) {
		return 0, ParquetEofException
	}
	// binaryBuilder := array
	panic("not implemented")
}

// func (d *Int64DictDecoder) DecodeBuffer(buffer interface{}, makeValues int) (int, error) {
// 	panic("not implemented")
// }

// ----------------------------------------------------------------------
// Factory functions

func NewTypedEncoder(
	encoding EncodingType, useDictionary bool, descr *ColumnDescriptor,
	pool memory.Allocator) (TypedEncoderInterface, error) {

	if pool == nil {
		pool = memory.DefaultAllocator
	}

	encoder, err := NewEncoder(encoding, useDictionary, descr, pool)
	if err != nil {
		return nil, err
	}
	return encoder.(TypedEncoderInterface), nil
}

// Encode goes from Arrow into Parquet
func NewEncoder(encoding EncodingType, useDictionary bool,
	descr *ColumnDescriptor, pool memory.Allocator) (Encoder, error) {

	if pool == nil {
		pool = memory.DefaultAllocator
	}

	if useDictionary {
		switch descr.PhysicalType() {
		case Type_INT64:
			return NewInt64DictEncoder(Int64EncodingTraits, descr, pool)
		default:
			return nil, fmt.Errorf(
				"dict encoder for %s not implemented: %w",
				TypeToString(descr.PhysicalType()),
				ParquetNYIException,
			)
		}

	} else if encoding == EncodingType_PLAIN {
		return NewPlainEncoder(descr, pool)

	} else {
		return nil, fmt.Errorf(
			"Selected encoding is not supported: %w",
			ParquetNYIException,
		)
	}
}

func NewPlainEncoder(descr *ColumnDescriptor,
	pool memory.Allocator) (Encoder, error) {

	switch descr.PhysicalType() {
	// case Type_INT32:
	// return NewInt32PlainEncoder(descr, pool)
	case Type_INT64:
		return NewInt64PlainEncoder(Int64EncodingTraits, descr, pool)
	default:
		return nil, fmt.Errorf(
			"plain encoder for %s not implemented: %w",
			TypeToString(descr.PhysicalType()),
			ParquetNYIException,
		)
	}
}

// TODO: Remove this?
func NewTypedDecoder(encoding EncodingType,
	descr *ColumnDescriptor) (TypedDecoderInterface, error) {

	decoder, err := NewDecoder(encoding, descr)
	if err != nil {
		return nil, err
	}
	return decoder.(TypedDecoderInterface), nil
}

func NewDecoder(
	encoding EncodingType, descr *ColumnDescriptor) (Decoder, error) {

	if encoding != EncodingType_PLAIN {
		return nil, fmt.Errorf(
			"Selected encoding is not supported: %w",
			ParquetNYIException,
		)
	}

	return NewPlainDecoder(descr)
}

func NewPlainDecoder(descr *ColumnDescriptor) (Decoder, error) {
	switch descr.PhysicalType() {
	// case Type_INT32:
	// return NewInt32PlainDecoder(descr, pool)
	case Type_INT64:
		return NewInt64PlainDecoder(Int64EncodingTraits, descr)
	default:
		return nil, fmt.Errorf(
			"plain decoder for %s not implemented: %w",
			TypeToString(descr.PhysicalType()),
			ParquetNYIException,
		)
	}
}

func NewDictDecoder(
	descr *ColumnDescriptor, pool memory.Allocator) (DictDecoder, error) {

	switch descr.PhysicalType() {
	case Type_BOOLEAN:
		return nil, fmt.Errorf(
			"Dictionary decoding not implemented for boolean type: %w",
			ParquetNYIException,
		)
	// case Type_INT32:
	// return NewInt32DictDecoder(descr, pool)
	case Type_INT64:
		return NewInt64DictDecoder(Int64EncodingTraits, descr, pool)
	// case Type_INT96:
	// return NewInt96DictDecoder(descr, pool)
	// case Type_FLOAT:
	// return NewFloat32DictDecoder(descr, pool)
	// case Type_DOUBLE:
	// return NewFloat64DictDecoder(descr, pool)
	// case Type_BYTE_ARRAY:
	// return NewByteArrayDictDecoder(descr, pool)
	// case Type_FIXED_LEN_BYTE_ARRAY:
	// return NewFBLADictDecoder(descr, pool)
	default:
		return nil, fmt.Errorf(
			"dict decoder for %s not implemented: %w",
			TypeToString(descr.PhysicalType()),
			ParquetNYIException,
		)
	}
}

var (
	// Encoder
	_ Encoder               = (*Int64PlainEncoder)(nil)
	_ PlainEncoder          = (*Int64PlainEncoder)(nil)
	_ TypedEncoderInterface = (*Int64PlainEncoder)(nil)

	// Decoder
	_ Decoder = (*Int64PlainDecoder)(nil)
	// _ PlainDecoder = (*Int64PlainDecoder)(nil)
	_ TypedDecoderInterface = (*Int64PlainDecoder)(nil)

	_ Decoder               = (*Int64DictDecoder)(nil)
	_ TypedDecoderInterface = (*Int64DictDecoder)(nil)
	_ DictDecoder           = (*Int64DictDecoder)(nil)
)
