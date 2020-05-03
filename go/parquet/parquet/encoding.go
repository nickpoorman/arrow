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
type TypedEncoder interface {
	Put(src interface{}, numValues int) error
	PutSpaced(src interface{}, validBits []byte, validBitsOffset int) error

	EstimatedDataEncodedSize() int
	FlushValues() *memory.Buffer
	PutArrowArray(values array.Interface) error
}

type Decoder interface {
	// Sets the data for a new page. This will be called multiple times on the same
	// decoder and should reset all internal state.
	SetData(numValues int, data []byte, len int)

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

type TypedDecoder interface {
	Decoder

	// Decode values into a buffer
	//
	// Subclasses may override the more specialized Decode methods below.
	//
	// buffer - destination for decoded values
	// maxBalues - maximum number of values to decode
	// returns the number of values decoded. Should be identical to maxValues except
	// at the end of the current data page.
	DecodeBuffer(buffer interface{}, makeValues int) (int, error)

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
		validBits []byte, validBitsOffset int) (int, error)

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

// ----------------------------------------------------------------------
// Base encoder implementation

type encoderBase struct {
	// For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
	descr *ColumnDescriptor
	enc   EncodingType
	pool  memory.Allocator

	// Type length from descr
	typeLength int
}

func newEncoderBase(descr *ColumnDescriptor, encoding EncodingType, pool memory.Allocator) encoderBase {
	typeLength := -1
	if descr != nil {
		typeLength = descr.typeLength()
	}
	return encoderBase{
		descr:      descr,
		enc:        encoding,
		pool:       pool,
		typeLength: typeLength,
	}
}

func (e encoderBase) Encoding() EncodingType       { return e.enc }
func (e encoderBase) MemoryPool() memory.Allocator { return e.pool }

// ----------------------------------------------------------------------
// PlainEncoder<T> implementations

type Int64PlainEncoder struct {
	encoderBase

	// base
	// dtype PhysicalType // TODO: Remove (cleanup)
	sink *array.Int64BufferBuilder
}

func NewInt64PlainEncoder(descr *ColumnDescriptor, pool memory.Allocator) (*Int64PlainEncoder, error) {
	return &Int64PlainEncoder{
		encoderBase: newEncoderBase(descr, EncodingType_PLAIN, pool),

		// dtype: Int64Type, // TODO: Remove (cleanup)
		sink: array.NewInt64BufferBuilder(pool),
	}, nil
}

func (e *Int64PlainEncoder) EstimatedDataEncodedSize() int { return e.sink.Len() }

func (e *Int64PlainEncoder) FlushValues() *memory.Buffer { return e.sink.Finish() }

func (e *Int64PlainEncoder) Put(src interface{}, numValues int) error {
	switch v := src.(type) {
	case []int64:
		return e.PutValues(v)
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

func (e *Int64PlainEncoder) PutValues(values []int64) error {
	e.sink.AppendValues(values)
	return nil
}

func (e *Int64PlainEncoder) PutBuffer(buffer []byte) error {
	e.sink.Append(buffer)
	return nil
}

func (e *Int64PlainEncoder) PutSpaced(
	src interface{}, validBits []byte, validBitsOffset int) error {

	vals, ok := src.([]int64)
	if !ok {
		return fmt.Errorf(
			"PutSpaced: to %s from %T not supported",
			arrow.INT64.String(),
			src,
		)
	}

	return e.PutSpacedValues(vals, validBits, validBitsOffset)
}

func (e *Int64PlainEncoder) PutSpacedValues(
	src []int64, validBits []byte,
	validBitsOffset int) error {
	numValues := len(src)

	buffer := memory.NewResizableBuffer(e.MemoryPool())
	buffer.Resize(arrow.Int64Traits.BytesRequired(numValues))

	numValidValues := 0
	validBitsReader := bitutilext.NewBitmapReader(validBits, validBitsOffset, numValues)
	data := arrow.Int64Traits.CastFromBytes(buffer.Bytes())

	for i := 0; i < numValues; i++ {
		if validBitsReader.IsSet() {
			data[numValidValues] = src[i]
			numValidValues++
		}
		validBitsReader.Next()
	}
	return e.PutValues(data[:numValidValues])
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

	dtype PhysicalType
	sink  *array.Int64BufferBuilder

	// Indices that have not yet be written out by WriteIndices().
	bufferedIndices []int32 // C++ implementation has this as an ArrowPoolVector

	// The number of bytes needed to encode the dictionary.
	dictEncodedSize int

	memoTable utilext.MemoTable
}

//+ func New{{.DType}}DictEncoder(descr *ColumnDescriptor, pool memory.Allocator) (*{{.DType}}DictEncoder, error) {
func NewInt64DictEncoder(descr *ColumnDescriptor, pool memory.Allocator) (*Int64DictEncoder, error) {
	return &Int64DictEncoder{
		encoderBase: newEncoderBase(descr, EncodingType_PLAIN_DICTIONARY, pool),

		dtype: Int64Type,
		sink:  array.NewInt64BufferBuilder(pool),
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
			fmt.Errorf(
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

//+ Specialization required here for: FLBAType, ByteArrayType, Int96Type
//+ func (e *{{.DType}}DictEncoder) PutValue(v interface{}) error {
func (e *Int64DictEncoder) PutValue(v interface{}) error {
	// Put() implementation for primitive types
	onFound := func(memoIndex int32) {}
	onNotFound := func(memoIndex int32) {
		//+ e.dictEncodedSize += {{.Size}}
		e.dictEncodedSize += binary.Size(v) // SIZE_OF_T // TODO: Maybe we generate these?
	}

	var memoIndex int32
	if err := e.memoTable.GetOrInsert(v, onFound, onNotFound, &memoIndex); err != nil {
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
	src interface{}, numValues int, validBits []byte, validBitsOffset int) error {

	srcInt64, ok := src.([]int64)
	if !ok {
		return fmt.Errorf(
			"PutSpaced: to %s from %T not supported",
			arrow.INT64.String(),
			src,
		)
	}

	validBitsReader := bitutilext.NewBitmapReader(validBits, validBitsOffset, numValues)
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
func Int64AssertCanPutDictionary(encoder *Int64DictEncoder, dict array.Interface) error {
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
		var unusedMemoIndex int32
		if err := e.memoTable.GetOrInsert(
			data.Value(i), utilext.OnFoundNoOp,
			utilext.OnNotFoundNoOp, &unusedMemoIndex,
		); err != nil {
			return err
		}
	}

	return nil
}

func (e *Int64DictEncoder) FlushValues() *memory.Buffer {
	buffer := AllocateBuffer(e.pool, int(e.EstimatedDataEncodedSize()))
	resultSize := e.WriteIndices(buffer.Buf(), int(e.EstimatedDataEncodedSize()))
	buffer.ResizeNoShrink(resultSize)
	return buffer
}

// Writes out the encoded dictionary to buffer. buffer must be preallocated to
// DictEncodedSize() bytes.
func (e *Int64DictEncoder) WriteDict(buffer []byte) {
	// For primitive types, only a memcpy
	debug.Assert(e.dictEncodedSize == binary.Size(int64(0))*int(e.memoTable.Size()),
		fmt.Sprintf(
			"Assert: e.dictEncodedSize == binary.Size(int64(0))*int(e.memoTable.Size())"+
				" | %d == %d",
			e.dictEncodedSize, binary.Size(int64(0))*int(e.memoTable.Size()),
		))
	e.memoTable.CopyValues(0 /* startPos */, -1, buffer)
}

// ----------------------------------------------------------------------
// Base decoder implementation

type decoderBase struct {
	// For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
	descr      *ColumnDescriptor
	enc        EncodingType
	numValues  int
	data       []byte
	len        int
	typeLength int
}

func newDecoderBase(descr *ColumnDescriptor, enc EncodingType) *decoderBase {
	return &decoderBase{
		descr: descr,
		enc:   enc,
	}
}

// SetData returns a copy of decoderBase with the updated params.
func (d *decoderBase) SetData(numValues int, data []byte, len int) {
	d.numValues = numValues
	d.data = data
	d.len = len
}

func (d *decoderBase) valuesLeft() int        { return d.numValues }
func (d *decoderBase) Encoding() EncodingType { return d.enc }

// ----------------------------------------------------------------------
// Typed plain decoder implementations

type plainDecoderBase struct {
	dtype PhysicalType
}

func newPlainDecoderBase(dtype PhysicalType) plainDecoderBase {
	return plainDecoderBase{}
}

func (d plainDecoderBase) DecodeSpaced(
	buffer interface{}, numValues int, nullCount int,
	validBits []byte, validBitsOffset int) (int, error) {
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

type Int64PlainDecoder struct {
	decoderBase
	plainDecoderBase
}

func NewInt64PlainDecoder(descr *ColumnDescriptor) (*Int64PlainDecoder, error) {
	return &Int64PlainDecoder{
		decoderBase:      *newDecoderBase(descr, EncodingType_PLAIN),
		plainDecoderBase: newPlainDecoderBase(Int64Type),
	}, nil
}

func (d *Int64PlainDecoder) DecodeBuffer(buffer interface{}, maxValues int) (int, error) {
	if d.numValues < maxValues {
		maxValues = d.numValues
	}
	bytesConsumed, err := d.DecodePlain(d.data, d.len, maxValues, d.typeLength, buffer)
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
func NewInt64DictDecoder(descr *ColumnDescriptor, pool memory.Allocator) (*Int64DictDecoder, error) {
	return &Int64DictDecoder{
		decoderBase:         *newDecoderBase(descr, EncodingType_RLE_DICTIONARY),
		sink:                array.NewInt64BufferBuilder(pool),
		dictionary:          AllocateBuffer(pool, 0),
		dictionaryLength:    0,
		byteArrayData:       AllocateBuffer(pool, 0),
		byteArrayOffsets:    AllocateBuffer(pool, 0),
		indicesScratchSpace: AllocateBuffer(pool, 0),
	}, nil
}

// Perform type-specific initiatialization
func (d *Int64DictDecoder) SetDict(dictionary Int64DictDecoder) {
	panic("not implemented")
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

// TODO(nickpoorman): Maybe buffer here should be an []bytes and then we don't have to do T?
// func (d *Int64DictDecoder) Decode(buffer {{.CType}}, numValues int) (int, error) {
func (d *Int64DictDecoder) Decode(buffer int64, numValues int) (int, error) {
	if d.numValues < numValues {
		numValues = d.numValues
	}

	decodedValues := d.idxDecoder.GetBatchWithDict(
		// arrow.{{.DType}}Traits.CastFromBytes(d.dictionary.Bytes()),
		arrow.Int64Traits.CastFromBytes(d.dictionary.Bytes()),
		d.dictionaryLength, int(buffer), numValues)

	if decodedValues != numValues {
		return 0, ParquetEofException
	}
	d.numValues -= numValues
	return numValues, nil
}

// func (d *Int64DictDecoder) DecodeSpaced(buffer {{.CType}}, numValues int, nullCount int,
func (d *Int64DictDecoder) DecodeSpaced(buffer int64, numValues int, nullCount int,
	validBits []byte, validBitsOffset int64) (int, error) {
	numValues = util.MinInt(numValues, d.numValues)
	if numValues != d.idxDecoder.GetBatchWithDictSpaced(
		// arrow.{{.DType}}Traits.CastFromBytes(d.dictionary.Bytes()),
		arrow.Int64Traits.CastFromBytes(d.dictionary.Bytes()),
		d.dictionaryLength, buffer, numValues, nullCount, validBits,
		validBitsOffset,
	) {
		return 0, ParquetEofException
	}
	d.numValues -= numValues
	return numValues, nil
}

func (d *Int64DictDecoder) DecodeArrow(numValues int, nullCount int, validBits []byte,
	validBitsOffset int64) {
	panic("not implemented")
}

func (d *Int64DictDecoder) DecodeArrowDict(numValues int, nullCount int, validBits []byte,
	validBitsOffset int64) {
	panic("not implemented")
}

func (d *Int64DictDecoder) InsertDictionary(builder array.Builder) {
	panic("not implemented")
}

func (d *Int64DictDecoder) DecodeIndicesSpaced(numValues int, nullCount int,
	validBits []byte, validBitsOffset int64, builder array.Builder) (int, error) {

	if numValues > 0 {
		d.indicesScratchSpace.Resize(
			arrow.Int32Traits.BytesRequired(numValues),
		)
	}

	indicesBuffer := arrow.Int32Traits.CastFromBytes(d.indicesScratchSpace.Buf())

	if numValues != d.idxDecoder.GetBatchSpaced(numValues, nullCount, validBits,
		validBitsOffset, indicesBuffer) {
		return 0, ParquetEofException
	}
	// binaryBuilder := array
	panic("not implemented")
}

// ----------------------------------------------------------------------
// Factory functions

func NewTypedEncoder(
	dtype PhysicalType, encoding EncodingType, useDictionary bool,
	descr *ColumnDescriptor, pool memory.Allocator) (TypedEncoder, error) {

	if pool == nil {
		pool = memory.DefaultAllocator
	}

	encoder, err := NewEncoder(dtype.typeNum(), encoding, useDictionary, descr, pool)
	if err != nil {
		return nil, err
	}
	return encoder.(TypedEncoder), nil
}

// Encode goes from Arrow into Parquet
func NewEncoder(
	typeNum Type, encoding EncodingType, useDictionary bool,
	descr *ColumnDescriptor, pool memory.Allocator) (Encoder, error) {

	if pool == nil {
		pool = memory.DefaultAllocator
	}

	if useDictionary {
		switch typeNum {
		// case Type_INT64:
		// 	return NewEncoderInt64(encoding, useDictionary, descr, pool), nil
		default:
			return nil, fmt.Errorf(
				"dict encoder for %s not implemented: %w",
				TypeToString(typeNum),
				ParquetNYIException,
			)
		}

	} else if encoding == EncodingType_PLAIN {
		return NewPlainEncoder(typeNum, descr, pool)

	} else {
		return nil, fmt.Errorf(
			"Selected encoding is not supported: %w",
			ParquetNYIException,
		)
	}
}

func NewPlainEncoder(
	typeNum Type, descr *ColumnDescriptor,
	pool memory.Allocator) (Encoder, error) {

	switch typeNum {
	// case Type_INT32:
	// return NewInt32PlainEncoder(descr, pool)
	case Type_INT64:
		return NewInt64PlainEncoder(descr, pool)
	default:
		return nil, fmt.Errorf(
			"plain encoder for %s not implemented: %w",
			TypeToString(typeNum),
			ParquetNYIException,
		)
	}
}

func NewTypedDecoder(
	dtype PhysicalType, encoding EncodingType,
	descr *ColumnDescriptor) (TypedDecoder, error) {
	decoder, err := NewDecoder(dtype.typeNum(), encoding, descr)
	if err != nil {
		return nil, err
	}
	return decoder.(TypedDecoder), nil
}

func NewDecoder(
	typeNum Type, encoding EncodingType,
	descr *ColumnDescriptor) (Decoder, error) {

	if encoding != EncodingType_PLAIN {
		return nil, fmt.Errorf(
			"Selected encoding is not supported: %w",
			ParquetNYIException,
		)
	}

	return NewPlainDecoder(typeNum, descr)
}

func NewPlainDecoder(typeNum Type, descr *ColumnDescriptor) (Decoder, error) {
	switch typeNum {
	// case Type_INT32:
	// return NewInt32PlainDecoder(descr, pool)
	case Type_INT64:
		return NewInt64PlainDecoder(descr)
	default:
		return nil, fmt.Errorf(
			"plain encoder for %s not implemented: %w",
			TypeToString(typeNum),
			ParquetNYIException,
		)
	}
}

var (
	// Encoder
	_ Encoder      = (*Int64PlainEncoder)(nil)
	_ PlainEncoder = (*Int64PlainEncoder)(nil)
	_ TypedEncoder = (*Int64PlainEncoder)(nil)

	// Decoder
	_ Decoder = (*Int64PlainDecoder)(nil)
	// _ PlainDecoder = (*Int64PlainDecoder)(nil)
	_ TypedDecoder = (*Int64PlainDecoder)(nil)
)
