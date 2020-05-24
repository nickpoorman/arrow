package bitutil

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/nickpoorman/arrow-parquet-go/internal/bpacking"
	"github.com/nickpoorman/arrow-parquet-go/internal/bytearray"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
)

// Maximum byte length of a vlq encoded int
const KMaxVlqByteLength = 5

// Utility class to write bit/byte streams.  This class can write data to either be
// bit packed or byte aligned (and a single stream that has a mix of both).
// This class does not allocate memory.
type BitWriter struct {
	buffer   []byte
	maxBytes int

	// Bit-packed values are initially written to this variable before being memcpy'd to
	// buffer_. This is faster than writing values byte by byte directly to buffer_.
	// bufferedValues uint64
	bufferedValues []byte

	byteOffset int // Offset in buffer_
	bitOffset  int // Offset in buffered_values_
}

// buffer: buffer to write bits to.  Buffer should be preallocated with
// 'buffer_len' bytes.
func NewBitWriter(buffer []byte, bufferLen int) *BitWriter {
	b := &BitWriter{
		buffer:         buffer,
		maxBytes:       bufferLen,
		bufferedValues: make([]byte, 8),
	}
	b.Clear()
	return b
}

func (b *BitWriter) Clear() {
	b.zeroBufferedValues()
	b.byteOffset = 0
	b.bitOffset = 0
}

// The number of current bytes written, including the current byte (i.e. may include a
// fraction of a byte). Includes buffered values.
func (b *BitWriter) BytesWritten() int {
	return b.byteOffset + int(bitutil.BytesForBits(int64(b.bitOffset)))
}

// Writes a value to buffered_values_, flushing to buffer_ if necessary.  This is bit
// packed.  Returns false if there was not enough space. num_bits must be <= 32.
func (b *BitWriter) PutValue(v uint64, numBits int) bool {
	// TODO: revisit this limit if necessary (can be raised to 64 by fixing some edge cases)
	debug.Assert(numBits <= 32, "Assert: numBits <= 32")
	debug.Assert(v>>numBits == 0, "Assert: v >> numBits == 0")

	if b.byteOffset*8+b.bitOffset+numBits > b.maxBytes*8 {
		return false
	}

	// b.bufferedValues |= v << b.bitOffset
	b.setBufferedValues(b.bufferedValuesUint64() | (v << b.bitOffset))
	b.bitOffset += numBits

	if b.bitOffset >= 64 {
		// Flush buffered_values_ and write out bits of v that did not fit
		copy(b.buffer[b.byteOffset:], b.bufferedValues[:8])
		b.byteOffset += 8
		b.bitOffset -= 64
		b.setBufferedValues(v >> (numBits - b.bitOffset))
	}
	debug.Assert(b.bitOffset < 64, "Assert: b.bitOffset < 64")
	return true
}

func (b *BitWriter) bufferedValuesUint64() uint64 {
	return binary.LittleEndian.Uint64(b.bufferedValues)
}

func (b *BitWriter) setBufferedValues(v uint64) {
	binary.LittleEndian.PutUint64(b.bufferedValues, v)
}

func (b *BitWriter) zeroBufferedValues() {
	v := b.bufferedValues
	_ = v[7] // early bounds check to guarantee safety of writes below
	v[0] = 0
	v[1] = 0
	v[2] = 0
	v[3] = 0
	v[4] = 0
	v[5] = 0
	v[6] = 0
	v[7] = 0
}

// Writes v to the next aligned byte using num_bytes. If T is larger than
// num_bytes, the extra high-order bytes will be ignored. Returns false if
// there was not enough space.
func (b *BitWriter) PutAligned(v interface{}, numBytes int) bool {
	ptr := b.GetNextBytePtr(numBytes)
	if ptr == nil {
		return false
	}
	writeToBuffer(ptr, binary.LittleEndian, v)
	return true
}

// Write a Vlq encoded int to the buffer.  Returns false if there was not enough
// room.  The value is written byte aligned.
// For more details on vlq:
// en.wikipedia.org/wiki/Variable-length_quantity
func (b *BitWriter) PutVlqInt(v uint32) bool {
	result := true
	for (v & 0xFFFFFF80) != 0 {
		result = result && b.PutAligned(uint8((v&0x7F)|0x80), 1)
		v >>= 7
	}
	result = result && b.PutAligned(uint8(v&0x7F), 1)
	return result
}

// Writes an int zigzag encoded.
func (b *BitWriter) PutZigZagVlqInt(v int32) bool {
	uv := uint32(v)
	return b.PutVlqInt((uv << 1) ^ (uv >> 31))
}

// Get a pointer to the next aligned byte and advance the underlying buffer
// by num_bytes.
// Returns NULL if there was not enough space.
func (b *BitWriter) GetNextBytePtr(numBytes int /* default to 1 */) []byte {
	b.Flush( /* align */ true)
	debug.Assert(b.byteOffset <= b.maxBytes, "Assert: b.byteOffset <= b.maxBytes")
	if b.byteOffset+numBytes > b.maxBytes {
		return nil
	}
	ptr := b.buffer[b.byteOffset:]
	b.byteOffset += numBytes
	return ptr
}

// Flushes all buffered values to the buffer. Call this when done writing to
// the buffer.  If 'align' is true, buffered_values_ is reset and any future
// writes will be written to the next byte boundary.
func (b *BitWriter) Flush(align bool) {
	numBytes := int(bitutil.BytesForBits(int64(b.bitOffset)))
	debug.Assert(b.byteOffset+numBytes <= b.maxBytes, "Assert: b.byteOffset + numBytes <= b.maxBytes")
	copy(b.buffer[b.byteOffset:], b.bufferedValues[:numBytes])

	if align {
		b.zeroBufferedValues()
		b.byteOffset += numBytes
		b.bitOffset = 0
	}
}

func (b *BitWriter) Buffer() []byte {
	return b.buffer
}

func (b *BitWriter) BufferLen() int {
	return b.maxBytes
}

// TODO(nickpoorman): Write tests to verify this is working.
// func getValue(numBits int, v reflect.Value, maxBytes int, buffer []byte,
// 	bitOffset *int, byteOffset *int, bufferedValues []uint64) {

// 	// v[0] = bitutilext.TrailingBits(bufferedValues[0], *bitOffset+numBits) >> *bitOffset
// 	v.Set(reflect.ValueOf(TrailingBits(bufferedValues[0], *bitOffset+numBits) >> *bitOffset).Convert(v.Type()))

// 	*bitOffset += numBits
// 	if *bitOffset >= 64 {
// 		*byteOffset += 8
// 		*bitOffset -= 64

// 		bytesRemaining := maxBytes - *byteOffset
// 		if bytesRemaining < 8 {
// 			copy(arrow.Uint64Traits.CastToBytes(bufferedValues), buffer[*byteOffset:*byteOffset+bytesRemaining])
// 		} else { // predict true
// 			copy(arrow.Uint64Traits.CastToBytes(bufferedValues), buffer[*byteOffset:*byteOffset+8])
// 		}

// 		// Read bits of v that crossed into new buffered_values_
// 		// v[0] = v[0] | bitutilext.TrailingBits(bufferedValues[0], *bitOffset)<<(numBits-*bitOffset)
// 		v.Set(
// 			reflect.ValueOf(
// 				v.Convert(reflect.TypeOf(uint64(0))).Uint() |
// 					TrailingBits(
// 						bufferedValues[0], *bitOffset,
// 					)<<(numBits-*bitOffset),
// 			).Convert(v.Type()),
// 		)
// 		debug.Assert(*bitOffset <= 64, "Assert: *bitOffset <= 64")
// 	}
// }

// func getValue(numBits int, v reflect.Value, maxBytes int, buffer []byte,
// 	bitOffset *int, byteOffset *int, bufferedValues []byte) {

// 	// v[0] = bitutilext.TrailingBits(bufferedValues[0], *bitOffset+numBits) >> *bitOffset
// 	v.Set(
// 		reflect.ValueOf(
// 			TrailingBits(
// 				binary.LittleEndian.Uint64(bufferedValues), *bitOffset+numBits,
// 			) >> *bitOffset,
// 		).Convert(v.Type()),
// 	)

// 	*bitOffset += numBits
// 	if *bitOffset >= 64 {
// 		*byteOffset += 8
// 		*bitOffset -= 64

// 		bytesRemaining := maxBytes - *byteOffset
// 		if bytesRemaining < 8 {
// 			// copy(arrow.Uint64Traits.CastToBytes(bufferedValues), buffer[*byteOffset:*byteOffset+bytesRemaining])
// 			copy(bufferedValues, buffer[*byteOffset:*byteOffset+bytesRemaining])
// 		} else { // predict true
// 			// copy(arrow.Uint64Traits.CastToBytes(bufferedValues), buffer[*byteOffset:*byteOffset+8])
// 			copy(bufferedValues, buffer[*byteOffset:*byteOffset+8])
// 		}

// 		// Read bits of v that crossed into new buffered_values_
// 		// v[0] = v[0] | bitutilext.TrailingBits(bufferedValues[0], *bitOffset)<<(numBits-*bitOffset)
// 		v.Set(
// 			reflect.ValueOf(
// 				v.Convert(reflect.TypeOf(uint64(0))).Uint() |
// 					TrailingBits(
// 						binary.LittleEndian.Uint64(bufferedValues), *bitOffset,
// 					)<<(numBits-*bitOffset),
// 			).Convert(v.Type()),
// 		)
// 		debug.Assert(*bitOffset <= 64, "Assert: *bitOffset <= 64")
// 	}
// }

func getValue(numBits int, v bytearray.ByteArray, maxBytes int, buffer []byte,
	bitOffset *int, byteOffset *int, bufferedValues []byte) {

	// switch vT := v.(type) {
	// case *bool:
	// 	debug.Print("v is: %t\n", *vT)
	// case *int32:
	// 	// debug.Print("v is: %d\n", *vT)
	// 	debug.Print("getValue-first - numBits: %d | v(num): %d | v: %#b | maxBytes: %d | buffer:$#b | bitOffset: %d | byteOffset: %d | bufferedValues: $#b\n",
	// 		numBits, *vT, *vT, maxBytes,
	// 		// buffer,
	// 		*bitOffset, *byteOffset,
	// 		// bufferedValues,
	// 	)
	// }

	// v[0] = bitutilext.TrailingBits(bufferedValues[0], *bitOffset+numBits) >> *bitOffset
	// v.Set(
	// 	reflect.ValueOf(
	// 		TrailingBits(
	// 			binary.LittleEndian.Uint64(bufferedValues), *bitOffset+numBits,
	// 		) >> *bitOffset,
	// 	).Convert(v.Type()),
	// )
	trailing := TrailingBits(
		binary.LittleEndian.Uint64(bufferedValues), *bitOffset+numBits,
	) >> *bitOffset
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], trailing)
	// binary.LittleEndian.Uint64(bs)

	// readFromBuffer(buf[:], binary.LittleEndian, v)
	v.Copy(buf[:])

	// switch vT := v.(type) {
	// case *uint64:
	// 	// debug.Print("v is: %d\n", *vT)
	// 	debug.Print("getValue-second - numBits: %d | v(num): %d | v: %#b | maxBytes: %d | buffer: %#b | bitOffset: %d | byteOffset: %d | bufferedValues: %#b\n",
	// 		numBits, *vT, *vT, maxBytes, buffer, *bitOffset, *byteOffset, bufferedValues)
	// }

	*bitOffset += numBits
	if *bitOffset >= 64 {
		debug.Print("*bitOffset >= 64: %d\n", *bitOffset)
		*byteOffset += 8
		*bitOffset -= 64

		bytesRemaining := maxBytes - *byteOffset
		if bytesRemaining < 8 {
			// copy(arrow.Uint64Traits.CastToBytes(bufferedValues), buffer[*byteOffset:*byteOffset+bytesRemaining])
			copy(bufferedValues, buffer[*byteOffset:*byteOffset+bytesRemaining])
		} else { // predict true
			// copy(arrow.Uint64Traits.CastToBytes(bufferedValues), buffer[*byteOffset:*byteOffset+8])
			copy(bufferedValues, buffer[*byteOffset:*byteOffset+8])
		}

		// Read bits of v that crossed into new buffered_values_
		// v[0] = v[0] | bitutilext.TrailingBits(bufferedValues[0], *bitOffset)<<(numBits-*bitOffset)
		// v.Set(
		// 	reflect.ValueOf(
		// 		v.Convert(reflect.TypeOf(uint64(0))).Uint() |
		// 			TrailingBits(
		// 				binary.LittleEndian.Uint64(bufferedValues), *bitOffset,
		// 			)<<(numBits-*bitOffset),
		// 	).Convert(v.Type()),
		// )
		b := TrailingBits(
			binary.LittleEndian.Uint64(bufferedValues), *bitOffset,
		) << (numBits - *bitOffset)

		// var buf3 [8]byte
		// writeToBuffer(buf3[:], binary.LittleEndian, reflect.ValueOf(v).Elem().Interface())
		// bo := binary.LittleEndian.Uint64(buf3[:]) | b
		bo := v.Uint64() | b

		// // bo := reflect.ValueOf(v).Elem().Convert(reflect.TypeOf(uint64(0))).Uint() | b
		var buf2 [8]byte
		binary.LittleEndian.PutUint64(buf2[:], bo)
		// // debug.Print("v was: %v\n", *v.(*int32))
		// readFromBuffer(buf2[:], binary.LittleEndian, v)
		// // debug.Print("v is now: %v\n", *v.(*int32))
		v.Copy(buf2[:])

		debug.Assert(*bitOffset <= 64, "Assert: *bitOffset <= 64")
	}

	// switch vT := v.(type) {
	// case *uint64:
	// 	// debug.Print("v is: %d\n", *vT)
	// 	debug.Print("getValue-third - numBits: %d | v(num): %d | v: %#b | maxBytes: %d | buffer: %#b | bitOffset: %d | byteOffset: %d | bufferedValues: %#b\n",
	// 		numBits, *vT, *vT, maxBytes, buffer, *bitOffset, *byteOffset, bufferedValues)
	// }
}

// Utility class to read bit/byte stream.  This class can read bits or bytes
// that are either byte aligned or not.  It also has utilities to read multiple
// bytes in one read (e.g. encoded int).
type BitReader struct {
	buffer   []byte
	maxBytes int

	// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
	// faster than reading values byte by byte directly from buffer_.
	// bufferedValues uint64
	bufferedValues []byte

	byteOffset int // Offset in buffer_
	bitOffset  int // Offset in buffered_values_
}

// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
func NewBitReader(buffer []byte, bufferLen int) *BitReader {
	b := &BitReader{
		buffer:         buffer,
		maxBytes:       bufferLen,
		byteOffset:     0,
		bitOffset:      0,
		bufferedValues: make([]byte, 8),
	}
	numBytes := 8
	if b.maxBytes-b.byteOffset < numBytes {
		numBytes = b.maxBytes - b.byteOffset
	}
	copy(b.bufferedValues, b.buffer[b.byteOffset:b.byteOffset+numBytes])

	return b
}

func (b *BitReader) Reset(buffer []byte, bufferLen int) {
	b.buffer = buffer
	b.maxBytes = bufferLen
	b.byteOffset = 0
	b.bitOffset = 0
	numBytes := 8
	if b.maxBytes-b.byteOffset < numBytes {
		numBytes = b.maxBytes - b.byteOffset
	}
	copy(b.bufferedValues, b.buffer[b.byteOffset:b.byteOffset+numBytes])
}

// // Gets the next value from the buffer.  Returns true if 'v' could be read or false if
// // there are not enough bytes left. num_bits must be <= 32.
// func (b *BitReader) GetValue(numBits int, v interface{}) bool {
// 	// Make v is a slice and not a pointer to a concrete type
// 	// if vB, ok := v.(*bool); ok {
// 	// 	uv := []uint32{0}
// 	// 	if *vB {
// 	// 		uv[0] = 1
// 	// 	}
// 	// 	results := b.GetBatch(numBits, uv, 1) == 1
// 	// 	if results {
// 	// 		*vB = uv[0] != 0
// 	// 	}
// 	// 	return results
// 	// } else {
// 	value := reflect.ValueOf(v)
// 	elem := value.Elem()
// 	sl := reflect.MakeSlice(reflect.SliceOf(elem.Type()), 1, 1)
// 	sl.Index(0).Set(elem)
// 	result := b.GetBatch(numBits, sl.Interface(), 1) == 1
// 	value.Elem().Set(sl.Index(0)) // set the result of the first element in the slice back to v
// 	return result
// 	// }
// }

// Gets the next value from the buffer.  Returns true if 'v' could be read or false if
// there are not enough bytes left. num_bits must be <= 32.
func (b *BitReader) GetValue(numBits int, v bytearray.ByteArray) bool {
	// value := reflect.ValueOf(v)
	// elem := value.Elem()
	// sl := reflect.MakeSlice(reflect.SliceOf(elem.Type()), 1, 1)
	// sl.Index(0).Set(elem)
	// result := b.GetBatch(numBits, sl.Interface(), 1) == 1
	// value.Elem().Set(sl.Index(0)) // set the result of the first element in the slice back to v

	result := b.GetBatch(numBits, v, 1) == 1
	return result
	// }
}

// // Get a number of values from the buffer. Return the number of values actually read.
// func (b *BitReader) GetBatch(numBits int, v interface{}, batchSize int) int {
// 	debug.Assert(b.buffer != nil, "buffer must not be nil")
// 	// TODO: revisit this limit if necessary
// 	debug.Assert(numBits <= 32, "numBits must be less than or equal to 32")
// 	debug.Assert(numBits <= binary.Size(v)*8, "numBits must be less than or queal to the size of v")

// 	debug.Print("GetBatch-frist - numBits: %d | batchSize: %d\n", numBits, batchSize)

// 	bitOffset := b.bitOffset
// 	byteOffset := b.byteOffset
// 	bufferedValues := b.bufferedValues
// 	// bufferedValuesUint64 := arrow.Uint64Traits.CastFromBytes(bufferedValues)
// 	maxBytes := b.maxBytes
// 	buffer := b.buffer

// 	neededBits := numBits * batchSize
// 	remainingBits := (maxBytes-byteOffset)*8 - bitOffset
// 	if remainingBits < neededBits {
// 		batchSize = remainingBits / numBits
// 	}

// 	// debug.Print("GetBatch-second - numBits: %d | batchSize: %d | bitOffset: %d | byteOffset: %d | bufferedValues: %#b | maxBytes: %d | buffer: %#b | neededBits: %d | remainingBits: %d\n",
// 	// 	numBits, batchSize, bitOffset, byteOffset, bufferedValues, maxBytes, buffer, neededBits, remainingBits)

// 	vSlice := reflect.ValueOf(v)
// 	debug.Print("vSlice kind: %v | type.kind: %v | indirect: %v\n", vSlice.Kind(), vSlice.Type().Kind(), reflect.Indirect(vSlice).Kind())

// 	// TODO: Remove
// 	// var vUint64 []uint64
// 	// // Everything below needs v to be a uint64
// 	// switch vT := v.(type) {
// 	// case []int32:
// 	// 	vUint64 = arrow.Uint64Traits.CastFromBytes(arrow.Int32Traits.CastToBytes(vT))
// 	// case []uint32:
// 	// 	vUint64 = arrow.Uint64Traits.CastFromBytes(arrow.Uint32Traits.CastToBytes(vT))
// 	// case []float32:
// 	// 	vUint64 = arrow.Uint64Traits.CastFromBytes(arrow.Float32Traits.CastToBytes(vT))
// 	// default:
// 	// 	panic(fmt.Errorf("BitReader.GetBatch(): cast to uint64: Unhandled type : %T", v))
// 	// }

// 	i := 0
// 	if bitOffset != 0 {
// 		debug.Print("bitOffset != 0")
// 		for ; i < batchSize && bitOffset != 0; i++ {
// 			debug.Print("bitOffset != 0 .... getValue - i: %d | batchSize: %d | bitOffset: %d | \n", i, batchSize, bitOffset)
// 			getValue(numBits, vSlice.Index(i).Addr().Interface(), maxBytes, buffer, &bitOffset, &byteOffset, bufferedValues)
// 			// debug.Print("v is now: %#b\n", v.([]uint64)[i])
// 		}
// 	}

// 	switch vT := v.(type) {
// 	case []int32: // sizeof(T) == 4
// 		debug.Print("In int32")
// 		numUnpacked := bpacking.Unpack32(
// 			arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
// 			arrow.Uint32Traits.CastFromBytes(arrow.Int32Traits.CastToBytes(vT))[i:],
// 			batchSize-i, numBits)
// 		i += numUnpacked
// 		byteOffset += numUnpacked * numBits / 8
// 	case []uint32: // sizeof(T) == 4
// 		debug.Print("In uint32")
// 		numUnpacked := bpacking.Unpack32(
// 			arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
// 			arrow.Uint32Traits.CastFromBytes(arrow.Uint32Traits.CastToBytes(vT))[i:],
// 			batchSize-i, numBits)
// 		i += numUnpacked
// 		byteOffset += numUnpacked * numBits / 8
// 	case []float32: // sizeof(T) == 4
// 		debug.Print("In float32")
// 		numUnpacked := bpacking.Unpack32(
// 			arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
// 			arrow.Uint32Traits.CastFromBytes(arrow.Float32Traits.CastToBytes(vT))[i:],
// 			batchSize-i, numBits)
// 		i += numUnpacked
// 		byteOffset += numUnpacked * numBits / 8
// 	default:
// 		debug.Print("In default. i: %d | batchSize: %d\n", i, batchSize)
// 		const bufferSize int = 1024
// 		var unpackBuffer [bufferSize]uint32
// 		for i < batchSize {
// 			unpackSize := batchSize - i
// 			if bufferSize < unpackSize {
// 				unpackSize = bufferSize
// 			}
// 			numUnpacked := bpacking.Unpack32(
// 				arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
// 				unpackBuffer[:], unpackSize, numBits,
// 			)
// 			debug.Print("numUnpacked: ", numUnpacked)
// 			if numUnpacked == 0 {
// 				break
// 			}
// 			for k := 0; k < numUnpacked; k++ {
// 				// v[i+k] = unpackBuffer[k]
// 				switch vT := v.(type) {
// 				case []bool:
// 					vT[i+k] = unpackBuffer[k] != 0
// 				default:
// 					e := vSlice.Index(i + k)
// 					e.Set(
// 						reflect.ValueOf(unpackBuffer[k]).Convert(e.Type()),
// 					)
// 				}
// 			}
// 			i += numUnpacked
// 			byteOffset += numUnpacked * numBits / 8
// 		}
// 	}

// 	bytesRemaining := maxBytes - byteOffset
// 	debug.Print("maxBytes: %d | byteOffset: %d | bytesRemaining: %d\n", maxBytes, byteOffset, bytesRemaining)
// 	if bytesRemaining >= 8 {
// 		copy(bufferedValues, buffer[byteOffset:byteOffset+8])
// 	} else {
// 		copy(bufferedValues, buffer[byteOffset:byteOffset+bytesRemaining])
// 	}

// 	for ; i < batchSize; i++ {
// 		// debug.Print("lastloop - i: %d | batchSize: %d\n", i, batchSize)
// 		value := vSlice.Index(i).Addr().Interface()
// 		// if vSlice.Kind() == reflect.Ptr
// 		getValue(numBits, value,
// 			maxBytes, buffer, &bitOffset, &byteOffset, bufferedValues)
// 	}

// 	b.bitOffset = bitOffset
// 	b.byteOffset = byteOffset
// 	b.bufferedValues = bufferedValues

// 	debug.Print("GetBatch-returning - bitOffset: %d | byteOffset: %d | bufferedValues: %#b\n", b.bitOffset, b.byteOffset, b.bufferedValues)

// 	return batchSize
// }

// Get a number of values from the buffer. Return the number of values actually read.
func (b *BitReader) GetBatch(numBits int, v bytearray.ByteArray, batchSize int) int {
	debug.Assert(b.buffer != nil, "buffer must not be nil")
	// TODO: revisit this limit if necessary
	debug.Assert(numBits <= 32, "numBits must be less than or equal to 32")
	// debug.Assert(numBits <= binary.Size(v)*8, "numBits must be less than or equal to the size of v")
	debug.Assert(numBits <= v.BytesSize()*8, "numBits must be less than or equal to the size of v")

	debug.Print("GetBatch-frist - numBits: %d | batchSize: %d\n", numBits, batchSize)

	bitOffset := b.bitOffset
	byteOffset := b.byteOffset
	bufferedValues := b.bufferedValues
	// bufferedValuesUint64 := arrow.Uint64Traits.CastFromBytes(bufferedValues)
	maxBytes := b.maxBytes
	buffer := b.buffer

	neededBits := numBits * batchSize
	remainingBits := (maxBytes-byteOffset)*8 - bitOffset
	if remainingBits < neededBits {
		batchSize = remainingBits / numBits
	}

	// debug.Print("GetBatch-second - numBits: %d | batchSize: %d | bitOffset: %d | byteOffset: %d | bufferedValues: %#b | maxBytes: %d | buffer: %#b | neededBits: %d | remainingBits: %d\n",
	// 	numBits, batchSize, bitOffset, byteOffset, bufferedValues, maxBytes, buffer, neededBits, remainingBits)

	// vSlice := reflect.ValueOf(v)
	// debug.Print("vSlice kind: %v | type.kind: %v | indirect: %v\n", vSlice.Kind(), vSlice.Type().Kind(), reflect.Indirect(vSlice).Kind())

	// TODO: Remove
	// var vUint64 []uint64
	// // Everything below needs v to be a uint64
	// switch vT := v.(type) {
	// case []int32:
	// 	vUint64 = arrow.Uint64Traits.CastFromBytes(arrow.Int32Traits.CastToBytes(vT))
	// case []uint32:
	// 	vUint64 = arrow.Uint64Traits.CastFromBytes(arrow.Uint32Traits.CastToBytes(vT))
	// case []float32:
	// 	vUint64 = arrow.Uint64Traits.CastFromBytes(arrow.Float32Traits.CastToBytes(vT))
	// default:
	// 	panic(fmt.Errorf("BitReader.GetBatch(): cast to uint64: Unhandled type : %T", v))
	// }

	i := 0
	if bitOffset != 0 {
		debug.Print("bitOffset != 0")
		for ; i < batchSize && bitOffset != 0; i++ {
			debug.Print("bitOffset != 0 .... getValue - i: %d | batchSize: %d | bitOffset: %d | \n", i, batchSize, bitOffset)
			// getValue(numBits, vSlice.Index(i).Addr().Interface(), maxBytes, buffer, &bitOffset, &byteOffset, bufferedValues)
			getValue(numBits, v.ElementAt(i), maxBytes, buffer, &bitOffset, &byteOffset, bufferedValues)
			// debug.Print("v is now: %#b\n", v.([]uint64)[i])
		}
	}

	// switch vT := v.(type) {
	// case []int32: // sizeof(T) == 4
	// 	debug.Print("In int32")
	// 	numUnpacked := bpacking.Unpack32(
	// 		arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
	// 		arrow.Uint32Traits.CastFromBytes(arrow.Int32Traits.CastToBytes(vT))[i:],
	// 		batchSize-i, numBits)
	// 	i += numUnpacked
	// 	byteOffset += numUnpacked * numBits / 8
	// case []uint32: // sizeof(T) == 4
	// 	debug.Print("In uint32")
	// 	numUnpacked := bpacking.Unpack32(
	// 		arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
	// 		arrow.Uint32Traits.CastFromBytes(arrow.Uint32Traits.CastToBytes(vT))[i:],
	// 		batchSize-i, numBits)
	// 	i += numUnpacked
	// 	byteOffset += numUnpacked * numBits / 8
	// case []float32: // sizeof(T) == 4
	// 	debug.Print("In float32")
	// 	numUnpacked := bpacking.Unpack32(
	// 		arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
	// 		arrow.Uint32Traits.CastFromBytes(arrow.Float32Traits.CastToBytes(vT))[i:],
	// 		batchSize-i, numBits)
	// 	i += numUnpacked
	// 	byteOffset += numUnpacked * numBits / 8
	// default:
	debug.Print("In default. i: %d | batchSize: %d\n", i, batchSize)
	const bufferSize int = 1024
	var unpackBuffer [bufferSize]uint32
	isBool := v.ElementSize() == 1
	for i < batchSize {
		unpackSize := batchSize - i
		if bufferSize < unpackSize {
			unpackSize = bufferSize
		}
		numUnpacked := bpacking.Unpack32(
			arrow.Uint32Traits.CastFromBytes(buffer[byteOffset:]),
			unpackBuffer[:], unpackSize, numBits,
		)
		debug.Print("numUnpacked: ", numUnpacked)
		if numUnpacked == 0 {
			break
		}

		// v[i+k] = unpackBuffer[k]
		// switch vT := v.(type) {
		// case []bool:
		// 	vT[i+k] = unpackBuffer[k] != 0
		// default:
		// 	e := vSlice.Index(i + k)
		// 	e.Set(
		// 		reflect.ValueOf(unpackBuffer[k]).Convert(e.Type()),
		// 	)
		// }

		if isBool {
			for k := 0; k < numUnpacked; k++ {
				v.ElementAt(i + k).PutBool(unpackBuffer[k] != 0)
			}
		} else {
			for k := 0; k < numUnpacked; k++ {
				v.ElementAt(i + k).PutUint32(unpackBuffer[k])
			}
		}

		// TODO: We may be able to optimize by working with bytes directly
		// v.ElementsSlice(i).Copy(
		// 	arrow.Uint32Traits.CastToBytes(unpackBuffer[:numUnpacked]),
		// )

		i += numUnpacked
		byteOffset += numUnpacked * numBits / 8
	}
	// }

	bytesRemaining := maxBytes - byteOffset
	debug.Print("maxBytes: %d | byteOffset: %d | bytesRemaining: %d\n", maxBytes, byteOffset, bytesRemaining)
	if bytesRemaining >= 8 {
		copy(bufferedValues, buffer[byteOffset:byteOffset+8])
	} else {
		copy(bufferedValues, buffer[byteOffset:byteOffset+bytesRemaining])
	}

	for ; i < batchSize; i++ {
		// debug.Print("lastloop - i: %d | batchSize: %d\n", i, batchSize)
		// value := vSlice.Index(i).Addr().Interface()

		// if vSlice.Kind() == reflect.Ptr
		// getValue(numBits, value,
		getValue(numBits, v.ElementAt(i),
			maxBytes, buffer, &bitOffset, &byteOffset, bufferedValues)
	}

	b.bitOffset = bitOffset
	b.byteOffset = byteOffset
	b.bufferedValues = bufferedValues

	debug.Print("GetBatch-returning - bitOffset: %d | byteOffset: %d | bufferedValues: %#b\n", b.bitOffset, b.byteOffset, b.bufferedValues)

	return batchSize
}

// Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T
// needs to be a little-endian native type and big enough to store
// 'num_bytes'. The value is assumed to be byte-aligned so the stream will
// be advanced to the start of the next byte before 'v' is read. Returns
// false if there are not enough bytes left.
// v can be a
func (b *BitReader) GetAligned(numBytes int, v interface{}) bool {
	// TODO: If v is a single byte there might be a faster path we can implement

	if numBytes > binary.Size(v) {
		debug.Print("numBytes > binary.Size(v): numBytes: %d | Size(v): %d\n", numBytes, binary.Size(v))
		return false
	}
	debug.Print("GetAligned - numBytes: %d | v: %T\n", numBytes, v)
	bytesRead := int(bitutil.BytesForBits(int64(b.bitOffset)))
	if b.byteOffset+bytesRead+numBytes > b.maxBytes {
		debug.Print(
			"b.byteOffset+bytesRead+numBytes > b.maxBytes - filled buffer, returning "+
				"b.byteOffset: %d | bytesRead: %d | numBytes: %d | b.maxBytes: %d\n",
			b.byteOffset, bytesRead, numBytes, b.maxBytes)
		return false
	}

	// TODO: Remove
	// switch vT := v.(type) {
	// case *byte:
	// 	if numBytes > 1 {
	// 		return false
	// 	}
	// 	// Advance byte_offset to next unread byte and read num_bytes
	// 	b.byteOffset += bytesRead
	// 	*vT = b.buffer[b.byteOffset]
	// case []byte:
	// 	if numBytes > len(vT) {
	// 		return false
	// 	}
	// 	// Advance byte_offset to next unread byte and read num_bytes
	// 	b.byteOffset += bytesRead
	// 	copy(vT, b.buffer[b.byteOffset:b.byteOffset+numBytes])
	// default:
	// 	panic(fmt.Sprintf("GetAligned: unhandled type %T", v))
	// }

	// Advance byte_offset to next unread byte and read num_bytes
	b.byteOffset += bytesRead
	debug.Print("byteOffset: %d | end: %d\n", b.byteOffset, b.byteOffset+numBytes)

	readFromBuffer(b.buffer[b.byteOffset:b.byteOffset+numBytes], binary.LittleEndian, v)

	b.byteOffset += numBytes

	// Reset buffered_values_
	b.bitOffset = 0
	bytesRemaining := b.maxBytes - b.byteOffset
	if bytesRemaining < 8 {
		// b.bufferedValues, _ = binary.Uvarint(b.buffer[b.byteOffset : b.byteOffset+bytesRemaining])
		copy(b.bufferedValues, b.buffer[b.byteOffset:b.byteOffset+bytesRemaining])
	} else {
		// b.bufferedValues, _ = binary.Uvarint(b.buffer[b.byteOffset:8])
		copy(b.bufferedValues, b.buffer[b.byteOffset:b.byteOffset+8])
	}
	return true
}

// Reads a vlq encoded int from the stream.  The encoded int must start at
// the beginning of a byte. Return false if there were not enough bytes in
// the buffer.
func (b *BitReader) GetVlqInt() (uint32, bool) {
	debug.Print("called GetVlqInt")
	var tmp uint32 = 0
	for i := 0; i < KMaxVlqByteLength; i++ {
		var bite byte
		if !b.GetAligned(1, &bite) {
			return tmp, false
		}
		tmp |= uint32(bite&0x7F) << (7 * i)

		if (bite & 0x80) == 0 {
			return tmp, true
		}
	}

	return tmp, false
}

// Reads a zigzag encoded int `into` v.
func (b *BitReader) GetZigZagVlqInt() (int32, bool) {
	u, ok := b.GetVlqInt()
	if !ok {
		return 0, false
	}

	return int32((u >> 1) ^ (u << 31)), true
}

// Returns the number of bytes left in the stream, not including the current
// byte (i.e., there may be an additional fraction of a byte).
func (b *BitReader) BytesLeft() int {
	return b.maxBytes - (b.byteOffset + int(bitutil.BytesForBits(int64(b.bitOffset))))
}

func writeToBuffer(bs []byte, order binary.ByteOrder, data interface{}) {
	// https://golang.org/src/encoding/binary/binary.go?s=7891:7955#L261
	switch v := data.(type) {
	case bool:
		if v {
			bs[0] = 1
		} else {
			bs[0] = 0
		}
	case int8:
		bs[0] = byte(v)
	case uint8:
		bs[0] = v
	case int16:
		order.PutUint16(bs, uint16(v))
	case uint16:
		order.PutUint16(bs, v)
	case int32:
		order.PutUint32(bs, uint32(v))
	case uint32:
		order.PutUint32(bs, v)
	case int64:
		order.PutUint64(bs, uint64(v))
	case int: // cast int to int64
		order.PutUint64(bs, uint64(v))
	case uint64:
		order.PutUint64(bs, v)
	case float32:
		order.PutUint32(bs, math.Float32bits(v))
	case float64:
		order.PutUint64(bs, math.Float64bits(v))
	default:
		panic(fmt.Sprintf("writeToBuffer: unknown type: %T", data))
	}
}

func readFromBuffer(bs []byte, order binary.ByteOrder, data interface{}) {
	// debug.Print("readFromBuffer-first - bs: %#b - len: %d | dataT: %T\n", bs, len(bs), data)
	if len(bs) == 0 {
		// There's nothing in the buffer to read
		return
	}
	switch data := data.(type) {
	case *bool:
		*data = bs[0] != 0
	case *int8:
		*data = int8(bs[0])
	case *uint8:
		*data = bs[0]
	case *int16:
		*data = int16(order.Uint16(bs))
	case *uint16:
		*data = order.Uint16(bs)
	case *int32:
		// debug.Print("readFromBuffer-second - bs: %#b | data: %d\n", bs, *data)
		*data = int32(order.Uint32(bs))
	case *uint32:
		*data = order.Uint32(bs)
	case *int64:
		*data = int64(order.Uint64(bs))
	case *int: // assuming int is an int64
		*data = int(order.Uint64(bs))
	case *uint64:
		// debug.Print("readFromBuffer-second - bs: %#b | data: %#b\n", bs, *data)
		*data = order.Uint64(bs)
	case *float32:
		*data = math.Float32frombits(order.Uint32(bs))
	case *float64:
		*data = math.Float64frombits(order.Uint64(bs))
	case []bool:
		for i, x := range bs { // Easier to loop over the input for 8-bit values.
			data[i] = x != 0
		}
	case []int8:
		for i, x := range bs {
			data[i] = int8(x)
		}
	case []uint8:
		copy(data, bs)
	case [8]uint8: // added for hardcoded buffers
		// debug.Print("data before: %#b\n", data)
		copy(data[:], bs)
		// debug.Print("data after: %#b\n", data)
	case []int16:
		for i := range data {
			data[i] = int16(order.Uint16(bs[2*i:]))
		}
	case []uint16:
		for i := range data {
			data[i] = order.Uint16(bs[2*i:])
		}
	case []int32:
		for i := range data {
			data[i] = int32(order.Uint32(bs[4*i:]))
		}
	case []uint32:
		for i := range data {
			data[i] = order.Uint32(bs[4*i:])
		}
	case []int64:
		for i := range data {
			data[i] = int64(order.Uint64(bs[8*i:]))
		}
	case []uint64:
		for i := range data {
			data[i] = order.Uint64(bs[8*i:])
		}
	case []float32:
		for i := range data {
			data[i] = math.Float32frombits(order.Uint32(bs[4*i:]))
		}
	case []float64:
		for i := range data {
			data[i] = math.Float64frombits(order.Uint64(bs[8*i:]))
		}
	default:
		panic(fmt.Sprintf("readFromBuffer: unknown type: %T", data))
	}
}
