package bytearray

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/arrow/bitutil"
)

const uint8Size = int(unsafe.Sizeof(uint8(0)))
const uint16Size = int(unsafe.Sizeof(uint16(0)))
const uint32Size = int(unsafe.Sizeof(uint32(0)))
const uint64Size = int(unsafe.Sizeof(uint64(0)))

type ByteArray struct {
	v     []byte
	eSize int // size of an element in bytes
}

func NewByteArray(v []byte, eSizeBytes int) ByteArray {
	return ByteArray{
		v:     v,
		eSize: eSizeBytes,
	}
}

func NewByteArrayBits(v []byte, eSizeBits int) ByteArray {
	return ByteArray{
		v:     v,
		eSize: int(bytesForBits(int64(eSizeBits))),
	}
}
func bytesForBits(bits int64) int64 { return (bits + 7) >> 3 }

func FromUint64(v uint64) ByteArray {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return ByteArray{
		v:     b[:],
		eSize: 8,
	}
}

func (b ByteArray) ElementSize() int {
	return b.eSize
}

func (b ByteArray) ElementsCapacity() int {
	return len(b.v) / b.eSize
}

// // first and last are the places value will be placed [first,last)
// func (b ByteArray) FillBytes(first, last int, value []byte) {
// 	l := len(value)
// 	for ; first < last; first++ {
// 		copy(b.v[first*l:], value)
// 	}
// }

// first and last are the places value will be placed [first,last)
func (b ByteArray) ElementsFillBytes(first, last int, value uint64) {
	// debug.Assert(len(b.ElementAt(first).Bytes()) == len(value),
	// 	fmt.Sprintf(
	// 		"ElementsFillBytes: element bytes len are not equal to len of value bytes: %d != %d",
	// 		len(b.ElementAt(first).Bytes()), len(value),
	// 	))
	// For each element starting at start
	for ; first < last; first++ {
		copy(b.ElementAt(first).Bytes(), value)
	}
}

// first and last are the places value will be placed [first,last)
// func (b ByteArray) FillUint64(first, last int, value uint64) {
// 	for ; first < last; first++ {
// 		b.putUint64AtUint64Offset(first, value)
// 	}
// }

func (b ByteArray) putUint64AtUint64Offset(offset int, value uint64) {
	binary.LittleEndian.PutUint64(b.v[offset*8:], value)
}

func (b ByteArray) putUint32AtUint32Offset(offset int, value uint32) {
	binary.LittleEndian.PutUint32(b.v[offset*4:], value)
}

func (b ByteArray) PutUint64(value uint64) {
	if len(b.v) < int(uint64Size) {
		var v [uint64Size]byte
		binary.LittleEndian.PutUint64(v[:], value)
		copy(b.v, v[:])
	} else {
		binary.LittleEndian.PutUint64(b.v, value)
	}
}

func (b ByteArray) PutUint32(value uint32) {
	if len(b.v) < int(uint32Size) {
		var v [uint32Size]byte
		binary.LittleEndian.PutUint32(v[:], value)
		copy(b.v, v[:])
	} else {
		binary.LittleEndian.PutUint32(b.v, value)
	}
}

func (b ByteArray) PutBool(value bool) {
	if value {
		b.v[0] = 1
	} else {
		b.v[0] = 0
	}
}

func (b ByteArray) ElementsSlice(start int) ByteArray {
	b.v = b.v[start*b.eSize:]
	return b
}

func (b ByteArray) ElementsSlice2(start, end int) ByteArray {
	b.v = b.v[start*b.eSize : end*b.eSize]
	return b
}

func (b ByteArray) BytesSlice(start int) ByteArray {
	b.v = b.v[start:]
	return b
}

func (b ByteArray) BytesSlice2(start, end int) ByteArray {
	b.v = b.v[start:end]
	return b
}

// At returns slice starting at element i up to element size.
func (b ByteArray) ElementAt(i int) ByteArray {
	b.v = b.v[i*b.eSize : (i+1)*b.eSize]
	return b
}

// ElementIndex returns the index starting at eSize
func (b ByteArray) ElementIndex(i int) int {
	return i * b.eSize
}

func (b ByteArray) Copy(src []byte) {
	copy(b.v, src)
}

func (b ByteArray) Bytes() []byte {
	return b.v
}

func (b ByteArray) Uint64() uint64 {
	if len(b.v) < int(uint64Size) {
		var v [uint64Size]byte
		copy(v[:], b.v)
		return binary.LittleEndian.Uint64(v[:])
	} else {
		return binary.LittleEndian.Uint64(b.v)
	}
}

func (b ByteArray) Uint32() uint32 {
	if len(b.v) < int(uint32Size) {
		var v [int(uint32Size)]byte
		copy(v[:], b.v)
		return binary.LittleEndian.Uint32(v[:])
	} else {
		return binary.LittleEndian.Uint32(b.v)
	}
}

func (b ByteArray) Uint16() uint16 {
	if len(b.v) < int(uint16Size) {
		var v [int(uint16Size)]byte
		copy(v[:], b.v)
		return binary.LittleEndian.Uint16(v[:])
	} else {
		return binary.LittleEndian.Uint16(b.v)
	}
}

func (b ByteArray) Uint8() uint8 {
	if len(b.v) < int(uint8Size) {
		return 0
	} else {
		return b.v[0]
	}
}

func (b ByteArray) Zero() {
	for i := 0; i < len(b.v); i++ {
		b.v[i] = 0
	}
}

// BytesSize returns the len of the underlying byte array.
func (b ByteArray) BytesSize() int {
	return len(b.v)
}

// ToValue will read the buffer and put it into the value provided
// func (b ByteArray) ElementToValue(value interface{}) {
// 	readFromBuffer(b.v, binary.LittleEndian, value)
// }

func (b ByteArray) ReadTo(value interface{}) {
	if len(b.v) == 0 {
		// There's nothing in the buffer to read
		return
	}

	switch kind := reflect.ValueOf(value).Kind(); kind {
	default:
		readFromBuffer(b.v, binary.LittleEndian, value)
	case reflect.Array, reflect.Slice:
		switch bitutil.CeilByte(b.eSize) {
		case 8:
			next := func() func() []byte {
				i := 0
				return func() []byte {
					buf := make([]uint8, 8)
					copy(buf, b.ElementAt(i).Bytes())
					i++
					return buf
				}
			}()
			readFromBufferToSlice(next, binary.LittleEndian, value)
		case 16:
			buf := make([]uint8, 16)
			for i := 0; i < b.ElementsCapacity(); i++ {
				copy(buf, b.ElementAt(i).Bytes())

			}
		case 32:
			buf := make([]uint8, 32)
			for i := 0; i < b.ElementsCapacity(); i++ {
				copy(buf, b.ElementAt(i).Bytes())

			}
		case 64:
			buf := make([]uint8, 64)
			for i := 0; i < b.ElementsCapacity(); i++ {
				copy(buf, b.ElementAt(i).Bytes())

			}
		default:
			panic("primitive size greater than 64 bytes not supported")
		}
	}
}

func readFromBufferToSlice(next func() []byte, order binary.ByteOrder, data interface{}) {
	// debug.Print("readFromBuffer-first - bs: %#b - len: %d | dataT: %T\n", bs, len(bs), data)
	switch data := data.(type) {
	case []bool:
		for i := range data { // Easier to loop over the input for 8-bit values.
			data[i] = next()[0] != 0
		}
	case []int8:
		for i := range data {
			data[i] = int8(next()[0])
		}
	case []uint8:
		for i := range data {
			data[i] = next()[0]
		}
	case [8]uint8: // added for hardcoded buffers
		for i := range data {
			data[i] = next()[0]
		}
	case []int16:
		for i := range data {
			data[i] = int16(order.Uint16(next()))
		}
	case []uint16:
		for i := range data {
			data[i] = order.Uint16(next())
		}
	case []int32:
		for i := range data {
			data[i] = int32(order.Uint32(next()))
		}
	case []uint32:
		for i := range data {
			data[i] = order.Uint32(next())
		}
	case []int64:
		for i := range data {
			data[i] = int64(order.Uint64(next()))
		}
	case []int:
		switch unsafe.Sizeof(int(0)) {
		case unsafe.Sizeof(int64(0)):
			for i := range data {
				data[i] = int(order.Uint64(next()))
			}
		case unsafe.Sizeof(int32(0)):
			for i := range data {
				data[i] = int(order.Uint32(next()))
			}
		}
	case []uint64:
		for i := range data {
			data[i] = order.Uint64(next())
		}
	case []float32:
		for i := range data {
			data[i] = math.Float32frombits(order.Uint32(next()))
		}
	case []float64:
		for i := range data {
			data[i] = math.Float64frombits(order.Uint64(next()))
		}
	default:
		panic(fmt.Sprintf("readFromBuffer: unknown type: %T", data))
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
	case *int:
		switch unsafe.Sizeof(int(0)) {
		case unsafe.Sizeof(int64(0)):
			*data = int(order.Uint64(bs))
		case unsafe.Sizeof(int32(0)):
			*data = int(order.Uint32(bs))
		}
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
	case []int:
		switch unsafe.Sizeof(int(0)) {
		case unsafe.Sizeof(int64(0)):
			for i := range data {
				data[i] = int(order.Uint64(bs[8*i:]))
			}
		case unsafe.Sizeof(int32(0)):
			for i := range data {
				data[i] = int(order.Uint32(bs[4*i:]))
			}
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
