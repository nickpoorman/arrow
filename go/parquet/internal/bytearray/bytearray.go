package bytearray

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
)

const boolSize = int(unsafe.Sizeof(bool(false)))
const uint8Size = int(unsafe.Sizeof(uint8(0)))
const uint16Size = int(unsafe.Sizeof(uint16(0)))
const uint32Size = int(unsafe.Sizeof(uint32(0)))
const uint64Size = int(unsafe.Sizeof(uint64(0)))
const intSize = int(unsafe.Sizeof(int(0)))
const int8Size = int(unsafe.Sizeof(int8(0)))
const int16Size = int(unsafe.Sizeof(int16(0)))
const int32Size = int(unsafe.Sizeof(int32(0)))
const int64Size = int(unsafe.Sizeof(int64(0)))

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
		eSize: int(BytesForBits(int64(eSizeBits))),
	}
}
func BytesForBits(bits int64) int64 { return (bits + 7) >> 3 }

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
func (b ByteArray) ElementsFillBytes(first, last int, value []byte) {
	debug.Print("ElementsFillBytes - first: %d | last: %d | value: %+v", first, last, value)
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

// func (b ByteArray) int8Next(i int, v []uint8) []byte {
// 	copy(Uint8CastToBytes(v), b.ElementAt(i).Bytes())
// 	return buf[:]
// }

func (b ByteArray) ReadInto(value interface{}) {
	if len(b.v) == 0 {
		// There's nothing in the buffer to read
		return
	}

	switch kind := reflect.ValueOf(value).Kind(); kind {
	default:
		b.readFromBuffer(binary.LittleEndian, value)
	case reflect.Array, reflect.Slice:
		b.readFromBufferToSlice(binary.LittleEndian, value)
	}
}

func (b ByteArray) readFromBufferToSlice(order binary.ByteOrder, data interface{}) {
	// debug.Print("readFromBuffer-first - bs: %#b - len: %d | dataT: %T\n", bs, len(bs), data)
	switch data := data.(type) {
	case []bool:
		for i := range data {
			// data[i] = next(i)[0] != 0
			copy(BoolCastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []int8:
		for i := range data {
			// data[i] = int8(next(i)[0])
			copy(Int8CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []uint8:
		for i := range data {
			// data[i] = next(i)[0]
			copy(Uint8CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case [8]uint8: // added for hardcoded buffers
		for i := range data {
			// data[i] = next(i)[0]
			copy(Uint8CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []int16:
		for i := range data {
			// data[i] = int16(order.Uint16(next(i)))
			copy(Int16CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []uint16:
		for i := range data {
			// data[i] = order.Uint16(next(i))
			copy(Uint16CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []int32:
		for i := range data {
			// data[i] = int32(order.Uint32(next(i)))
			copy(Int32CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []uint32:
		for i := range data {
			// data[i] = order.Uint32(next(i))
			copy(Uint32CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []int64:
		for i := range data {
			// data[i] = int64(order.Uint64(next(i)))
			copy(Int64CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []int:
		for i := range data {
			copy(IntCastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []uint64:
		for i := range data {
			// data[i] = order.Uint64(next(i))
			copy(Uint64CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []float32:
		for i := range data {
			// data[i] = math.Float32frombits(order.Uint32(next(i)))
			copy(Float32CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	case []float64:
		for i := range data {
			// data[i] = math.Float64frombits(order.Uint64(next(i)))
			copy(Float64CastToBytes(data[i:i+1]), b.ElementAt(i).Bytes())
		}
	default:
		panic(fmt.Sprintf("readFromBuffer: unknown type: %T", data))
	}
}

func (b ByteArray) readFromBuffer(order binary.ByteOrder, data interface{}) {
	if len(b.v) == 0 {
		// There's nothing in the buffer to read
		return
	}
	bs := b.v
	switch data := data.(type) {
	case *bool:
		*data = bs[0] != 0
	case *int8:
		*data = int8(bs[0])
	case *uint8:
		*data = bs[0]
	case *int16:
		if len(b.v) < 2 {
			bs = make([]byte, 2)
			copy(bs, b.v)
		}
		*data = int16(order.Uint16(bs))
	case *uint16:
		if len(b.v) < 2 {
			bs = make([]byte, 2)
			copy(bs, b.v)
		}
		*data = order.Uint16(bs)
	case *int32:
		if len(b.v) < 4 {
			bs = make([]byte, 4)
			copy(bs, b.v)
		}
		*data = int32(order.Uint32(bs))
	case *uint32:
		if len(b.v) < 4 {
			bs = make([]byte, 4)
			copy(bs, b.v)
		}
		*data = order.Uint32(bs)
	case *int64:
		if len(b.v) < 8 {
			bs = make([]byte, 8)
			copy(bs, b.v)
		}
		*data = int64(order.Uint64(bs))
	case *int:
		switch unsafe.Sizeof(int(0)) {
		case unsafe.Sizeof(int64(0)):
			if len(b.v) < 8 {
				bs = make([]byte, 8)
				copy(bs, b.v)
			}
			*data = int(order.Uint64(bs))
		case unsafe.Sizeof(int32(0)):
			if len(b.v) < 4 {
				bs = make([]byte, 4)
				copy(bs, b.v)
			}
			*data = int(order.Uint32(bs))
		}
	case *uint64:
		if len(b.v) < 8 {
			bs = make([]byte, 8)
			copy(bs, b.v)
		}
		*data = order.Uint64(bs)
	case *float32:
		if len(b.v) < 4 {
			bs = make([]byte, 4)
			copy(bs, b.v)
		}
		*data = math.Float32frombits(order.Uint32(bs))
	case *float64:
		if len(b.v) < 8 {
			bs = make([]byte, 8)
			copy(bs, b.v)
		}
		*data = math.Float64frombits(order.Uint64(bs))
	default:
		panic(fmt.Sprintf("readFromBuffer: unknown type: %T", data))
	}
}

func BoolCastToBytes(b []bool) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * boolSize
	s.Cap = h.Cap * boolSize

	return res
}

func Uint8CastToBytes(b []uint8) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * uint8Size
	s.Cap = h.Cap * uint8Size

	return res
}

func Uint16CastToBytes(b []uint16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * uint16Size
	s.Cap = h.Cap * uint16Size

	return res
}

func Uint32CastToBytes(b []uint32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * uint32Size
	s.Cap = h.Cap * uint32Size

	return res
}

func Uint64CastToBytes(b []uint64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * uint64Size
	s.Cap = h.Cap * uint64Size

	return res
}

func Int8CastToBytes(b []int8) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * int8Size
	s.Cap = h.Cap * int8Size

	return res
}

func Int16CastToBytes(b []int16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * int16Size
	s.Cap = h.Cap * int16Size

	return res
}

func Int32CastToBytes(b []int32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * int32Size
	s.Cap = h.Cap * int32Size

	return res
}

func Int64CastToBytes(b []int64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * int64Size
	s.Cap = h.Cap * int64Size

	return res
}

func IntCastToBytes(b []int) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * intSize
	s.Cap = h.Cap * intSize

	return res
}

func Float32CastToBytes(b []float32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * uint32Size
	s.Cap = h.Cap * uint32Size

	return res
}

func Float64CastToBytes(b []float64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * uint64Size
	s.Cap = h.Cap * uint64Size

	return res
}
