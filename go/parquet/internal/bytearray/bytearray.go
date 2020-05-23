package bytearray

import (
	"encoding/binary"
)

type ByteArray struct {
	v     []byte
	eSize int // size of an element in bytes
}

func Make(eSizeBytes int, len, cap int) ByteArray {
	return ByteArray{
		v:     make([]byte, len*eSizeBytes, cap*eSizeBytes),
		eSize: eSizeBytes,
	}
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

// first and last are the places value will be placed [first,last)
func (b ByteArray) FillBytes(first, last int, value []byte) {
	l := len(value)
	for ; first < last; first++ {
		copy(b.v[first*l:], value)
	}
}

// first and last are the places value will be placed [first,last)
func (b ByteArray) FillUint64(first, last int, value uint64) {
	for ; first < last; first++ {
		b.PutUint64At(first, value)
	}
}

func (b ByteArray) PutUint64At(i int, value uint64) {
	binary.LittleEndian.PutUint64(b.v[i*8:], value)
}

func (b ByteArray) PutUint32At(i int, value uint32) {
	binary.LittleEndian.PutUint32(b.v[i*4:], value)
}

func (b ByteArray) Slice(start int) ByteArray {
	b.v = b.v[start:]
	return b
}

func (b ByteArray) Slice2(start, end int) ByteArray {
	b.v = b.v[start:end]
	return b
}

// At returns slice starting at element i up to element size.
func (b ByteArray) At(i int) ByteArray {
	return b.Slice2(b.ElementIndex(i), b.eSize)
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
	return binary.LittleEndian.Uint64(b.v)
}

func (b ByteArray) Zero() {
	for i := 0; i < len(b.v); i++ {
		b.v[i] = 0
	}
}
