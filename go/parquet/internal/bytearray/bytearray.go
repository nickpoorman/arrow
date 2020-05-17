package bytearray

import "encoding/binary"

type ByteArray struct {
	v     []byte
	eSize int // size of an element in bytes
}

func NewByteArray(v []byte, eSize int) ByteArray {
	return ByteArray{
		v:     v,
		eSize: eSize,
	}
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
		binary.LittleEndian.PutUint64(b.v[first*8:], value)
	}
}

func (b ByteArray) Slice(start int) ByteArray {
	b.v = b.v[start:]
	return b
}

func (b ByteArray) Slice2(start, end int) ByteArray {
	b.v = b.v[start:end]
	return b
}

// ElementIndex returns the index starting at eSize
func (b ByteArray) ElementIndex(i int) int {
	return i * b.eSize
}
