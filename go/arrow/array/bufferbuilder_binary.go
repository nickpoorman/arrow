package array

import (
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

type BinaryBufferBuilder struct {
	bufferBuilder
}

func NewBinaryBufferBuilder(mem memory.Allocator) *BinaryBufferBuilder {
	return &BinaryBufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *BinaryBufferBuilder) AppendValues(v []byte) { b.Append(v) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *BinaryBufferBuilder) Values() []byte { return b.Bytes() }

// Value returns the byte element at the index i. Value will panic if i is negative or ≥ Len.
func (b *BinaryBufferBuilder) Value(i int) byte { return b.Values()[i] }

// Len returns the number of byte elements in the buffer.
func (b *BinaryBufferBuilder) Len() int { return b.length }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *BinaryBufferBuilder) AppendValue(v byte) {
	if b.capacity < b.length+1 {
		newCapacity := bitutil.NextPowerOf2(b.length + 1)
		b.resize(newCapacity)
	}
	b.bytes[b.length] = v
	b.length++
}
