package array

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

type BooleanBufferBuilder struct {
	bufferBuilder
}

func NewBooleanBufferBuilder(mem memory.Allocator) *BooleanBufferBuilder {
	return &BooleanBufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *BooleanBufferBuilder) AppendValues(v []bool) { b.Append(arrow.BooleanTraits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *BooleanBufferBuilder) Values() []bool { return arrow.BooleanTraits.CastFromBytes(b.Bytes()) }

// Value returns the bool element at the index i. Value will panic if i is negative or ≥ Len.
func (b *BooleanBufferBuilder) Value(i int) bool { return b.Values()[i] }

// Len returns the number of bool elements in the buffer.
func (b *BooleanBufferBuilder) Len() int { return b.length / arrow.BooleanSizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *BooleanBufferBuilder) AppendValue(v bool) {
	if b.capacity < b.length+arrow.BooleanSizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.BooleanSizeBytes)
		b.resize(newCapacity)
	}
	arrow.BooleanTraits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.BooleanSizeBytes
}
