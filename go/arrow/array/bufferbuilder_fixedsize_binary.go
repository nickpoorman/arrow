package array

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

type FixedSizeBinaryBufferBuilder struct {
	bufferBuilder
	dtype *arrow.FixedSizeBinaryType
}

func NewFixedSizeBinaryBufferBuilder(mem memory.Allocator, dtype *arrow.FixedSizeBinaryType) *FixedSizeBinaryBufferBuilder {
	return &FixedSizeBinaryBufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *FixedSizeBinaryBufferBuilder) AppendValues(v []byte) { b.Append(v) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *FixedSizeBinaryBufferBuilder) Values() []byte { return b.Bytes() }

// Value returns the byte element at the index i. Value will panic if i is negative or ≥ Len.
func (b *FixedSizeBinaryBufferBuilder) Value(i int) byte { return b.Values()[i] }

// Len returns the number of byte elements in the buffer.
func (b *FixedSizeBinaryBufferBuilder) Len() int { return b.length }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *FixedSizeBinaryBufferBuilder) AppendValue(v []byte) {
	if len(v) != b.dtype.ByteWidth {
		panic(
			fmt.Errorf(
				"expected len(v) (%d) to match ByteWidth (%d)",
				len(v),
				b.dtype.ByteWidth,
			),
		)
	}
	if b.capacity < b.length+b.dtype.ByteWidth {
		newCapacity := bitutil.NextPowerOf2(b.length + b.dtype.ByteWidth)
		b.resize(newCapacity)
	}
	b.Append(v)
}
