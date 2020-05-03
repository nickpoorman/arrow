// Code generated by array/bufferbuilder_numeric.gen.go.tmpl. DO NOT EDIT.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package array

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

type Int64BufferBuilder struct {
	bufferBuilder
}

func NewInt64BufferBuilder(mem memory.Allocator) *Int64BufferBuilder {
	return &Int64BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Int64BufferBuilder) AppendValues(v []int64) { b.Append(arrow.Int64Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Int64BufferBuilder) Values() []int64 { return arrow.Int64Traits.CastFromBytes(b.Bytes()) }

// Value returns the int64 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Int64BufferBuilder) Value(i int) int64 { return b.Values()[i] }

// Len returns the number of int64 elements in the buffer.
func (b *Int64BufferBuilder) Len() int { return b.length / arrow.Int64SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Int64BufferBuilder) AppendValue(v int64) {
	if b.capacity < b.length+arrow.Int64SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int64SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int64Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int64SizeBytes
}

type Uint64BufferBuilder struct {
	bufferBuilder
}

func NewUint64BufferBuilder(mem memory.Allocator) *Uint64BufferBuilder {
	return &Uint64BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Uint64BufferBuilder) AppendValues(v []uint64) { b.Append(arrow.Uint64Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Uint64BufferBuilder) Values() []uint64 { return arrow.Uint64Traits.CastFromBytes(b.Bytes()) }

// Value returns the uint64 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Uint64BufferBuilder) Value(i int) uint64 { return b.Values()[i] }

// Len returns the number of uint64 elements in the buffer.
func (b *Uint64BufferBuilder) Len() int { return b.length / arrow.Uint64SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Uint64BufferBuilder) AppendValue(v uint64) {
	if b.capacity < b.length+arrow.Uint64SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Uint64SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Uint64Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Uint64SizeBytes
}

type Float64BufferBuilder struct {
	bufferBuilder
}

func NewFloat64BufferBuilder(mem memory.Allocator) *Float64BufferBuilder {
	return &Float64BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Float64BufferBuilder) AppendValues(v []float64) {
	b.Append(arrow.Float64Traits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Float64BufferBuilder) Values() []float64 {
	return arrow.Float64Traits.CastFromBytes(b.Bytes())
}

// Value returns the float64 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Float64BufferBuilder) Value(i int) float64 { return b.Values()[i] }

// Len returns the number of float64 elements in the buffer.
func (b *Float64BufferBuilder) Len() int { return b.length / arrow.Float64SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Float64BufferBuilder) AppendValue(v float64) {
	if b.capacity < b.length+arrow.Float64SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Float64SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Float64Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Float64SizeBytes
}

type Int32BufferBuilder struct {
	bufferBuilder
}

func NewInt32BufferBuilder(mem memory.Allocator) *Int32BufferBuilder {
	return &Int32BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Int32BufferBuilder) AppendValues(v []int32) { b.Append(arrow.Int32Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Int32BufferBuilder) Values() []int32 { return arrow.Int32Traits.CastFromBytes(b.Bytes()) }

// Value returns the int32 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Int32BufferBuilder) Value(i int) int32 { return b.Values()[i] }

// Len returns the number of int32 elements in the buffer.
func (b *Int32BufferBuilder) Len() int { return b.length / arrow.Int32SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Int32BufferBuilder) AppendValue(v int32) {
	if b.capacity < b.length+arrow.Int32SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int32SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int32Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int32SizeBytes
}

type Uint32BufferBuilder struct {
	bufferBuilder
}

func NewUint32BufferBuilder(mem memory.Allocator) *Uint32BufferBuilder {
	return &Uint32BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Uint32BufferBuilder) AppendValues(v []uint32) { b.Append(arrow.Uint32Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Uint32BufferBuilder) Values() []uint32 { return arrow.Uint32Traits.CastFromBytes(b.Bytes()) }

// Value returns the uint32 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Uint32BufferBuilder) Value(i int) uint32 { return b.Values()[i] }

// Len returns the number of uint32 elements in the buffer.
func (b *Uint32BufferBuilder) Len() int { return b.length / arrow.Uint32SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Uint32BufferBuilder) AppendValue(v uint32) {
	if b.capacity < b.length+arrow.Uint32SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Uint32SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Uint32Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Uint32SizeBytes
}

type Float32BufferBuilder struct {
	bufferBuilder
}

func NewFloat32BufferBuilder(mem memory.Allocator) *Float32BufferBuilder {
	return &Float32BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Float32BufferBuilder) AppendValues(v []float32) {
	b.Append(arrow.Float32Traits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Float32BufferBuilder) Values() []float32 {
	return arrow.Float32Traits.CastFromBytes(b.Bytes())
}

// Value returns the float32 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Float32BufferBuilder) Value(i int) float32 { return b.Values()[i] }

// Len returns the number of float32 elements in the buffer.
func (b *Float32BufferBuilder) Len() int { return b.length / arrow.Float32SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Float32BufferBuilder) AppendValue(v float32) {
	if b.capacity < b.length+arrow.Float32SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Float32SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Float32Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Float32SizeBytes
}

type Int16BufferBuilder struct {
	bufferBuilder
}

func NewInt16BufferBuilder(mem memory.Allocator) *Int16BufferBuilder {
	return &Int16BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Int16BufferBuilder) AppendValues(v []int16) { b.Append(arrow.Int16Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Int16BufferBuilder) Values() []int16 { return arrow.Int16Traits.CastFromBytes(b.Bytes()) }

// Value returns the int16 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Int16BufferBuilder) Value(i int) int16 { return b.Values()[i] }

// Len returns the number of int16 elements in the buffer.
func (b *Int16BufferBuilder) Len() int { return b.length / arrow.Int16SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Int16BufferBuilder) AppendValue(v int16) {
	if b.capacity < b.length+arrow.Int16SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int16SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int16Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int16SizeBytes
}

type Uint16BufferBuilder struct {
	bufferBuilder
}

func NewUint16BufferBuilder(mem memory.Allocator) *Uint16BufferBuilder {
	return &Uint16BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Uint16BufferBuilder) AppendValues(v []uint16) { b.Append(arrow.Uint16Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Uint16BufferBuilder) Values() []uint16 { return arrow.Uint16Traits.CastFromBytes(b.Bytes()) }

// Value returns the uint16 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Uint16BufferBuilder) Value(i int) uint16 { return b.Values()[i] }

// Len returns the number of uint16 elements in the buffer.
func (b *Uint16BufferBuilder) Len() int { return b.length / arrow.Uint16SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Uint16BufferBuilder) AppendValue(v uint16) {
	if b.capacity < b.length+arrow.Uint16SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Uint16SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Uint16Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Uint16SizeBytes
}

type Int8BufferBuilder struct {
	bufferBuilder
}

func NewInt8BufferBuilder(mem memory.Allocator) *Int8BufferBuilder {
	return &Int8BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Int8BufferBuilder) AppendValues(v []int8) { b.Append(arrow.Int8Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Int8BufferBuilder) Values() []int8 { return arrow.Int8Traits.CastFromBytes(b.Bytes()) }

// Value returns the int8 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Int8BufferBuilder) Value(i int) int8 { return b.Values()[i] }

// Len returns the number of int8 elements in the buffer.
func (b *Int8BufferBuilder) Len() int { return b.length / arrow.Int8SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Int8BufferBuilder) AppendValue(v int8) {
	if b.capacity < b.length+arrow.Int8SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int8SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int8Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int8SizeBytes
}

type Uint8BufferBuilder struct {
	bufferBuilder
}

func NewUint8BufferBuilder(mem memory.Allocator) *Uint8BufferBuilder {
	return &Uint8BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Uint8BufferBuilder) AppendValues(v []uint8) { b.Append(arrow.Uint8Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Uint8BufferBuilder) Values() []uint8 { return arrow.Uint8Traits.CastFromBytes(b.Bytes()) }

// Value returns the uint8 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Uint8BufferBuilder) Value(i int) uint8 { return b.Values()[i] }

// Len returns the number of uint8 elements in the buffer.
func (b *Uint8BufferBuilder) Len() int { return b.length / arrow.Uint8SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Uint8BufferBuilder) AppendValue(v uint8) {
	if b.capacity < b.length+arrow.Uint8SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Uint8SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Uint8Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Uint8SizeBytes
}

type TimestampBufferBuilder struct {
	bufferBuilder
}

func NewTimestampBufferBuilder(mem memory.Allocator) *TimestampBufferBuilder {
	return &TimestampBufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *TimestampBufferBuilder) AppendValues(v []arrow.Timestamp) {
	b.Append(arrow.TimestampTraits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *TimestampBufferBuilder) Values() []arrow.Timestamp {
	return arrow.TimestampTraits.CastFromBytes(b.Bytes())
}

// Value returns the arrow.Timestamp element at the index i. Value will panic if i is negative or ≥ Len.
func (b *TimestampBufferBuilder) Value(i int) arrow.Timestamp { return b.Values()[i] }

// Len returns the number of arrow.Timestamp elements in the buffer.
func (b *TimestampBufferBuilder) Len() int { return b.length / arrow.TimestampSizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *TimestampBufferBuilder) AppendValue(v arrow.Timestamp) {
	if b.capacity < b.length+arrow.TimestampSizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.TimestampSizeBytes)
		b.resize(newCapacity)
	}
	arrow.TimestampTraits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.TimestampSizeBytes
}

type Time32BufferBuilder struct {
	bufferBuilder
}

func NewTime32BufferBuilder(mem memory.Allocator) *Time32BufferBuilder {
	return &Time32BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Time32BufferBuilder) AppendValues(v []arrow.Time32) {
	b.Append(arrow.Time32Traits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Time32BufferBuilder) Values() []arrow.Time32 {
	return arrow.Time32Traits.CastFromBytes(b.Bytes())
}

// Value returns the arrow.Time32 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Time32BufferBuilder) Value(i int) arrow.Time32 { return b.Values()[i] }

// Len returns the number of arrow.Time32 elements in the buffer.
func (b *Time32BufferBuilder) Len() int { return b.length / arrow.Time32SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Time32BufferBuilder) AppendValue(v arrow.Time32) {
	if b.capacity < b.length+arrow.Time32SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Time32SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Time32Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Time32SizeBytes
}

type Time64BufferBuilder struct {
	bufferBuilder
}

func NewTime64BufferBuilder(mem memory.Allocator) *Time64BufferBuilder {
	return &Time64BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Time64BufferBuilder) AppendValues(v []arrow.Time64) {
	b.Append(arrow.Time64Traits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Time64BufferBuilder) Values() []arrow.Time64 {
	return arrow.Time64Traits.CastFromBytes(b.Bytes())
}

// Value returns the arrow.Time64 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Time64BufferBuilder) Value(i int) arrow.Time64 { return b.Values()[i] }

// Len returns the number of arrow.Time64 elements in the buffer.
func (b *Time64BufferBuilder) Len() int { return b.length / arrow.Time64SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Time64BufferBuilder) AppendValue(v arrow.Time64) {
	if b.capacity < b.length+arrow.Time64SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Time64SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Time64Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Time64SizeBytes
}

type Date32BufferBuilder struct {
	bufferBuilder
}

func NewDate32BufferBuilder(mem memory.Allocator) *Date32BufferBuilder {
	return &Date32BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Date32BufferBuilder) AppendValues(v []arrow.Date32) {
	b.Append(arrow.Date32Traits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Date32BufferBuilder) Values() []arrow.Date32 {
	return arrow.Date32Traits.CastFromBytes(b.Bytes())
}

// Value returns the arrow.Date32 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Date32BufferBuilder) Value(i int) arrow.Date32 { return b.Values()[i] }

// Len returns the number of arrow.Date32 elements in the buffer.
func (b *Date32BufferBuilder) Len() int { return b.length / arrow.Date32SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Date32BufferBuilder) AppendValue(v arrow.Date32) {
	if b.capacity < b.length+arrow.Date32SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Date32SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Date32Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Date32SizeBytes
}

type Date64BufferBuilder struct {
	bufferBuilder
}

func NewDate64BufferBuilder(mem memory.Allocator) *Date64BufferBuilder {
	return &Date64BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *Date64BufferBuilder) AppendValues(v []arrow.Date64) {
	b.Append(arrow.Date64Traits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *Date64BufferBuilder) Values() []arrow.Date64 {
	return arrow.Date64Traits.CastFromBytes(b.Bytes())
}

// Value returns the arrow.Date64 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *Date64BufferBuilder) Value(i int) arrow.Date64 { return b.Values()[i] }

// Len returns the number of arrow.Date64 elements in the buffer.
func (b *Date64BufferBuilder) Len() int { return b.length / arrow.Date64SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *Date64BufferBuilder) AppendValue(v arrow.Date64) {
	if b.capacity < b.length+arrow.Date64SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Date64SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Date64Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Date64SizeBytes
}

type DurationBufferBuilder struct {
	bufferBuilder
}

func NewDurationBufferBuilder(mem memory.Allocator) *DurationBufferBuilder {
	return &DurationBufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *DurationBufferBuilder) AppendValues(v []arrow.Duration) {
	b.Append(arrow.DurationTraits.CastToBytes(v))
}

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *DurationBufferBuilder) Values() []arrow.Duration {
	return arrow.DurationTraits.CastFromBytes(b.Bytes())
}

// Value returns the arrow.Duration element at the index i. Value will panic if i is negative or ≥ Len.
func (b *DurationBufferBuilder) Value(i int) arrow.Duration { return b.Values()[i] }

// Len returns the number of arrow.Duration elements in the buffer.
func (b *DurationBufferBuilder) Len() int { return b.length / arrow.DurationSizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *DurationBufferBuilder) AppendValue(v arrow.Duration) {
	if b.capacity < b.length+arrow.DurationSizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.DurationSizeBytes)
		b.resize(newCapacity)
	}
	arrow.DurationTraits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.DurationSizeBytes
}
