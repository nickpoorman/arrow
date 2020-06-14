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

package memory

import (
	"bytes"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/internal/debug"
)

// Buffer is a wrapper type for a buffer of bytes.
type Buffer struct {
	refCount int64
	buf      []byte
	length   int
	mutable  bool
	mem      Allocator
}

// NewBufferBytes creates a fixed-size buffer from the specified data.
func NewBufferBytes(data []byte) *Buffer {
	return &Buffer{refCount: 0, buf: data, length: len(data)}
}

// NewResizableBuffer creates a mutable, resizable buffer with an Allocator for managing memory.
func NewResizableBuffer(mem Allocator) *Buffer {
	return &Buffer{refCount: 1, mutable: true, mem: mem}
}

// Retain increases the reference count by 1.
func (b *Buffer) Retain() {
	if b.mem != nil {
		atomic.AddInt64(&b.refCount, 1)
	}
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
func (b *Buffer) Release() {
	if b.mem != nil {
		debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

		if atomic.AddInt64(&b.refCount, -1) == 0 {
			b.mem.Free(b.buf)
			b.buf, b.length = nil, 0
		}
	}
}

// Reset resets the buffer for reuse.
func (b *Buffer) Reset(buf []byte) {
	b.buf = buf
	b.length = len(buf)
}

// Buf returns the slice of memory allocated by the Buffer, which is adjusted by calling Reserve.
func (b *Buffer) Buf() []byte { return b.buf }

// Bytes returns a slice of size Len, which is adjusted by calling Resize.
func (b *Buffer) Bytes() []byte { return b.buf[:b.length] }

// Mutable returns a bool indicating whether the buffer is mutable or not.
func (b *Buffer) Mutable() bool { return b.mutable }

// Len returns the length of the buffer.
func (b *Buffer) Len() int { return b.length }

// Cap returns the capacity of the buffer.
func (b *Buffer) Cap() int { return len(b.buf) }

// Reserve reserves the provided amount of capacity for the buffer.
func (b *Buffer) Reserve(capacity int) {
	if capacity > len(b.buf) {
		newCap := roundUpToMultipleOf64(capacity)
		if len(b.buf) == 0 {
			b.buf = b.mem.Allocate(newCap)
		} else {
			b.buf = b.mem.Reallocate(newCap, b.buf)
		}
	}
}

// Resize resizes the buffer to the target size.
func (b *Buffer) Resize(newSize int) {
	b.resize(newSize, true)
}

// ResizeNoShrink resizes the buffer to the target size, but will not
// shrink it.
func (b *Buffer) ResizeNoShrink(newSize int) {
	b.resize(newSize, false)
}

func (b *Buffer) resize(newSize int, shrink bool) {
	if !shrink || newSize > b.length {
		b.Reserve(newSize)
	} else {
		// Buffer is not growing, so shrink to the requested size without
		// excess space.
		newCap := roundUpToMultipleOf64(newSize)
		if len(b.buf) != newCap {
			if newSize == 0 {
				b.mem.Free(b.buf)
				b.buf = nil
			} else {
				b.buf = b.mem.Reallocate(newCap, b.buf)
			}
		}
	}
	b.length = newSize
}

func (b *Buffer) Equals(right *Buffer) bool {
	bBuf := b.Buf()
	rightBuff := right.Buf()
	return b == right || (b.Len() == right.Len() &&
		(&bBuf == &rightBuff ||
			bytes.Equal(b.Bytes(), right.Bytes())))
}

func (b *Buffer) EqualsLen(right *Buffer, nbytes int) bool {
	leftB := b.Buf()
	rightB := right.Buf()
	return b == right || (b.Len() >= nbytes && right.Len() >= nbytes &&
		(&leftB == &rightB ||
			bytes.Equal(b.Bytes()[:nbytes], right.Bytes()[:nbytes])))
}

// Zero bytes in padding, i.e. bytes between length and capacity.
func (b *Buffer) ZeroPadding() {
	debug.Assert(b.mutable, "buffer not mutable")
	if b.Cap() != 0 {
		memory.Set(b.mutable[b.length:], 0x00)
	}
}
