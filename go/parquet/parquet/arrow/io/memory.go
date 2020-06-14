package io

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
)

// ----------------------------------------------------------------------
// OutputStream that writes to resizable buffer

const kBufferMinimumSize = 256

type BufferOutputStream struct {
	buffer      *memory.Buffer
	isOpen      bool
	capacity    int
	position    int
	mutableData []byte
}

func NewBufferOutputStreamFromBuffer(buffer *memory.Buffer) *BufferOutputStream {
	return &BufferOutputStream{
		buffer:      buffer,
		isOpen:      true,
		capacity:    buffer.Len(),
		position:    0,
		mutableData: buffer.Buf(),
	}
}

// Create in-memory output stream with indicated capacity using a
// memory pool
// initialCapacity the initial allocated internal capacity of
// the OutputStream. When set to -1 uses default of 4096.
// pool a MemoryPool to use for allocations
// return the created stream
func NewBufferOutputStream(initialCapacity int, pool memory.Allocator) *BufferOutputStream {
	if initialCapacity == -1 {
		initialCapacity = 4096
	}
	ptr := &BufferOutputStream{}
	ptr.Reset(initialCapacity, pool)
	return ptr
}

// Close the stream, preserving the buffer (retrieve it with Finish()).
func (b *BufferOutputStream) Close() error {
	if b.isOpen {
		b.isOpen = false
		if b.position < b.capacity {
			b.buffer.ResizeNoShrink(b.position)
		}
	}
	return nil
}

func (b *BufferOutputStream) Closed() bool       { return !b.isOpen }
func (b *BufferOutputStream) Tell() (int, error) { return b.position, nil }

func (b *BufferOutputStream) Write(data []byte, nbytes int) error {
	if !b.isOpen {
		return fmt.Errorf("OutputStream is closed")
	}
	debug.Assert(b.buffer != nil, "Buffer should not be nil")
	if nbytes > 0 {
		if b.position+nbytes >= b.capacity {
			b.Reserve(nbytes)
		}
		copy(b.mutableData[b.position:], data[:nbytes])
		b.position += nbytes
	}
	return nil
}

func (b *BufferOutputStream) WriteFromBuffer(data memory.Buffer) error {
	panic("not implemented")
}

func (b *BufferOutputStream) WriteString(data string) error {
	dataBytes := []byte(data)
	return b.Write(dataBytes, len(dataBytes))
}

// Close the stream and return the buffer
func (b *BufferOutputStream) Finish() *memory.Buffer {
	b.Close()
	b.buffer.ZeroPadding()
	b.isOpen = false
	return b.buffer
}

/// Initialize state of OutputStream with newly allocated memory and
/// set position to 0
/// initialCapacity the starting allocated capacity. When set to -1 uses default of 1024.
/// pool the memory pool to use for allocations
/// return Status
func (b *BufferOutputStream) Reset(initialCapacity int, pool memory.Allocator) {
	if initialCapacity == -1 {
		initialCapacity = 1024
	}
	if pool == nil {
		pool = memory.DefaultAllocator
	}
	b.buffer = memory.NewResizableBuffer(pool)
	b.buffer.Reserve(initialCapacity)
	b.isOpen = true
	b.capacity = initialCapacity
	b.position = 0
	b.mutableData = b.buffer.Buf()
}

func (b *BufferOutputStream) Capacity() int { return b.capacity }

func (b *BufferOutputStream) Reserve(nbytes int) {
	// Always overallocate by doubling.
	newCapacity := kBufferMinimumSize
	if b.capacity > newCapacity {
		newCapacity = b.capacity
	}
	for newCapacity < b.position+nbytes {
		newCapacity = newCapacity * 2
	}
	if newCapacity > b.capacity {
		b.buffer.Resize(newCapacity)
		b.capacity = newCapacity
		b.mutableData = b.buffer.Buf()
	}
}

// Stubed for interface
func (b *BufferOutputStream) Abort() error      { panic("not implemented") }
func (b *BufferOutputStream) Flush() error      { panic("not implemented") }
func (b *BufferOutputStream) Mode() os.FileMode { panic("not implemented") }

var (
	_ OutputStream = (*BufferOutputStream)(nil)
)
