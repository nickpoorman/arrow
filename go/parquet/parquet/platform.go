package parquet

import "github.com/apache/arrow/go/arrow/memory"

func AllocateBuffer(pool memory.Allocator, size int) *memory.Buffer {
	buffer := memory.NewResizableBuffer(pool)
	buffer.Resize(size)
	return buffer
}
