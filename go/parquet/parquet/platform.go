package parquet

import (
	"github.com/apache/arrow/go/arrow/memory"
	arrowio "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/io"
)

// type Buffer memory.Buffer
// type Codec compress.CompressionCodec
type MemoryPool memory.Allocator
type MutableBuffer memory.Buffer
type ResizableBuffer memory.Buffer
type ArrowInputFile arrowio.RandomAccessFile
type ArrowInputStream arrowio.InputStream

const kDefaultOutputStreamSize int64 = 1024
const kNonPageOrdinal int16 = -1

func CreateOutputStream(pool memory.Allocator) *arrowio.BufferedOutputStream {
	return arrowio.NewBufferOutputStream(kDefaultOutputStreamSize, pool)
}

func AllocateBuffer(pool memory.Allocator, size int) *memory.Buffer {
	buffer := memory.NewResizableBuffer(pool)
	buffer.Resize(size)
	return buffer
}
