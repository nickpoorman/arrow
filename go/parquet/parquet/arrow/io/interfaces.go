package io

import (
	"context"
	"io"
	"os"

	"github.com/apache/arrow/go/arrow/memory"
)

type FileInterface interface {
	// Close the stream cleanly
	//
	// For writable streams, this will attempt to flush any pending data
	// before releasing the underlying resource.
	//
	// After Close() is called, closed() returns true and the stream is not
	// available for further operations.
	Close() error

	// Close the stream abruptly
	//
	// This method does not guarantee that any pending data is flushed.
	// It merely releases any underlying resource used by the stream for
	// its operation.
	//
	// After Abort() is called, closed() returns true and the stream is not
	// available for further operations.
	Abort() error

	// Return the position in this stream
	Tell() (int64, error)

	// Return whether the stream is closed
	Closed() bool

	Mode() os.FileMode
}

type Seekable interface {
	Seek(position int64) error
}

type Writable interface {
	// Write the given data to the stream
	//
	// This method always processes the bytes in full.  Depending on the
	// semantics of the stream, the data may be written out immediately,
	// held in a buffer, or written asynchronously.  In the case where
	// the stream buffers the data, it will be copied.  To avoid potentially
	// large copies, use the Write variant that takes an owned Buffer.
	Write(data []byte, nbytes int64) error

	// Write the given data to the stream
	//
	// Since the Buffer owns its memory, this method can avoid a copy if
	// buffering is required.  See Write([]byte, int64) for details.
	WriteFromBuffer(data memory.Buffer) error

	// Flush buffered bytes, if any
	Flush() error

	WriteString(data string) error
}

type Readable interface {
	// Read data from current file position.
	//
	// Read at most `nbytes` from the current file position into `out`.
	// The number of bytes read is returned.
	Read(nbytes int64, out []byte) (int64, error)

	// Read data from current file position.
	//
	// Read at most `nbytes` from the current file position. Less bytes may
	// be read if EOF is reached. This method updates the current file position.
	//
	// In some cases (e.g. a memory-mapped file), this method may avoid a
	// memory copy.
	ReadToBuffer(nbytes int64) (*memory.Buffer, error)
}

type OutputStream interface {
	FileInterface
	Writable
}

type InputStream interface {
	FileInterface
	Readable

	// Advance or skip stream indicated number of bytes
	// nbytes the number to move forward
	// return Status
	Advance(nbytes int64) error

	// Return zero-copy string_view to upcoming bytes.
	//
	// Do not modify the stream position.  The view becomes invalid after
	// any operation on the stream.  May trigger buffering if the requested
	// size is larger than the number of buffered bytes.
	//
	// May return NotImplemented on streams that don't support it.
	//
	// nbytes the maximum number of bytes to see
	Peek(nbytes int64) ([]byte, error)

	// Return true if InputStream is capable of zero copy Buffer reads
	//
	// Zero copy reads imply the use of Buffer-returning Read() overloads.
	SupportsZeroCopy() bool
}

type RandomAccessFile interface {
	InputStream
	Seekable

	// Create an isolated InputStream that reads a segment of a
	// RandomAccessFile. Multiple such stream can be created and used
	// independently without interference
	// file a file instance
	// file_offset the starting position in the file
	// nbytes the extent of bytes to read. The file should have
	// sufficient bytes available
	GetStream(file RandomAccessFile, fileOffset int64, nbytes int64) InputStream

	// Return the total file size in bytes.
	//
	// This method does not read or move the current file position, so is safe
	// to call concurrently with e.g. ReadAt().
	GetSize() (int64, error)

	// Read data from given file position.
	//
	// At most `nbytes` bytes are read.  The number of bytes read is returned
	// (it can be less than `nbytes` if EOF is reached).
	//
	// This method can be safely called from multiple threads concurrently.
	// It is unspecified whether this method updates the file position or not.
	//
	// The default RandomAccessFile-provided implementation uses Seek() and Read(),
	// but subclasses may override it with a more efficient implementation
	// that doesn't depend on implicit file positioning.
	//
	// position Where to read bytes from
	// nbytes The number of bytes to read
	// out The buffer to read bytes into
	// return The number of bytes read, or an error
	ReadAt(position int64, nbytes int64, out []byte) (int64, error)

	// Read data from given file position.
	//
	// At most `nbytes` bytes are read, but it can be less if EOF is reached.
	//
	// position Where to read bytes from
	// nbytes The number of bytes to read
	// return A buffer containing the bytes read, or an error
	ReadAtToBuffer(position int64, nbytes int64) *memory.Buffer

	// EXPERIMENTAL
	ReadAsync(ctx context.Context, position int64, nbytes int64)
}

type WritableFile interface {
	OutputStream
	Seekable
	WriteAt(position int64, data []byte, nbytes int64) error
}

type ReadWriteFileInterface interface {
	RandomAccessFile
	WritableFile
}

type BufferedOutputStream struct{}

func NewBufferOutputStream(size int64, pool memory.Allocator) *BufferedOutputStream {
	return &BufferedOutputStream{}
}

// func Peek(r io.Reader, nbytes int) ([]byte, error) {
// 	// If it has a Peek method
// 	peeker, ok := r.(interface{ Peek(n int) ([]byte, error) })
// 	if ok {
// 		return peeker.Peek(nbytes)
// 	}
// 	// If it's a ReaderAt
// 	readerAt, ok := r.(io.ReaderAt)
// 	if ok {
// 		readerAt.ReadAt()
// 	}

// }

func Advance(r io.Reader, nbytes int64) error {
	// If it's a seeker do seek
	seeker, ok := r.(io.Seeker)
	if ok {
		if _, err := seeker.Seek(nbytes, io.SeekCurrent); err != nil {
			return err
		}
	}
	byteReader, ok := r.(io.ByteReader)
	if ok {
		for i := int64(0); i < nbytes; i++ {
			if _, err := byteReader.ReadByte(); err != nil {
				return err
			}
		}
	}
	b := make([]byte, 1)
	for i := int64(0); i < nbytes; i++ {
		if _, err := r.Read(b); err != nil {
			return err
		}
	}
	return nil
}
