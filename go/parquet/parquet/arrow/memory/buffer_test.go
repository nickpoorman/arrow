package memory

import (
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
)

func TestBufferEqualsWithSameContent(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	bufferSize := 128 * 1024
	rawBuffer1 := pool.Allocate(bufferSize)
	memory.Set(rawBuffer1, 12)
	rawBuffer2 := pool.Allocate(bufferSize)
	memory.Set(rawBuffer2, 12)
	rawBuffer3 := pool.Allocate(bufferSize)
	memory.Set(rawBuffer3, 3)

	buffer1 := memory.NewBufferBytes(rawBuffer1)
	buffer2 := memory.NewBufferBytes(rawBuffer2)
	buffer3 := memory.NewBufferBytes(rawBuffer3)

	testutil.AssertTrue(t, BuffersEqual(buffer1, buffer1))
	testutil.AssertTrue(t, BuffersEqual(buffer1, buffer2))
	testutil.AssertFalse(t, BuffersEqual(buffer1, buffer3))

	pool.Free(rawBuffer1)
	pool.Free(rawBuffer2)
	pool.Free(rawBuffer3)
}

func TestBufferEqualsWithSameBuffer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	bufferSize := 128 * 1024
	rawBuffer := pool.Allocate(bufferSize)
	memory.Set(rawBuffer, 111)

	buffer1 := memory.NewBufferBytes(rawBuffer)
	buffer2 := memory.NewBufferBytes(rawBuffer)
	testutil.AssertTrue(t, BuffersEqual(buffer1, buffer2))

	nbytes := bufferSize / 2
	buffer3 := memory.NewBufferBytes(rawBuffer[:nbytes])
	testutil.AssertTrue(t, BuffersEqualNBytes(buffer1, buffer3, nbytes))
	testutil.AssertFalse(t, BuffersEqualNBytes(buffer1, buffer3, nbytes+1))

	pool.Free(rawBuffer)
}
