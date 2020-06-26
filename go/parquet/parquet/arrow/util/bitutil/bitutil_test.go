package bitutil

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
)

func AssertReaderSet(t *testing.T, reader *BitmapReader) {
	t.Helper()
	testutil.AssertTrue(t, reader.IsSet())
	testutil.AssertFalse(t, reader.IsNotSet())
	reader.Next()
}

func AssertReaderNotSet(t *testing.T, reader *BitmapReader) {
	t.Helper()
	testutil.AssertFalse(t, reader.IsSet())
	testutil.AssertTrue(t, reader.IsNotSet())
	reader.Next()
}

// Assert that a BitmapReader yields the given bit values
func AssertReaderValues(t *testing.T, reader *BitmapReader, values []int) {
	t.Helper()
	for _, value := range values {
		if value != 0 {
			AssertReaderSet(t, reader)
		} else {
			AssertReaderNotSet(t, reader)
		}
	}
}

func WriteSliceToWriter(writer *BitmapWriter, values []int) {
	for _, value := range values {
		if value != 0 {
			writer.Set()
		} else {
			writer.Clear()
		}
		writer.Next()
	}
	writer.Finish()
}

func BitmapFromSlice(values []int, bitOffset int) (buffer *memory.Buffer, length int) {
	length = len(values)
	buffer = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffer.Resize(length + bitOffset)
	writer := NewBitmapWriter(buffer.Bytes(), bitOffset, length)
	WriteSliceToWriter(writer, values)
	return
}

func TestBitmapReader_NormalOperation(t *testing.T) {
	var buffer *memory.Buffer
	var length int

	offsets := []int{0, 1, 3, 5, 7, 8, 12, 13, 21, 38, 75, 120}
	for _, offset := range offsets {
		buffer, length = BitmapFromSlice([]int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}, offset)
		testutil.AssertEqInt(t, length, 14)

		reader := NewBitmapReader(buffer.Bytes(), offset, length)
		AssertReaderValues(t, reader, []int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1})
	}
}

func TestBitmapReader_DoesNotReadOutOfBounds(t *testing.T) {
	bitmap := make([]byte, 16)
	length := 128

	r1 := NewBitmapReader(bitmap, 0, length)

	for i := 0; i < length; i++ {
		testutil.AssertTrue(t, r1.IsNotSet())
		r1.Next()
	}

	r2 := NewBitmapReader(bitmap, 5, length-5)

	for i := 0; i < (length - 5); i++ {
		testutil.AssertTrue(t, r2.IsNotSet())
		r2.Next()
	}

	// Does not access invalid memory
	_ = NewBitmapReader(nil, 0, 0)
}

func TestBitmapWriter_NormalOperation(t *testing.T) {
	for _, fillByte := range []byte{0x00, 0xff} {
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			writer := NewBitmapWriter(bitmap, 0, 12)
			WriteSliceToWriter(writer, []int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
			// {0b00110110, 0b....1010, ........, ........}
			want := []byte{
				0x36,
				(0x0a | (fillByte & 0xf0)),
				fillByte, fillByte,
			}
			testutil.AssertBytesEq(t, bitmap, want)
		}
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			writer := NewBitmapWriter(bitmap, 3, 12)
			WriteSliceToWriter(writer, []int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
			// {0b10110..., 0b.1010001, ........, ........}
			want := []byte{
				(0xb0 | (fillByte & 0x07)),
				(0x51 | (fillByte & 0x80)),
				fillByte, fillByte,
			}
			testutil.AssertBytesEq(t, bitmap, want)
		}
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			writer := NewBitmapWriter(bitmap, 20, 12)
			WriteSliceToWriter(writer, []int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
			// {........, ........, 0b0110...., 0b10100011}
			want := []byte{
				fillByte, fillByte,
				(0x60 | (fillByte & 0x0f)),
				0xa3,
			}
			testutil.AssertBytesEq(t, bitmap, want)
		}
		// 0-length writes
		for pos := 0; pos < 32; pos++ {
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			writer := NewBitmapWriter(bitmap, pos, 0)
			WriteSliceToWriter(writer, []int{})
			testutil.AssertBytesEq(t, bitmap, []byte{fillByte, fillByte, fillByte, fillByte})
		}
	}
}

func TestBitmapWriter_DoesNotWriteOutOfBounds(t *testing.T) {
	bitmap := make([]byte, 16)
	length := 128
	numValues := 0

	r1 := NewBitmapWriter(bitmap, 0, length)

	for i := 0; i < length; i++ {
		r1.Set()
		r1.Clear()
		r1.Next()
	}
	r1.Finish()
	numValues = r1.Position()

	testutil.AssertEqInt(t, numValues, length)

	r2 := NewBitmapWriter(bitmap, 5, length-5)

	for i := 0; i < (length - 5); i++ {
		r2.Set()
		r2.Clear()
		r2.Next()
	}
	r2.Finish()
	numValues = r2.Position()

	testutil.AssertEqInt(t, numValues, (length - 5))
}

func TestBitUtil_Log2(t *testing.T) {
	testutil.AssertEqInt(t, Log2(1), 0)
	testutil.AssertEqInt(t, Log2(2), 1)
	testutil.AssertEqInt(t, Log2(3), 2)
	testutil.AssertEqInt(t, Log2(4), 2)
	testutil.AssertEqInt(t, Log2(5), 3)
	testutil.AssertEqInt(t, Log2(8), 3)
	testutil.AssertEqInt(t, Log2(9), 4)
	testutil.AssertEqInt(t, Log2(math.MaxInt32), 31)
	testutil.AssertEqInt(t, Log2(math.MaxUint32), 32)
	testutil.AssertEqInt(t, Log2(math.MaxUint64), 64)
}

func TestBitUtil_NumRequiredBits(t *testing.T) {
	testutil.AssertEqInt(t, NumRequiredBits(0), 0)
	testutil.AssertEqInt(t, NumRequiredBits(1), 1)
	testutil.AssertEqInt(t, NumRequiredBits(2), 2)
	testutil.AssertEqInt(t, NumRequiredBits(3), 2)
	testutil.AssertEqInt(t, NumRequiredBits(4), 3)
	testutil.AssertEqInt(t, NumRequiredBits(5), 3)
	testutil.AssertEqInt(t, NumRequiredBits(7), 3)
	testutil.AssertEqInt(t, NumRequiredBits(8), 4)
	testutil.AssertEqInt(t, NumRequiredBits(9), 4)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint32-1), 32)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint32), 32)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint32+1), 33)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint64/2), 63)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint64/2+1), 64)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint64-1), 64)
	testutil.AssertEqInt(t, NumRequiredBits(math.MaxUint64), 64)
}

func TestBitUtil_CountLeadingZeros(t *testing.T) {
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(0)), 32)
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(1)), 31)
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(2)), 30)
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(3)), 30)
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(4)), 29)
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(7)), 29)
	testutil.AssertEqInt(t, CountLeadingZeros32(uint32(8)), 28)
	testutil.AssertEqInt(t, CountLeadingZeros32(math.MaxUint32/2), 1)
	testutil.AssertEqInt(t, CountLeadingZeros32(math.MaxUint32/2+1), 0)
	testutil.AssertEqInt(t, CountLeadingZeros32(math.MaxUint32), 0)

	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(0)), 64)
	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(1)), 63)
	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(2)), 62)
	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(3)), 62)
	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(4)), 61)
	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(7)), 61)
	testutil.AssertEqInt(t, CountLeadingZeros64(uint64(8)), 60)
	testutil.AssertEqInt(t, CountLeadingZeros64(math.MaxUint32), 32)
	testutil.AssertEqInt(t, CountLeadingZeros64(math.MaxUint32+1), 31)
	testutil.AssertEqInt(t, CountLeadingZeros64(math.MaxUint64/2), 1)
	testutil.AssertEqInt(t, CountLeadingZeros64(math.MaxUint64/2+1), 0)
	testutil.AssertEqInt(t, CountLeadingZeros64(math.MaxUint64), 0)
}

func TestBitUtil_ByteSwap(t *testing.T) {
	testutil.AssertDeepEq(t, ByteSwap32(0), uint32(0))
	testutil.AssertDeepEq(t, ByteSwap32(0x11223344), uint32(0x44332211))

	testutil.AssertDeepEq(t, ByteSwap64(0), uint64(0))
	testutil.AssertDeepEq(t, ByteSwap64(0x1122334455667788), uint64(0x8877665544332211))

	testutil.AssertDeepEq(t, ByteSwap16(0), uint16(0))
	testutil.AssertDeepEq(t, ByteSwap16(0x1122), uint16(0x2211))
}

func TestBitUtil_SetBitsTo(t *testing.T) {
	for _, fillByte := range [2]byte{0x00, 0xff} {
		{
			// test set within a byte
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			SetBitsTo(bitmap, 2, 2, true)
			SetBitsTo(bitmap, 4, 2, false)
			testutil.AssertBytesEq(t, bitmap[:1], []byte{(fillByte & (^byte(0x3C))) | 0xC})
		}
		{
			// test straddling a single byte boundary
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			SetBitsTo(bitmap, 4, 7, true)
			SetBitsTo(bitmap, 11, 7, false)
			testutil.AssertBytesEq(t, bitmap[:3], []byte{
				(fillByte & 0xF) | 0xF0,
				0x7,
				(fillByte & (^byte(0x3))),
			})
		}
		{
			// test byte aligned end
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			SetBitsTo(bitmap, 4, 4, true)
			SetBitsTo(bitmap, 8, 8, false)
			testutil.AssertBytesEq(t, bitmap[:3], []byte{
				(fillByte & 0xF) | 0xF0,
				0x00,
				fillByte,
			})
		}
		{
			// test byte aligned end, multiple bytes
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			SetBitsTo(bitmap, 0, 24, false)
			falseByte := byte(0)
			testutil.AssertBytesEq(t, bitmap, []byte{
				falseByte,
				falseByte,
				falseByte,
				fillByte,
			})
		}
	}
}
