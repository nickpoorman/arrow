package util

import "github.com/apache/arrow/go/arrow/bitutil"

type TransferMode int

const (
	_ TransferMode = iota
	TransferMode_Copy
	TransferMode_Invert
)

func TransferBitmap(
	mode TransferMode,
	data []byte, offset int64, length int64, destOffset int64, dest []byte) {

	bitOffset := offset % 8
	destBitOffset := destOffset % 8

	if bitOffset != 0 || destBitOffset != 0 {
		reader := NewBitmapWordReader(data, offset, length)
		writer := NewBitmapWordWriter(dest, destOffset, length)

		nwords := reader.Words()

		if mode == TransferMode_Invert {
			for ; nwords > 0; nwords-- {
				b, validBits := reader.NextTrailingByte()
				writer.PutNextTrailingByte(^b, validBits)
			}
		} else {
			for ; nwords > 0; nwords-- {
				b, validBits := reader.NextTrailingByte()
				writer.PutNextTrailingByte(b, validBits)
			}
		}
	} else if length != 0 {
		numBytes := bitutil.BytesForBits(length)

		// Shift by its byte offset
		data = data[offset/8:]
		dest = dest[destOffset/8:]

		// Take care of the trailing bits in the last byte
		// E.g., if trailing_bits = 5, last byte should be
		// - low  3 bits: new bits from last byte of data buffer
		// - high 5 bits: old bits from last byte of dest buffer
		trailingBits := numBytes*8 - length
		trailMask := (uint8(1) << (8 - trailingBits)) - 1
		var lastData byte

		if mode == TransferMode_Invert {
			for i := 0; i < numBytes; i++ {
				dest[i] = ^(data[i])
			}
			lastData = ^(data[numBytes-1])
		} else {
			copy(dest, data[:numBytes-1])
			lastData = data[numBytes-1]
		}

		// Set last byte
		dest[numBytes-1] &= ^(trailMask)
		dest[numBytes-1] |= lastData & trailMask
	}
}

func CopyBitmap(
	data []byte, offset int64, length int64, dest []byte, destOffset int64) {
	TransferBitmap(TransferMode_Copy, data, offset, length, destOffset, dest)
}
