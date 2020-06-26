package bitutil

import (
	"math/bits"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

var (
	// Bitmask selecting the (k - 1) preceding bits in a byte
	kPrecedingBitmask         = [8]byte{0, 1, 3, 7, 15, 31, 63, 127}
	kPrecedingWrappingBitmask = [8]byte{255, 1, 3, 7, 15, 31, 63, 127}

	// the bitwise complement version of kPrecedingBitmask
	kTrailingBitmask = [8]byte{255, 254, 252, 248, 240, 224, 192, 128}
)

type BitmapReader struct {
	bitmap   []byte
	position int
	length   int

	currentByte byte
	byteOffset  int
	bitOffset   int
}

func NewBitmapReader(bitmap []byte, startOffset int, length int) *BitmapReader {
	br := BitmapReader{
		bitmap:      bitmap,
		position:    0,
		length:      length,
		currentByte: 0,
		byteOffset:  startOffset / 8,
		bitOffset:   startOffset % 8,
	}
	if length > 0 {
		br.currentByte = bitmap[br.byteOffset]
	}
	return &br
}

func (br *BitmapReader) IsSet() bool {
	return br.currentByte&(1<<br.bitOffset) != 0
}

func (br *BitmapReader) IsNotSet() bool {
	return br.currentByte&(1<<br.bitOffset) == 0
}

func (br *BitmapReader) Next() {
	br.bitOffset++
	br.position++
	if br.bitOffset == 8 { // branch predict false
		br.bitOffset = 0
		br.byteOffset++
		if br.position >= br.length { // branch predict false
			return
		}
		br.currentByte = br.bitmap[br.byteOffset]
	}
}

func (br *BitmapReader) Position() int { return br.position }

type BitmapWriter struct {
	// A sequential bitwise writer that preserves surrounding bit values.
	bitmap    []byte
	position_ int
	length    int

	currentByte byte
	bitMask     byte
	byteOffset  int
}

func NewBitmapWriter(bitmap []byte, startOffset int, length int) *BitmapWriter {
	bw := BitmapWriter{
		bitmap:     bitmap,
		position_:  0,
		length:     length,
		byteOffset: startOffset / 8,
		bitMask:    bitutil.BitMask[startOffset%8], // TODO: Remove comment: bitutil.BitMask[byte(startOffset)%8],
	}
	if length > 0 {
		bw.currentByte = bitmap[bw.byteOffset]
	} else {
		bw.currentByte = 0
	}
	return &bw
}

func (br *BitmapWriter) Set() { br.currentByte |= br.bitMask }

func (br *BitmapWriter) Clear() { br.currentByte &= br.bitMask ^ 0xFF }

func (br *BitmapWriter) Next() {
	br.bitMask = br.bitMask << 1
	br.position_++
	if br.bitMask == 0 {
		// Finished this byte, need advancing
		br.bitMask = 0x01
		br.bitmap[br.byteOffset] = br.currentByte
		br.byteOffset++
		if br.position_ >= br.length { // branch predict false
			return
		}
		br.currentByte = br.bitmap[br.byteOffset]
	}
}

func (br *BitmapWriter) Finish() {
	// Store current byte if we didn't went past bitmap storage
	if br.length > 0 && (br.bitMask != 0x01 || br.position_ < br.length) {
		br.bitmap[br.byteOffset] = br.currentByte
	}
}

func (br *BitmapWriter) Position() int { return br.position_ }

// -------------

// Returns ceil(log2(x)).
func Log2(x uint64) int {
	return NumRequiredBits(x - 1)
}

// Returns the minimum number of bits needed to represent an unsigned value
func NumRequiredBits(x uint64) int {
	return 64 - CountLeadingZeros64(x)
}

// Count the number of leading zeros in an unsigned 32-bit integer.
func CountLeadingZeros32(value uint32) int {
	return bits.LeadingZeros32(value)
}

// Count the number of leading zeros in an unsigned 64-bit integer.
func CountLeadingZeros64(value uint64) int {
	return bits.LeadingZeros64(value)
}

func CeilDiv(value int64, divisor int64) int64 {
	if value == 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func TrailingBits(v uint64, numBits int) uint64 {
	if numBits == 0 {
		return 0
	}
	if numBits >= 64 {
		return v
	}
	n := 64 - numBits
	return (v << n) >> n
}

//
// Byte-swap 16-bit, 32-bit and 64-bit values
//

// Swap the byte order (i.e. endianness)
// https://github.com/golang/go/blob/11da2b227a71c9c041320e22843047ad9b0ab1a8/src/runtime/internal/sys/intrinsics.go#L61
func ByteSwap64(value uint64) uint64 { return bits.ReverseBytes64(value) }

func ByteSwap32(value uint32) uint32 { return bits.ReverseBytes32(value) }

func ByteSwap16(value uint16) uint16 { return bits.ReverseBytes16(value) }

func SetBitsTo(bits []byte, startOffset int64, length int64, bitsAreSet bool) {
	if length == 0 {
		return
	}

	iBegin := startOffset
	iEnd := startOffset + length
	var fillByte byte
	if bitsAreSet {
		fillByte = 255
	}

	bytesBegin := iBegin / 8
	bytesEnd := iEnd/8 + 1

	firstByteMask := kPrecedingBitmask[iBegin%8]
	lastByteMask := kTrailingBitmask[iEnd%8]

	if bytesEnd == bytesBegin+1 {
		// set bits within a single byte
		onlyByteMask := firstByteMask
		if iEnd%8 != 0 {
			onlyByteMask = firstByteMask | lastByteMask
		}
		bits[bytesBegin] &= onlyByteMask
		bits[bytesBegin] |= (fillByte & byte(^onlyByteMask))
		return
	}

	// set/clear trailing bits of first byte
	bits[bytesBegin] &= firstByteMask
	bits[bytesBegin] |= (fillByte & byte(^firstByteMask))

	if bytesEnd-bytesBegin > 2 {
		// set/clear whole bytes
		memory.Set(bits[bytesBegin+1:bytesEnd-bytesBegin-2], fillByte)
	}

	if iEnd%8 == 0 {
		return
	}

	// set/clear leading bits of last byte
	bits[bytesEnd-1] &= lastByteMask
	bits[bytesEnd-1] |= (fillByte & byte(^lastByteMask))
}
