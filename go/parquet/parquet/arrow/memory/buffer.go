package memory

import (
	"bytes"

	"github.com/apache/arrow/go/arrow/memory"
)

func BuffersEqual(left *memory.Buffer, right *memory.Buffer) bool {
	leftBuf := left.Buf()
	rightBuff := right.Buf()
	return left == right || (left.Len() == right.Len() &&
		(&leftBuf == &rightBuff ||
			bytes.Equal(left.Bytes(), right.Bytes())))
}

func BuffersEqualNBytes(left *memory.Buffer, right *memory.Buffer, nbytes int) bool {
	leftBuf := left.Buf()
	rightBuff := right.Buf()
	return left == right || (left.Len() >= nbytes && right.Len() >= nbytes &&
		(&leftBuf == &rightBuff ||
			bytes.Equal(left.Bytes()[:nbytes], right.Bytes()[:nbytes])))
}
