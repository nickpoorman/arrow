package compress

import (
	"fmt"
	"math"
)

const kUseDefaultCompressionLevel = math.MinInt64

type CompressorBuilder func(compressionLevel int) Compressor

type Compressor interface {
	Compress(buf []byte) []byte
	Uncompress(buf []byte) ([]byte, error)
}

type Decompressor struct {
	Compressor
}

var compressors map[CompressionCodec]CompressorBuilder

func init() {
	compressors = make(map[CompressionCodec]CompressorBuilder)
}

func Uncompress(buf []byte, compressMethod CompressionCodec) ([]byte, error) {
	b, ok := compressors[compressMethod]
	if !ok {
		return nil, fmt.Errorf("unsupported compress method: %w", CompressException)
	}
	c := b(kUseDefaultCompressionLevel)

	return c.Uncompress(buf)
}

func Compress(buf []byte, compressMethod CompressionCodec) []byte {
	b, ok := compressors[compressMethod]
	if !ok {
		return nil
	}
	c := b(kUseDefaultCompressionLevel)

	return c.Compress(buf)
}
