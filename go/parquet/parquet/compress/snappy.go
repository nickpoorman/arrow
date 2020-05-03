// +build !no_snappy

package compress

import (
	"github.com/golang/snappy"
)

func init() {
	compressors[CompressionCodec_SNAPPY] = NewSnappyCompressor
}

func NewSnappyCompressor(compressionLevel int) Compressor {
	return &SnappyCompressor{}
}

type SnappyCompressor struct{}

func (c *SnappyCompressor) Compress(buf []byte) []byte {
	return snappy.Encode(nil, buf)
}

func (c *SnappyCompressor) Uncompress(buf []byte) (bytes []byte, err error) {
	return snappy.Decode(nil, buf)
}
