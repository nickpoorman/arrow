// +build !no_snappy

package compress

import (
	"fmt"

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

func (c *SnappyCompressor) UncompressTo(dst []byte, src []byte) error {
	dLen, err := snappy.DecodedLen(src)
	if err != nil {
		return err
	}
	if dLen > len(dst) {
		return fmt.Errorf(
			"Output buffer size (%d) must be %d or larger.",
			len(dst), dLen,
		)
	}
	_, err = snappy.Decode(dst, src)
	return err
}
