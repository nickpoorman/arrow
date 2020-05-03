// +build !no_zstd

package compress

import (
	"github.com/klauspost/compress/zstd"
)

func init() {
	compressors[CompressionCodec_ZSTD] = NewZSTDCompressor
}

func NewZSTDCompressor(compressionLevel int) Compressor {
	el := zstd.SpeedDefault
	if compressionLevel != kUseDefaultCompressionLevel {
		el = zstd.EncoderLevel(compressionLevel)
	}

	// Create encoder/decoder with default parameters.
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(el))
	dec, _ := zstd.NewReader(nil)

	return &ZSTDCompressor{
		enc: enc,
		dec: dec,
	}
}

type ZSTDCompressor struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func (c *ZSTDCompressor) Compress(buf []byte) []byte {
	return c.enc.EncodeAll(buf, nil)
}

func (c *ZSTDCompressor) Uncompress(buf []byte) (bytes []byte, err error) {
	return c.dec.DecodeAll(buf, nil)
}
