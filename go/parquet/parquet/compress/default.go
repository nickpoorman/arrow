package compress

func init() {
	compressors[CompressionCodec_UNCOMPRESSED] = NewUncompressedCompressor
}

func NewUncompressedCompressor(compressionLevel int) Compressor {
	return &UncompressedCompressor{}
}

type UncompressedCompressor struct{}

func (c *UncompressedCompressor) Compress(buf []byte) []byte {
	return buf
}

func (c *UncompressedCompressor) Uncompress(buf []byte) (bytes []byte, err error) {
	return buf, nil
}
