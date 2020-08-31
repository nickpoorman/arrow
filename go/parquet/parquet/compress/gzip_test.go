package compress

import (
	"bytes"
	"testing"
)

func TestGzipCompression(t *testing.T) {
	gzipCompressorBuilder := compressors[CompressionCodec_GZIP]
	gzipCompressor := gzipCompressorBuilder(kUseDefaultCompressionLevel)
	input := []byte("test data")
	compressed := gzipCompressor.Compress(input)
	output, err := gzipCompressor.Uncompress(compressed)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(input, output) {
		t.Fatalf("expected output %s but was %s", string(input), string(output))
	}
}

func BenchmarkGzipCompression(b *testing.B) {
	gzipCompressorBuilder := compressors[CompressionCodec_GZIP]
	gzipCompressor := gzipCompressorBuilder(kUseDefaultCompressionLevel)
	input := []byte("test data")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gzipCompressor.Compress(input)
	}
}
