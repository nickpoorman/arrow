// +build !no_gzip

package compress

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/klauspost/compress/gzip"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
)

var gzipWriterPools map[int]*sync.Pool

func init() {
	gzipWriterPools = make(map[int]*sync.Pool)
	for level := -1; level <= 9; level++ {
		gzipWriterPools[level] = &sync.Pool{
			New: func() interface{} {
				writer, err := gzip.NewWriterLevel(nil, level)
				if err != nil {
					panic(fmt.Errorf("init gzipWriterPool: %w", err))
				}
				return writer
			},
		}
	}

	compressors[CompressionCodec_GZIP] = NewGzipCompressor
}

func NewGzipCompressor(compressionLevel int) Compressor {
	if compressionLevel != kUseDefaultCompressionLevel {
		if compressionLevel < -1 {
			compressionLevel = -1
		} else if compressionLevel > 9 {
			compressionLevel = 9
		}
		return &GzipCompressor{
			compressionLevel: compressionLevel,
		}
	}
	return &GzipCompressor{
		compressionLevel: gzip.DefaultCompression,
	}
}

type GzipCompressor struct {
	compressionLevel int
}

func (c *GzipCompressor) Compress(buf []byte) []byte {
	res := new(bytes.Buffer)
	gzipWriterPool := gzipWriterPools[c.compressionLevel]
	gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
	gzipWriter.Reset(res)
	if _, err := gzipWriter.Write(buf); err != nil {
		debug.Warn(fmt.Errorf("GzipCompressor: Compress: %w", err))
	}
	gzipWriter.Close()
	gzipWriter.Reset(nil)
	gzipWriterPool.Put(gzipWriter)
	return res.Bytes()
}

func (c *GzipCompressor) Uncompress(buf []byte) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	gzipReader, _ := gzip.NewReader(rbuf)
	res, err := ioutil.ReadAll(gzipReader)
	return res, err
}
