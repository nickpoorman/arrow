package compress

import (
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
)

type CompressionCodec int

const (
	CompressionCodec_UNKNOWN CompressionCodec = iota
	CompressionCodec_UNCOMPRESSED
	CompressionCodec_SNAPPY
	CompressionCodec_GZIP
	CompressionCodec_BROTLI
	CompressionCodec_ZSTD
	CompressionCodec_LZ4
	CompressionCodec_LZO
	CompressionCodec_BZ2
)

func CompressionTypeFromThrift(codec parquet.CompressionCodec) (CompressionCodec, error) {
	switch codec {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return CompressionCodec_UNCOMPRESSED, nil
	case parquet.CompressionCodec_SNAPPY:
		return CompressionCodec_SNAPPY, nil
	case parquet.CompressionCodec_GZIP:
		return CompressionCodec_GZIP, nil
	case parquet.CompressionCodec_LZO:
		return CompressionCodec_LZO, nil
	case parquet.CompressionCodec_BROTLI:
		return CompressionCodec_BROTLI, nil
	case parquet.CompressionCodec_LZ4:
		return CompressionCodec_LZ4, nil
	case parquet.CompressionCodec_ZSTD:
		return CompressionCodec_ZSTD, nil
	default:
		return CompressionCodec_UNKNOWN, fmt.Errorf("Unknown compression type: %s: %w", codec.String(), CompressException)
	}
}

// Return a string name for compression type
func GetCodecAsString(codec CompressionCodec) string {
	switch codec {
	case CompressionCodec_UNCOMPRESSED:
		return "UNCOMPRESSED"
	case CompressionCodec_SNAPPY:
		return "SNAPPY"
	case CompressionCodec_GZIP:
		return "GZIP"
	case CompressionCodec_BROTLI:
		return "BROTLI"
	case CompressionCodec_ZSTD:
		return "ZSTD"
	case CompressionCodec_LZ4:
		return "LZ4"
	case CompressionCodec_LZO:
		return "LZO"
	case CompressionCodec_BZ2:
		return "BZ2"
	case CompressionCodec_UNKNOWN:
		return "UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

func CodecCreate(codec CompressionCodec, compressionLevel int) (Compressor, error) {
	b, ok := compressors[codec]
	if !ok {
		return nil, fmt.Errorf("unsupported codec builder: %v: %w", codec, CompressException)
	}

	compressionLevelSet := compressionLevel == kUseDefaultCompressionLevel
	switch codec {
	case CompressionCodec_UNCOMPRESSED:
		if compressionLevelSet {
			return nil, fmt.Errorf("Compression level cannot be specified for UNCOMPRESSED: %w", CompressException)
		}
		return b(compressionLevel), nil

	case CompressionCodec_SNAPPY:
		if compressionLevelSet {
			return nil, fmt.Errorf("Snappy doesn't support setting a compression level: %w", CompressException)
		}
		return b(compressionLevel), nil

	case CompressionCodec_GZIP:
		return b(compressionLevel), nil

	case CompressionCodec_BROTLI:
		return b(compressionLevel), nil

	case CompressionCodec_ZSTD:
		return b(compressionLevel), nil

	case CompressionCodec_LZ4:
		if compressionLevelSet {
			return nil, fmt.Errorf("LZ4 doesn't support setting a compression level: %w", CompressException)
		}
		return b(compressionLevel), nil

	case CompressionCodec_LZO:
		if compressionLevelSet {
			return nil, fmt.Errorf("LZ0 doesn't support setting a compression level: %w", CompressException)
		}
		return b(compressionLevel), nil

	case CompressionCodec_BZ2:
		return b(compressionLevel), nil

	default:
		return nil, fmt.Errorf("Unrecognized codec: %v: %w", codec, CompressException)
	}
}
