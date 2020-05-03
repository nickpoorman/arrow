package parquet

import (
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/parquet/compress"
	"github.com/xitongsys/parquet-go/parquet"
)

// ----------------------------------------------------------------------
// Convert Thrift enums to / from parquet enums

func TypeFromThrift(t parquet.Type) Type {
	return Type(int(t))
}

func ConvertedTypeFromThrift(t parquet.ConvertedType) ConvertedType {
	// item 0 is NONE
	return ConvertedType(int(t) + 1)
}

func RepetitionTypeFromThrift(t parquet.FieldRepetitionType) RepetitionType {
	return RepetitionType(int(t))
}

func EncodingTypeFromThrift(t parquet.Encoding) EncodingType {
	return EncodingType(int(t))
}

// TODO(nickpoorman): Once we switch over to parquet.thrift in arrow project uncomment these.
// func AadMetadataFromThriftAesGcmV1(aesGcmV1 parquet.AesGcmV1) AadMetadata {
// 	return AadMetadata{aesGcmV1.AadPrefix, aesGcmV1.AadFileUnique, aesGcmV1.SupplyAadPrefix}
// }
// func AadMetadataFromThriftAesGcmCtrV1(aesGcmCtrV1 parquet.AesGcmCtrV1) AadMetadata {
// 	return AadMetadata{aesGcmCtrV1.AadPrefix, aesGcmCtrV1.AadFileUnique, aesGcmCtrV1.SupplyAadPrefix}
// }
// func EncryptionAlgorithmFromThrift(t parquet.EncryptionAlgorithm) EncryptionAlgorithm {
// 	// TODO: Implement
// }
// func AesGcmV1ToAesGcmV1Thrift(aad AadMetadata) {
// 	// TODO(nickpoorman): Implement
// }
// func AesGcmV1ToAesGcmCtrV1Thrift(aad AadMetadata) {
// 	// TODO(nickpoorman): Implement
// }
// func EncryptionAlgorithmTohrift(t EncryptionAlgorithm) parquet.EncryptionAlgorithm{
// 	// TODO: Implement
// }

func TypeToThrift(t Type) parquet.Type {
	return parquet.Type(int64(t))
}

func ConvertedTypeToThrift(t ConvertedType) parquet.ConvertedType {
	// item 0 is NONE
	debug.Assert(t != ConvertedType_NONE, "ConvertedTypeToThrift: Cannot convert Arrow NONE ConvertedType to Thrift ConvertedType")
	return parquet.ConvertedType(int64(t) - 1)
}

func RepetitionTypeToThrift(t RepetitionType) parquet.FieldRepetitionType {
	return parquet.FieldRepetitionType(int64(t))
}

func EncodingTypeToThrift(t EncodingType) parquet.Encoding {
	return parquet.Encoding(int64(t))
}

func CompressionCodecFromThrift(t parquet.CompressionCodec) compress.CompressionCodec {
	switch t {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return compress.CompressionCodec_UNCOMPRESSED
	case parquet.CompressionCodec_SNAPPY:
		return compress.CompressionCodec_SNAPPY
	case parquet.CompressionCodec_GZIP:
		return compress.CompressionCodec_GZIP
	case parquet.CompressionCodec_LZO:
		return compress.CompressionCodec_LZO
	case parquet.CompressionCodec_BROTLI:
		return compress.CompressionCodec_BROTLI
	case parquet.CompressionCodec_LZ4:
		return compress.CompressionCodec_LZ4
	case parquet.CompressionCodec_ZSTD:
		return compress.CompressionCodec_ZSTD
	default:
		debug.Assert(false, "Cannot reach here")
		return compress.CompressionCodec_UNCOMPRESSED
	}
}

func CompressionCodecToThrift(t compress.CompressionCodec) parquet.CompressionCodec {
	switch t {
	case compress.CompressionCodec_UNCOMPRESSED:
		return parquet.CompressionCodec_UNCOMPRESSED
	case compress.CompressionCodec_SNAPPY:
		return parquet.CompressionCodec_SNAPPY
	case compress.CompressionCodec_GZIP:
		return parquet.CompressionCodec_GZIP
	case compress.CompressionCodec_LZO:
		return parquet.CompressionCodec_LZO
	case compress.CompressionCodec_BROTLI:
		return parquet.CompressionCodec_BROTLI
	case compress.CompressionCodec_LZ4:
		return parquet.CompressionCodec_LZ4
	case compress.CompressionCodec_ZSTD:
		return parquet.CompressionCodec_ZSTD
	default:
		debug.Assert(false, "Cannot reach here")
		return parquet.CompressionCodec_UNCOMPRESSED
	}
}

func StatisticsToThrift(stats EncodedStatistics) parquet.Statistics {
	// TODO(nickpoorman): Implement once we have implemented statistics
	return parquet.Statistics{}
}

// ----------------------------------------------------------------------
// Thrift struct serialization / deserialization utilities

// TODO: Implement the rest of thrift_internal.h
