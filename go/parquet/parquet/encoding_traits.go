package parquet

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

type TypedEncoder struct {
	PhysicalType
}

var BooleanEncoder = TypedEncoder{BooleanType}
var Int32Encoder = TypedEncoder{Int32Type}
var Int64Encoder = TypedEncoder{Int64Type}
var Int96Encoder = TypedEncoder{Int96Type}
var FloatEncoder = TypedEncoder{FloatType}
var DoubleEncoder = TypedEncoder{DoubleType}
var ByteArrayEncoder = TypedEncoder{ByteArrayType}
var FLBAEncoder = TypedEncoder{FLBAType}

type TypedDecoder struct {
	PhysicalType
}

var BooleanDecoder = TypedDecoder{BooleanType}
var Int32Decoder = TypedDecoder{Int32Type}
var Int64Decoder = TypedDecoder{Int64Type}
var Int96Decoder = TypedDecoder{Int96Type}
var FloatDecoder = TypedDecoder{FloatType}
var DoubleDecoder = TypedDecoder{DoubleType}
var ByteArrayDecoder = TypedDecoder{ByteArrayType}
var FLBADecoder = TypedDecoder{FLBAType}

type Accumulator struct {
	Builder array.Builder
	Chunks  []array.Interface
}

func NewAccumulator(builder array.Builder) *Accumulator {
	return &Accumulator{
		Builder: builder,
		Chunks:  make([]array.Interface, 0),
	}
}

type EncodingTraits struct {
	Encoder TypedEncoder
	Decoder TypedDecoder

	ArrowType   arrow.DataType
	Accumulator func(mem memory.Allocator, dtype arrow.DataType) *Accumulator
	// TODO: Implement DictionaryBuilder?
	// DictAccumulator array.Builder
}

var BooleanEncodingTraits = EncodingTraits{
	Encoder: BooleanEncoder,
	Decoder: BooleanDecoder,

	ArrowType: arrow.FixedWidthTypes.Boolean,
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewBooleanBuilder(mem))
	},
}

var Int32EncodingTraits = EncodingTraits{
	Encoder: Int32Encoder,
	Decoder: Int32Decoder,

	ArrowType: arrow.PrimitiveTypes.Int32,
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewInt32Builder(mem))
	},
}

var Int64EncodingTraits = EncodingTraits{
	Encoder: Int64Encoder,
	Decoder: Int64Decoder,

	ArrowType: arrow.PrimitiveTypes.Int64,
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewInt64Builder(mem))
	},
}

var Int96EncodingTraits = EncodingTraits{
	Encoder: Int96Encoder,
	Decoder: Int96Decoder,
}

var FloatEncodingTraits = EncodingTraits{
	Encoder: FloatEncoder,
	Decoder: FloatDecoder,

	ArrowType: arrow.PrimitiveTypes.Float32,
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewFloat32Builder(mem))
	},
}

var DoubleEncodingTraits = EncodingTraits{
	Encoder: DoubleEncoder,
	Decoder: DoubleDecoder,

	ArrowType: arrow.PrimitiveTypes.Float64,
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewFloat64Builder(mem))
	},
}

var ByteArrayEncodingTraits = EncodingTraits{
	Encoder: ByteArrayEncoder,
	Decoder: ByteArrayDecoder,

	ArrowType: arrow.BinaryTypes.Binary,
	// Internal helper class for decoding BYTE_ARRAY data where we can
	// overflow the capacity of a single arrow::BinaryArray
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewBinaryBuilder(mem, dtype.(arrow.BinaryDataType)))
	},
}

var FLBAEncodingTraits = EncodingTraits{
	Encoder: FLBAEncoder,
	Decoder: FLBADecoder,

	// TODO: Probably going to need a different solution for this
	// since we'll need to specify the ByteWidth.
	ArrowType: &arrow.FixedSizeBinaryType{},
	Accumulator: func(mem memory.Allocator, dtype arrow.DataType) *Accumulator {
		return NewAccumulator(array.NewFixedSizeBinaryBuilder(mem, dtype.(*arrow.FixedSizeBinaryType)))
	},
}
