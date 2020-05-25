package parquet

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
	"github.com/nickpoorman/arrow-parquet-go/parquet/compress"
)

// TODO: Implement this
type Decryptor interface{}

// 16 MB is the default maximum page header size
const kDefaultMaxPageHeaderSize = uint32(16 * 1024 * 1024)

// 16 KB is the default expected page header size
const kDefaultPageHeaderSize = uint32(16 * 1024)

type LevelDecoder struct {
	bitWidth           int
	numValuesRemaining int
	encoding           EncodingType
	rleDecoder         *util.RleDecoder
	bitPackedDecoder   *bitutilext.BitReader
}

func NewLevelDecoder() *LevelDecoder {
	return &LevelDecoder{
		numValuesRemaining: 0,
	}
}

func (l *LevelDecoder) SetData(encoding EncodingType, maxLevel int16,
	numBufferedValues int, data []byte, dataSize int32) (int, error) {

	numBytes := int(0)
	l.encoding = encoding
	l.numValuesRemaining = numBufferedValues
	l.bitWidth = bitutilext.Log2(uint64(maxLevel) + 1)
	switch encoding {
	case EncodingType_RLE:
		if dataSize < 4 {
			return 0, fmt.Errorf(
				"Received invalid levels (corrupt data page?): %w",
				ParquetException,
			)
		}
		numBytes = int(binary.LittleEndian.Uint32(data))
		if numBytes < 0 || numBytes > int(dataSize-4) {
			return -1, fmt.Errorf(
				"Received invalid number of bytes (corrupt data page?): %w",
				ParquetException,
			)
		}
		decoderData := data[4:]
		if l.rleDecoder == nil {
			l.rleDecoder = util.NewRleDecoder(decoderData, numBytes, l.bitWidth)
		} else {
			l.rleDecoder.Reset(decoderData, numBytes, l.bitWidth)
		}
		return 4 + numBytes, nil
	case EncodingType_BIT_PACKED:
		numBytes = int(bitutil.BytesForBits(int64(numBufferedValues * l.bitWidth)))
		if numBytes < 0 || numBytes > int(dataSize-4) {
			return -1, fmt.Errorf(
				"Received invalid number of bytes (corrupt data page?): %w",
				ParquetException,
			)
		}
		if l.bitPackedDecoder == nil {
			l.bitPackedDecoder = bitutilext.NewBitReader(data, numBytes)
		} else {
			l.bitPackedDecoder.Reset(data, numBytes)
		}
		return numBytes, nil
	default:
		return -1, fmt.Errorf(
			"Unknown encoding type for levels.: %w",
			ParquetException,
		)
	}
}

type CryptoContext struct {
	startDecryptWithDictionaryPage bool
	rowGroupOrdinal                int16
	columnOrdinal                  int16
	metaDecryptor                  Decryptor
	dataDecryptor                  Decryptor
}

func NewCryptoContext(
	startDecryptWithDictionaryPage bool,
	rgOrdinal int16, colOrdinal int16,
	meta Decryptor, data Decryptor,
) *CryptoContext {
	return &CryptoContext{
		startDecryptWithDictionaryPage: startDecryptWithDictionaryPage,
		rowGroupOrdinal:                rgOrdinal,
		columnOrdinal:                  colOrdinal,
		metaDecryptor:                  meta,
		dataDecryptor:                  data,
	}
}

// Abstract page iterator interface. This way, we can feed column pages to the
// ColumnReader through whatever mechanism we choose
type PageReader interface {
	Open(stream io.Reader, totalNumRows int64, codec compress.CompressionCodec,
		pool memory.Allocator, ctx *CryptoContext)

	// returns nil on EOS, *Page
	// containing new Page otherwise
	NextPage() *Page

	SetMaxPageHeaderSize(size uint32)
}

type ColumnReader struct {
}

func NewColumnReader(descr *ColumnDescriptor, pager *PageReader,
	pool memory.Allocator) *ColumnReader {
	return &ColumnReader{}
}

// Returns true if there are still values in this column.
func (c *ColumnReader) HasNext() bool {}

func (c *ColumnReader) Type() Type {}

func (c *ColumnReader) ColumnDescriptor() *ColumnDescriptor {}

// API to read values from a single column. This is a main client facing API.
type TypedColumnReader struct {
	ColumnReader
	dtype PhysicalType
}

func NewTypedColumnReader(dtype PhysicalType) *TypedColumnReader {
	return &TypedColumnReader{}
}

// Read a batch of repetition levels, definition levels, and values from the
// column.
//
// Since null values are not stored in the values, the number of values read
// may be less than the number of repetition and definition levels. With
// nested data this is almost certainly true.
//
// Set defLevels or repLevels to nil if you want to skip reading them.
// This is only safe if you know through some other source that there are no
// undefined values.
//
// To fully exhaust a row group, you must read batches until the number of
// values read reaches the number of stored values according to the metadata.
//
// This API is the same for both V1 and V2 of the DataPage
//
// returns actual number of levels read (see values_read for number of values read)
func (t *TypedColumnReader) ReadBatch(batchSize int64, defLevels int16, repLevels int16,
	values interface{}, valuesRead int64) int64 {
}

// Read a batch of repetition levels, definition levels, and values from the
// column and leave spaces for null entries on the lowest level in the values
// buffer.
//
// In comparison to ReadBatch the length of repetition and definition levels
// is the same as of the number of values read for max_definition_level == 1.
// In the case of max_definition_level > 1, the repetition and definition
// levels are larger than the values but the values include the null entries
// with definition_level == (max_definition_level - 1).
//
// To fully exhaust a row group, you must read batches until the number of
// values read reaches the number of stored values according to the metadata.
//
//  batchSize the number of levels to read
//  defLevels The Parquet definition levels, output has
//   the length levelsRead.
//  repLevels The Parquet repetition levels, output has
//   the length levelsRead.
//  values The values in the lowest nested level including
//   spacing for nulls on the lowest levels; output has the length
//   valuesRead.
//  validBits Memory allocated for a bitmap that indicates if
//   the row is null or on the maximum definition level. For performance
//   reasons the underlying buffer should be able to store 1 bit more than
//   required. If this requires an additional byte, this byte is only read
//   but never written to.
//  validBitsOffset The offset in bits of the valid_bits where the
//   first relevant bit resides.
//  levelsRead The number of repetition/definition levels that were read.
//  valuesRead The number of values read, this includes all
//   non-null entries as well as all null-entries on the lowest level
//   (i.e. definitionLevel == maxDefinitionLevel - 1)
//  nullCount The number of nulls on the lowest levels.
//   (i.e. (valuesRead - nullCount) is total number of non-null entries)
func (t *TypedColumnReader) ReadBatchSpaced(
	batchsize int64, defLevels int16,
	repLevels int16, values interface{}, validBits []byte,
	validBitsOffset int64, levelsRead int16,
	valuesRead int64, nullCount int64,
) int64 {
}

// Skip reading levels
// Returns the number of levels skipped
func (t *TypedColumnReader) Skip(numRowsToSkip int64) int64 {}

var BoolReader = NewTypedColumnReader(BooleanType)
var Int32Reader = NewTypedColumnReader(Int32Type)
var Int64Reader = NewTypedColumnReader(Int64Type)
var Int96Reader = NewTypedColumnReader(Int96Type)
var FloatReader = NewTypedColumnReader(FloatType)
var DoubleReader = NewTypedColumnReader(DoubleType)
var ByteArrayReader = NewTypedColumnReader(ByteArrayType)
var FixedLenByteArrayReader = NewTypedColumnReader(FLBAType)
