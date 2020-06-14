package parquet

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/bytearray"
	u "github.com/nickpoorman/arrow-parquet-go/internal/util"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
	"github.com/nickpoorman/arrow-parquet-go/parquet/compress"
	"github.com/xitongsys/parquet-go/parquet"
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

func (l *LevelDecoder) SetDataV2(numBytes int, maxLevel int16,
	numBufferedValues int, data []byte) error {
	// Repetition and definition levels always uses RLE encoding
	// in the DataPageV2 format.
	if numBytes < 0 {
		return fmt.Errorf(
			"Invalid page header (corrupt data page?): %w",
			ParquetException,
		)
	}
	l.encoding = EncodingType_RLE
	l.numValuesRemaining = numBufferedValues
	l.bitWidth = bitutilext.Log2(uint64(maxLevel) + 1)

	if l.rleDecoder == nil {
		l.rleDecoder = util.NewRleDecoder(data, numBytes, l.bitWidth)
	} else {
		l.rleDecoder.Reset(data, numBytes, l.bitWidth)
	}
	return nil
}

func (l *LevelDecoder) Decode(batchSize int, levels []int16) int {
	numDecoded := 0

	numValues := u.MinInt(l.numValuesRemaining, batchSize)
	if l.encoding == EncodingType_RLE {
		numDecoded = l.rleDecoder.GetBatch(
			bytearray.Int16CastToBytes(levels), numValues)
	} else {
		numDecoded = l.bitPackedDecoder.GetBatch(
			l.bitWidth,
			bytearray.NewByteArray(bytearray.Int16CastToBytes(levels), bytearray.Int16Size),
			numValues)
	}
	l.numValuesRemaining -= numDecoded
	return numDecoded
}

func defaultReaderProperties() *ReaderProperties {
	return NewDefaultReaderProperties()
}

// Extracts encoded statistics from V1 and V2 data page headers
func ExtractStatsFromHeader(header interface {
	IsSetStatistics() bool
	GetStatistics() *parquet.Statistics
}) *EncodedStatistics {
	pageStatistics := NewEncodedStatistics()
	if !header.IsSetStatistics() {
		return pageStatistics
	}
	stats := header.GetStatistics()
	if stats.IsSetMax() {
		pageStatistics.setMax(string(stats.GetMax()))
	}
	if stats.IsSetMin() {
		pageStatistics.setMin(string(stats.GetMin()))
	}
	if stats.IsSetNullCount() {
		pageStatistics.setNullCount(stats.GetNullCount())
	}
	if stats.IsSetDistinctCount() {
		pageStatistics.setDistinctCount(stats.GetDistinctCount())
	}
	return pageStatistics
}

// ----------------------------------------------------------------------
// SerializedPageReader deserializes Thrift metadata and pages that have been
// assembled in a serialized stream for storing in a Parquet files

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
type SerializedPageReader struct {
	stream ArrowInputStream

	currentPageHeader parquet.PageHeader
	currentPage       Page

	// Compression codec to use.
	decompressor        compress.Compressor
	decompressionBuffer *memory.Buffer

	// The fields below are used for calculation of AAD (additional authenticated data)
	// suffix which is part of the Parquet Modular Encryption.
	// The AAD suffix for a parquet module is built internally by
	// concatenating different parts some of which include
	// the row group ordinal, column ordinal and page ordinal.
	// Please refer to the encryption specification for more details:
	// https://github.com/apache/parquet-format/blob/encryption/Encryption.md#44-additional-authenticated-data
	cryptoCtx   CryptoContext
	pageOrdinal int16 // page ordinal does not count the dictionary page

	// Maximum allowed page size
	maxPageHeaderSize uint32

	// Number of rows read in data pages so far
	seenNumRows int64

	// Number of rows in all the data pages
	totalNumRows int64

	// data_page_aad_ and data_page_header_aad_ contain the AAD for data page and data page
	// header in a single column respectively.
	// While calculating AAD for different pages in a single column the pages AAD is
	// updated by only the page ordinal.
	dataPageAad       string
	dataPageHeaderAad string
	// Encryption
	decryptionBuffer *memory.Buffer
}

var _ PageReader = (*SerializedPageReader)(nil)

func NewSerializedPageReader(
	// stream io.Reader, // TODO: Wrap our streams as an io.Reader
	stream ArrowInputStream,
	totalNumRows int64,
	codec compress.CompressionCodec,
	pool memory.Allocator,
	cryptoCtx *CryptoContext,
) (*SerializedPageReader, error) {
	decompressor, err := GetCodec(codec)
	if err != nil {
		return nil, err
	}

	spr := &SerializedPageReader{
		stream:              stream,
		decompressionBuffer: AllocateBuffer(pool, 0),
		pageOrdinal:         0,
		seenNumRows:         0,
		totalNumRows:        totalNumRows,
		decryptionBuffer:    AllocateBuffer(pool, 0),
		maxPageHeaderSize:   kDefaultMaxPageHeaderSize,
		decompressor:        decompressor,
	}
	if cryptoCtx != nil {
		spr.cryptoCtx = *cryptoCtx
		spr.InitDecryption()
	}

	return spr, nil
}

// Implement the PageReader interface
func (s *SerializedPageReader) NextPage() (*Page, error) {
	// Loop here because there may be unhandled page types that we skip until
	// finding a page that we do know what to do with

	for s.seenNumRows < s.totalNumRows {
		var headerSize uint32 = 0
		var allowedPageSize uint32 = kDefaultMaxPageHeaderSize

		// Page headers can be very large because of page statistics
		// We try to deserialize a larger buffer progressively
		// until a maximum allowed header limit
		for {
			view, err := s.stream.Peek(int64(allowedPageSize))
			if err != nil {
				return nil, err
			}
			if len(view) == 0 {
				return nil, nil
			}

			// This gets used, then set by DeserializeThriftMsg
			headerSize := uint32(len(view))
			err = func() error {
				if s.cryptoCtx.metaDecryptor != nil {
					panic("not yet implemented")
					// if err := s.UpdateDecryption(
					// 	s.cryptoCtx.metaDecryptor,
					// 	encryption.kDictionaryPageHeader,
					// 	s.dataPageHeaderAad,
					// ); err != nil {
					//	return err
					// }
				}
				if err := DeserializeThriftMsg(
					view,
					&headerSize,
					s.currentPageHeader,
					s.cryptoCtx.metaDecryptor,
				); err != nil {
					return err
				}
				return nil
			}()
			if err == nil {
				break
			} else {
				// Failed to deserialize. Double the allowed page header size and try again
				allowedPageSize *= 2
				if allowedPageSize > s.maxPageHeaderSize {
					return nil, fmt.Errorf(
						"Deserializing page header failed.:\n %w: %w",
						err,
						ParquetException,
					)
				}
			}
		}
		// Advance the stream offset
		if err := s.stream.Advance(int64(headerSize)); err != nil {
			return nil, err
		}

		compressedLen := s.currentPageHeader.GetCompressedPageSize()
		uncompressedLen := s.currentPageHeader.GetUncompressedPageSize()
		if s.cryptoCtx.dataDecryptor != nil {
			panic("not yet implemented")
			// s.UpdateDecryption(
			// 	s.cryptoCtx,
			// 	encryption.kDictionaryPage,
			// 	s.dataPageAad,
			// )
		}
		// Read the compressed data page.
		// pageBuffer := make([]byte, compressedLen)
		pageBuffer, err := s.stream.ReadToBuffer(int64(compressedLen))
		if err != nil {
			return nil, err
		}
		if pageBuffer.Len() != int(compressedLen) {
			return nil, fmt.Errorf(
				"Page was smaller (%d) than expected (%d): %w",
				pageBuffer.Len(),
				compressedLen,
				ParquetException,
			)
		}

		// Decrypt it if we need to
		if s.cryptoCtx.dataDecryptor != nil {
			panic("not yet implemented")
		}

		// Uncompress it if we need to
		if s.decompressor != nil {
			pageBuffer = s.DecompressPage(
				int(compressedLen), int(uncompressedLen), pageBuffer.Bytes())
		}

		pageType := s.currentPageHeader.GetType()

		switch pageType {
		case parquet.PageType_DICTIONARY_PAGE:
			s.cryptoCtx.startDecryptWithDictionaryPage = false
			dictHeader := s.currentPageHeader.GetDictionaryPageHeader()

			isSorted := dictHeader.IsSetIsSorted() && dictHeader.GetIsSorted()
			if dictHeader.GetNumValues() < 0 {
				return nil, fmt.Errorf(
					"Invalid page header (negative number of values): %w",
					ParquetException,
				)
			}
			return NewDictionaryPage(
				pageBuffer,
				dictHeader.GetNumValues(),
				dictHeader.GetEncoding(),
				isSorted,
			)
		case parquet.PageType_DATA_PAGE:
			s.pageOrdinal++
			header := s.currentPageHeader.GetDataPageHeader()

			if header.GetNumValues() < 0 {
				fmt.Errorf(
					"Invalid page header (negative number of values): %w",
					ParquetException,
				)
			}
			pageStatistics := ExtractStatsFromHeader(header)
			s.seenNumRows += int64(header.GetNumValues())

			return NewDataPageV1(
				pageBuffer,
				header.GetNumValues(),
				header.GetEncoding(),
				header.GetDefinitionLevelEncoding(),
				header.GetRepetitionLevelEncoding(),
				uncompressedLen,
				pageStatistics,
			)
		case parquet.PageType_DATA_PAGE_V2:
			s.pageOrdinal++
			header := s.currentPageHeader.GetDataPageHeaderV2()

			if header.GetNumValues() < 0 {
				fmt.Errorf(
					"Invalid page header (negative number of values): %w",
					ParquetException,
				)
			}
			if header.GetDefinitionLevelsByteLength() < 0 ||
				header.GetRepetitionLevelsByteLength() < 0 {
				fmt.Errorf(
					"Invalid page header (negative levels byte length): %w",
					ParquetException,
				)
			}
			isCompressed := header.IsSetIsCompressed() && header.GetIsCompressed()
			pageStatistics := ExtractStatsFromHeader(header)
			s.seenNumRows += int64(header.GetNumValues())

			return NewDataPageV2(
				pageBuffer,
				header.GetNumValues(),
				header.GetNumNulls(),
				header.GetNumRows(),
				header.GetEncoding(),
				header.GetDefinitionLevelsByteLength(),
				header.GetRepetitionLevelsByteLength(),
				uncompressedLen,
				isCompressed,
				pageStatistics,
			)
		default:
			// We don't know what this page type is. We're allowed to skip non-data
			// pages.
			continue
		}
	}
	return nil, nil
}

func (s *SerializedPageReader) setMaxPageHeaderSize(size uint32) {
	s.maxPageHeaderSize = size
}

func (s *SerializedPageReader) UpdateDecryption(
	decryptor Decryptor,
	moduleType int8,
	pageAad string,
) error {
	panic("not yet implemented")
	// 	debug.Assert(decryptor != nil, "Assert: decryptor != nil")
	// 	if s.cryptoCtx.startDecryptWithDictionaryPage {
	// 		aad := encryption.CreateModuleAad(
	// 			decryptor.FileAad(), moduleType, s.cryptoCtx.rowGroupOrdinal,
	// 			s.cryptoCtx.columnOrdinal, kNonPageOrdinal,
	// 		)
	// 		decryptor.UpdateAad(aad)
	// 	} else {
	// 		encryption.QuickUpdatePageAad(pageAad, s.pageOrdinal)
	// 		decryptor.UpdateAad(pageAad)
	// 	}
}

func (s *SerializedPageReader) InitDecryption() {
	panic("not yet implemented")
	// 	// Prepare the AAD for quick update later.
	// 	if s.cryptoCtx.dataDecryptor != nil {
	// 		debug.Assert(s.cryptoCtx.dataDecryptor.FileAad().empty(), "Assert: FileAad empty")
	// 		s.dataPageAad = encryption.CreateModuleAad(
	// 			s.cryptoCtx.dataDecryptor.FileAad(), Encryption_kDataPage,
	// 			s.cryptoCtx.rowGroupOrdinal, s.cryptoCtx.columnOrdinal, kNonPageOrdinal,
	// 		)
	// 	}
	// 	if s.cryptoCtx.metaDecryptor != nil {
	// 		debug.Assert(s.cryptoCtx.metaDecryptor.FileAad().empty(), "Assert: FileAad empty")
	// 		s.dataPageHeaderAad = encryption.CreateModuleAad(
	// 			s.cryptoCtx.metaDecryptor.FileAad(), Encryption_kDataPageHeader,
	// 			s.cryptoCtx.rowGroupOrdinal, s.cryptoCtx.columnOrdinal, kNonPageOrdinal,
	// 		)
	// 	}
}

func (s *SerializedPageReader) DecompressPage(
	compressedLen int, uncompressedLen int,
	pageBuffer []byte,
) *memory.Buffer {
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
