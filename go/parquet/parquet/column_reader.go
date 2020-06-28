package parquet

import (
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/bytearray"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
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
	return NewReaderProperties(memory.DefaultAllocator)
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
	currentPage       *Page

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
func (s *SerializedPageReader) NextPage() (Page, error) {
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
			pageBuffer, err = s.DecompressPage(
				int(compressedLen), int(uncompressedLen), pageBuffer.Bytes(),
			)
			if err != nil {
				return nil, err
			}
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
				EncodingTypeFromThrift(dictHeader.GetEncoding()),
				isSorted,
			), nil
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
				EncodingTypeFromThrift(header.GetEncoding()),
				EncodingTypeFromThrift(header.GetDefinitionLevelEncoding()),
				EncodingTypeFromThrift(header.GetRepetitionLevelEncoding()),
				int64(uncompressedLen),
				pageStatistics,
			), nil
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
				EncodingTypeFromThrift(header.GetEncoding()),
				header.GetDefinitionLevelsByteLength(),
				header.GetRepetitionLevelsByteLength(),
				int64(uncompressedLen),
				isCompressed,
				pageStatistics,
			), nil
		default:
			// We don't know what this page type is. We're allowed to skip non-data
			// pages.
			continue
		}
	}
	return nil, nil
}

func (s *SerializedPageReader) SetMaxPageHeaderSize(size uint32) {
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
) (*memory.Buffer, error) {
	// Grow the uncompressed buffer if we need to.
	if uncompressedLen > s.decompressionBuffer.Len() {
		s.decompressionBuffer.ResizeNoShrink(uncompressedLen)
	}

	if s.currentPageHeader.Type != parquet.PageType_DATA_PAGE_V2 {
		if err := s.decompressor.UncompressTo(
			s.decompressionBuffer.Buf()[:uncompressedLen],
			pageBuffer[:compressedLen]); err != nil {
			return nil, err
		}
	} else {
		// The levels are not compressed in V2 format
		header := s.currentPageHeader.GetDataPageHeaderV2()
		levelsLength := int(header.GetRepetitionLevelsByteLength() + header.GetDefinitionLevelsByteLength())
		decompressed := s.decompressionBuffer.Buf()
		copy(decompressed, pageBuffer[:levelsLength])
		decompressed = decompressed[levelsLength:]
		compressedValues := pageBuffer[levelsLength:]

		// Decompress the values
		if err := s.decompressor.UncompressTo(
			decompressed[:uncompressedLen-levelsLength],
			compressedValues[:compressedLen-levelsLength],
		); err != nil {
			return nil, err
		}
	}

	return s.decompressionBuffer, nil
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
	// returns nil on EOS, *Page
	// containing new Page otherwise
	NextPage() (Page, error)

	SetMaxPageHeaderSize(size uint32)
}

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
func isDictionaryIndexEncoding(e EncodingType) bool {
	return e == EncodingType_RLE_DICTIONARY || e == EncodingType_PLAIN_DICTIONARY
}

type ColumnReader interface {
	// Returns true if there are still values in this column.
	HasNext() (bool, error)
	Type() Type
	Descr() *ColumnDescriptor
}

// func NewColumnReader(dtype PhysicalType,
// 	descr *ColumnDescriptor, pager *PageReader,
// 	pool memory.Allocator) *ColumnReader {
// 	return &ColumnReader{
// 		dtype: dtype,
// 	}
// }

// // Returns true if there are still values in this column.
// func (c *ColumnReader) HasNext() bool {
// 	panic("not yet implemented")
// }

// func (c *ColumnReader) Type() Type {
// 	panic("not yet implemented")
// }

// func (c *ColumnReader) ColumnDescriptor() *ColumnDescriptor {
// 	panic("not yet implemented")
// }

// type TypedColumnReader interface {
// 	Type() PhysicalType

// 	ReadBatch(batchSize int64, defLevels int16, repLevels int16,
// 		values interface{}, valuesRead int64) int64

// 	ReadBatchSpaced(
// 		batchsize int64, defLevels int16,
// 		repLevels int16, values interface{}, validBits []byte,
// 		validBitsOffset int64, levelsRead int16,
// 		valuesRead int64, nullCount int64,
// 	) int64

// 	Skip(numRowsToSkip int64) int64
// }

// API to read values from a single column. This is a main client facing API.
type TypedColumnReader interface {

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
	ReadBatch(batchSize int64, defLevels []int16, repLevels []int16,
		values interface{}, valuesRead *int64) (int64, error)

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
	ReadBatchSpaced(batchSize int64, defLevels []int16,
		repLevels []int16, values interface{}, validBits []byte,
		validBitsOffset int64, levelsRead *int64,
		valuesRead *int64, nullCountOut *int64) (int64, error)

	// Skip reading levels
	// Returns the number of levels skipped
	Skip(numRowsToSkip int64) (int64, error)
}

type columnReaderImplBase struct {
	dtype       PhysicalType
	descr       *ColumnDescriptor
	maxDefLevel int16
	maxRepLevel int16

	pager       PageReader
	currentPage Page

	// Not set if full schema for this field has no optional or repeated elements
	definitionLevelDecoder LevelDecoder

	// Not set for flat schemas.
	repetitionLevelDecoder LevelDecoder

	// The total number of values stored in the data page. This is the maximum of
	// the number of encoded definition levels or encoded values. For
	// non-repeated, required columns, this is equal to the number of encoded
	// values. For repeated or optional values, there may be fewer data values
	// than levels, and this tells you how many encoded levels there are in that
	// case.
	numBufferedValues int64

	// The number of values from the current data page that have been decoded
	// into memory
	numDecodedValues int64

	pool memory.Allocator

	currentDecoder  TypedDecoderInterface
	currentEncoding EncodingType

	// Flag to signal when a new dictionary has been set, for the benefit of
	// DictionaryRecordReader
	newDictionary bool

	// Map of encoding type to the respective decoder object. For example, a
	// column chunk's data pages may include both dictionary-encoded and
	// plain-encoded data.
	decoders map[int]TypedDecoderInterface
}

func newColumnReaderImplBase(descr *ColumnDescriptor,
	pool memory.Allocator) *columnReaderImplBase {

	return &columnReaderImplBase{
		descr:             descr,
		maxDefLevel:       descr.MaxDefinitionLevel,
		maxRepLevel:       descr.MaxRepetitionLevel,
		numBufferedValues: 0,
		numDecodedValues:  0,
		pool:              pool,
		currentDecoder:    nil,
		currentEncoding:   EncodingType_UNKNOWN,
	}
}

func (c *columnReaderImplBase) consumeBufferedValues(numValues int64) {
	c.numDecodedValues += numValues
}

// Read up to batchSize values from the current data page into the
// pre-allocated memory: out
//
// returns: the number of values read into the out buffer
func (c *columnReaderImplBase) ReadValues(batchSize int64, out interface{}) (int, error) {
	return c.currentDecoder.Decode(out, int(batchSize))
}

// Read up to batchSize values from the current data page into the
// pre-allocated memory out, leaving spaces for null entries according
// to the defLevels.
//
// returns: the number of values read into the out buffer
func (c *columnReaderImplBase) ReadValuesSpaced(batchSize int64, out interface{},
	nullCount int64, validBits []byte, validBitsOffset int64) (int, error) {

	return c.currentDecoder.DecodeSpaced(out, int(batchSize), int(nullCount),
		validBits, validBitsOffset)
}

// Read multiple definition levels into preallocated memory
//
// Returns the number of decoded definition levels
func (c *columnReaderImplBase) ReadDefinitionLevels(
	batchSize int64, levels []int16) int64 {

	if c.maxDefLevel == 0 {
		return 0
	}
	return int64(c.definitionLevelDecoder.Decode(int(batchSize), levels))
}

func (c *columnReaderImplBase) HasNextInternal() (bool, error) {
	// Either there is no data page available yet, or the data page has been
	// exhausted
	if c.numBufferedValues == 0 || c.numDecodedValues == c.numBufferedValues {
		ok, err := c.ReadNewPage()
		if err != nil {
			return false, err
		}
		if !ok || c.numBufferedValues == 0 {
			return false, nil
		}
	}
	return true, nil
}

// Read multiple repetition levels into preallocated memory
// Returns the number of decoded repetition levels
func (c *columnReaderImplBase) ReadRepetitionLevels(batchSize int64, levels []int16) int64 {
	if c.maxRepLevel == 0 {
		return 0
	}
	return int64(c.repetitionLevelDecoder.Decode(int(batchSize), levels))
}

// Advance to the next data page
func (c *columnReaderImplBase) ReadNewPage() (bool, error) {
	var err error
	// Loop until we find the next data page.
	for {
		c.currentPage, err = c.pager.NextPage()
		if err != nil {
			return false, err
		}
		if c.currentPage == nil {
			// EOS
			return false, nil
		}

		if c.currentPage.Type() == PageType_DICTIONARY_PAGE {
			err := c.ConfigureDictionary(*c.currentPage.(*DictionaryPage))
			if err != nil {
				return false, err
			}
			continue
		} else if c.currentPage.Type() == PageType_DATA_PAGE {
			page := c.currentPage.(*DataPageV1)
			levelsByteSize, err := c.InitializeLevelDecoders(
				*page.DataPage, page.repetitionLevelEncoding, page.definitionLevelEncoding,
			)
			if err != nil {
				return false, err
			}
			if err := c.InitializeDataDecoder(
				*page.DataPage, levelsByteSize); err != nil {
				return false, err
			}
			return true, nil
		} else {
			// We don't know what this page type is. We're allowed to skip non-data
			// pages.
			continue
		}
	}
	return true, nil
}

func (c *columnReaderImplBase) ConfigureDictionary(page DictionaryPage) error {
	encoding := int(page.Encoding())
	if page.Encoding() == EncodingType_PLAIN_DICTIONARY ||
		page.Encoding() == EncodingType_PLAIN {
		encoding = EncodingType_RLE_DICTIONARY
	}

	if _, ok := c.decoders[encoding]; ok {
		return fmt.Errorf(
			"Column cannot have more than one dictionary: %w",
			ParquetException)
	}

	if page.Encoding() == EncodingType_PLAIN_DICTIONARY ||
		page.Encoding() == EncodingType_PLAIN {
		dictionary, err := NewTypedDecoder(EncodingType_PLAIN, c.descr)
		if err != nil {
			return err
		}
		dictionary.SetData(int(page.NumValues()), page.Data(), page.Size())

		// The dictionary is fully decoded during DictionaryDecoder::Init, so the
		// DictionaryPage buffer is no longer required after this step
		//
		// TODO(nickpoorman): investigate whether this all-or-nothing decoding of the
		// dictionary makes sense and whether performance can be improved

		decoder, err := NewDictDecoder(c.descr, c.pool)
		if err != nil {
			return err
		}
		if err := decoder.SetDict(dictionary); err != nil {
			return err
		}
		c.decoders[encoding] = decoder
	} else {
		return fmt.Errorf(
			"only plain dictionary encoding has been implemented: %w",
			ParquetNYIException,
		)
	}

	c.newDictionary = true
	c.currentDecoder = c.decoders[encoding]
	debug.Assert(c.currentDecoder != nil, "currentDecoder must not be nil")

	return nil
}

// Initialize repetition and definition level decoders on the next data page.
//
// If the data page includes repetition and definition levels, we
// initialize the level decoders and return the number of encoded level bytes.
// The return value helps determine the number of bytes in the encoded data.
func (c *columnReaderImplBase) InitializeLevelDecoders(page DataPage,
	repetitionLevelEncoding EncodingType,
	definitionLevelEncoding EncodingType) (int64, error) {

	// Read a data page.
	c.numBufferedValues = int64(page.NumValues())

	// Have not decoded any values from the data page yet
	c.numDecodedValues = 0

	buffer := page.Data()
	var levelsByteSize int64
	maxSize := int32(page.Size())

	// Data page Layout: Repetition Levels - Definition Levels - encoded values.
	// Levels are encoded as rle or bit-packed.
	// Init repetition levels
	if c.maxRepLevel > 0 {
		repLevelsBytes, err := c.repetitionLevelDecoder.SetData(
			repetitionLevelEncoding, c.maxRepLevel,
			int(c.numBufferedValues), buffer, maxSize,
		)
		if err != nil {
			return 0, err
		}
		buffer = buffer[repLevelsBytes:]
		levelsByteSize += int64(repLevelsBytes)
		maxSize -= int32(repLevelsBytes)
	}
	// TODO figure a way to set max_def_level_ to 0
	// if the initial value is invalid

	// Init definition levels
	if c.maxDefLevel > 0 {
		defLevelsBytes, err := c.definitionLevelDecoder.SetData(
			definitionLevelEncoding, c.maxDefLevel,
			int(c.numBufferedValues), buffer, maxSize,
		)
		if err != nil {
			return 0, err
		}
		levelsByteSize += int64(defLevelsBytes)
		maxSize -= int32(defLevelsBytes)
	}

	return levelsByteSize, nil
}

func (c *columnReaderImplBase) InitializeLevelDecodersV2(
	page DataPageV2) (int64, error) {

	// Read a data page.
	c.numBufferedValues = int64(page.NumValues())

	// Have not decoded any values from the data page yet
	c.numDecodedValues = 0
	buffer := page.Data()

	totalLevelsLength := int64(page.RepetitionLevelsByteLength() +
		page.DefinitionLevelsByteLength())

	if totalLevelsLength > int64(page.Size()) {
		return 0, fmt.Errorf(
			"Data page too small for levels (corrupt header?): %w",
			ParquetException,
		)
	}

	if c.maxRepLevel > 0 {
		c.repetitionLevelDecoder.SetDataV2(
			int(page.RepetitionLevelsByteLength()),
			c.maxRepLevel,
			int(c.numBufferedValues),
			buffer,
		)
		buffer = buffer[page.RepetitionLevelsByteLength():]
	}

	if c.maxDefLevel > 0 {
		c.definitionLevelDecoder.SetDataV2(
			int(page.DefinitionLevelsByteLength()),
			c.maxDefLevel,
			int(c.numBufferedValues),
			buffer,
		)
	}

	return totalLevelsLength, nil
}

// Get a decoder object for this page or create a new decoder if this is the
// first page with this encoding.
func (c *columnReaderImplBase) InitializeDataDecoder(
	page DataPage, levelsByteSize int64) error {

	buffer := page.Data()[levelsByteSize:]
	dataSize := int64(page.Size()) - levelsByteSize

	if dataSize < 0 {
		return fmt.Errorf(
			"Page smaller than size of encoded levels: %w",
			ParquetException,
		)
	}

	encoding := page.Encoding()

	if isDictionaryIndexEncoding(encoding) {
		encoding = EncodingType_RLE_DICTIONARY
	}

	dec, ok := c.decoders[int(encoding)]
	if ok {
		debug.Assert(dec != nil, "decoder must not be nil")
		if encoding == EncodingType_RLE_DICTIONARY {
			debug.Assert(
				c.currentDecoder.Encoding() == EncodingType_RLE_DICTIONARY,
				"encoding must be EncodingType_RLE_DICTIONARY",
			)
		}
		c.currentDecoder = dec
	} else {
		switch encoding {
		case EncodingType_PLAIN:
			decoder, err := NewTypedDecoder(EncodingType_PLAIN, c.descr)
			if err != nil {
				return err
			}
			c.currentDecoder = decoder
			c.decoders[int(encoding)] = decoder
		case EncodingType_BYTE_STREAM_SPLIT:
			decoder, err := NewTypedDecoder(EncodingType_BYTE_STREAM_SPLIT, c.descr)
			if err != nil {
				return err
			}
			c.currentDecoder = decoder
			c.decoders[int(encoding)] = decoder
		case EncodingType_RLE_DICTIONARY:
			return fmt.Errorf("Dictionary page must be before data page: %w", ParquetException)
		case EncodingType_DELTA_BINARY_PACKED,
			EncodingType_DELTA_LENGTH_BYTE_ARRAY, EncodingType_DELTA_BYTE_ARRAY:
			return fmt.Errorf(
				"Unsupported encoding: %w",
				ParquetNYIException,
			)
		default:
			return fmt.Errorf(
				"Unknown encoding type (%v): %w",
				encoding,
				ParquetException,
			)
		}
	}
	c.currentEncoding = encoding
	c.currentDecoder.SetData(int(c.numBufferedValues), buffer, int(dataSize))

	return nil
}

type typedColumnReaderImpl struct {
	columnReaderImplBase
	dtype PhysicalType
}

func newTypedColumnReaderImpl(dtype PhysicalType,
	descr *ColumnDescriptor, pager *PageReader,
	pool memory.Allocator) *typedColumnReaderImpl {
	return &typedColumnReaderImpl{
		columnReaderImplBase: *newColumnReaderImplBase(descr, pool),
		dtype:                dtype,
	}
}

func (t *typedColumnReaderImpl) HasNext() (bool, error) {
	return t.HasNextInternal()
}

func (t *typedColumnReaderImpl) ReadBatch(batchSize int64, defLevels []int16,
	repLevels []int16, values interface{}, valuesRead *int64) (int64, error) {

	// HasNext invokes ReadNewPage
	hasNext, err := t.HasNext()
	if err != nil {
		return 0, err
	}
	if !hasNext {
		*valuesRead = 0
		return 0, nil
	}

	// TODO(nickpoorman): keep reading data pages until batch_size is reached, or the
	// row group is finished
	batchSize = u.MinInt64(batchSize, t.numBufferedValues-t.numDecodedValues)

	var numDefLevels int64
	var numRepLevels int64

	var valuesToRead int64

	// If the field is required and non-repeated, there are no definition levels
	if t.maxDefLevel > 0 && len(defLevels) != 0 {
		numDefLevels = t.ReadDefinitionLevels(batchSize, defLevels)
		// TODO(nickpoorman): this tallying of values-to-decode can be performed with better
		// cache-efficiency if fused with the level decoding.
		for i := int64(0); i < numDefLevels; i++ {
			if defLevels[i] == t.maxDefLevel {
				valuesToRead++
			}
		}
	} else {
		// Required field, read all values
		valuesToRead = batchSize
	}

	// Not present for non-repeated fields
	if t.maxRepLevel > 0 && len(repLevels) != 0 {
		numRepLevels = t.ReadRepetitionLevels(batchSize, repLevels)
		if len(defLevels) != 0 && numDefLevels != numRepLevels {
			return 0, fmt.Errorf(
				"Number of decoded rep / def levels did not match: %w",
				ParquetException,
			)
		}
	}

	vr, err := t.ReadValues(valuesToRead, values)
	if err != nil {
		return 0, err
	}
	*valuesRead = int64(vr)
	totalValues := u.MaxInt64(int64(numDefLevels), *valuesRead)
	t.consumeBufferedValues(totalValues)

	return totalValues, nil
}

func (t *typedColumnReaderImpl) ReadBatchSpaced(batchSize int64, defLevels []int16,
	repLevels []int16, values interface{}, validBits []byte, validBitsOffset int64,
	levelsRead *int64, valuesRead *int64, nullCountOut *int64) (int64, error) {

	// HasNext invokes ReadNewPage
	hasNext, err := t.HasNext()
	if err != nil {
		return 0, err
	}
	if !hasNext {
		*levelsRead = 0
		*valuesRead = 0
		*nullCountOut = 0
		return 0, nil
	}

	var totalValues int64
	// TODO(nickpoorman): keep reading data pages until batch_size is reached, or the
	// row group is finished
	batchSize = u.MinInt64(batchSize, t.numBufferedValues-t.numDecodedValues)

	// If the field is required and non-repeated, there are no definition levels
	if t.maxDefLevel > 0 {
		numDefLevels := t.ReadDefinitionLevels(batchSize, defLevels)

		// Not present for non-repeated fields
		if t.maxRepLevel > 0 {
			numRepLevels := t.ReadRepetitionLevels(batchSize, repLevels)
			if numDefLevels != numRepLevels {
				return 0, fmt.Errorf(
					"Number of decoded rep / def levels did not match: %w",
					ParquetException,
				)
			}
		}

		hasSpacedValues := HasSpacedValues(t.descr)

		var nullCount int64
		if !hasSpacedValues {
			var valuesToRead int64
			for i := int64(0); i < numDefLevels; i++ {
				if defLevels[i] == t.maxDefLevel {
					valuesToRead++
				}
			}
			tv, err := t.ReadValues(valuesToRead, values)
			if err != nil {
				return 0, err
			}
			totalValues = int64(tv)
			bitutilext.SetBitsTo(validBits, validBitsOffset,
				/*length=*/ int64(totalValues),
				/*bitsAreSet*/ true,
			)
			*valuesRead = int64(totalValues)
		} else {
			DefinitionLevelsToBitmap(defLevels, numDefLevels, t.maxDefLevel,
				t.maxRepLevel, valuesRead, &nullCount,
				validBits, validBitsOffset,
			)
			tv, err := t.ReadValuesSpaced(*valuesRead, values,
				nullCount, validBits, validBitsOffset,
			)
			if err != nil {
				return 0, err
			}
			totalValues = int64(tv)
		}
		*levelsRead = numDefLevels
		*nullCountOut = nullCount

	} else {
		// Required field, read all values
		tv, err := t.ReadValues(batchSize, values)
		if err != nil {
			return 0, err
		}
		totalValues = int64(tv)
		bitutilext.SetBitsTo(validBits, validBitsOffset,
			/*length=*/ int64(totalValues),
			/*bitsAreSet*/ true,
		)
		*nullCountOut = 0
		*levelsRead = totalValues
	}

	t.consumeBufferedValues(*levelsRead)
	return totalValues, nil
}

func (t *typedColumnReaderImpl) Skip(numRowsToSkip int64) (int64, error) {
	rowsToSkip := numRowsToSkip
	for {
		hasNext, err := t.HasNext()
		if err != nil {
			return 0, err
		}
		if !hasNext {
			break
		}
		if !(rowsToSkip > 0) {
			break
		}
		// If the number of rows to skip is more than the number of undecoded values, skip the
		// Page.
		if rowsToSkip > (t.numBufferedValues - t.numDecodedValues) {
			rowsToSkip -= t.numBufferedValues - t.numDecodedValues
			t.numDecodedValues = t.numBufferedValues
		} else {
			// We need to read this Page
			// Jump to the right offset in the Page
			var batchSize int64 = 1024 // ReadBatch with a smaller memory footprint
			var valuesRead int64

			// This will be enough scratch space to accommodate 16-bit levels or any
			// value type
			scratch := AllocateBuffer(t.pool, int(batchSize)*t.dtype.typeTraits.valueByteSize)

			for {
				batchSize = u.MinInt64(batchSize, rowsToSkip)
				// TODO(nickpoorman): Maybe we should be using the defined arrow types here?
				mutableData := bytearray.Int16CastFromBytes(scratch.Buf())
				values, err := t.dtype.ReinterpretCastToPrimitiveType(scratch.Buf())
				if err != nil {
					return 0, err
				}
				vr, err := t.ReadBatch(
					batchSize,
					mutableData,
					mutableData,
					values,
					&valuesRead,
				)
				if err != nil {
					return 0, err
				}
				valuesRead = vr
				rowsToSkip -= valuesRead

				if !(valuesRead > 0 && rowsToSkip > 0) {
					break
				}
			}
		}
	}
	return numRowsToSkip - rowsToSkip, nil
}

func (t *typedColumnReaderImpl) Type() Type {
	return t.descr.PhysicalType()
}

func (t *typedColumnReaderImpl) Descr() *ColumnDescriptor {
	return t.descr
}

// ----------------------------------------------------------------------
// Dynamic column reader constructor

func NewColumnReader(descr *ColumnDescriptor, pager *PageReader,
	pool memory.Allocator) (ColumnReader, error) {

	switch descr.PhysicalType() {
	case Type_BOOLEAN:
		return newTypedColumnReaderImpl(BooleanType, descr, pager, pool), nil
	case Type_INT32:
		return newTypedColumnReaderImpl(Int32Type, descr, pager, pool), nil
	case Type_INT64:
		return newTypedColumnReaderImpl(Int64Type, descr, pager, pool), nil
	case Type_INT96:
		return newTypedColumnReaderImpl(Int96Type, descr, pager, pool), nil
	case Type_FLOAT:
		return newTypedColumnReaderImpl(FloatType, descr, pager, pool), nil
	case Type_DOUBLE:
		return newTypedColumnReaderImpl(DoubleType, descr, pager, pool), nil
	case Type_BYTE_ARRAY:
		return newTypedColumnReaderImpl(ByteArrayType, descr, pager, pool), nil
	case Type_FIXED_LEN_BYTE_ARRAY:
		return newTypedColumnReaderImpl(FLBAType, descr, pager, pool), nil
	default:
		return nil, fmt.Errorf("type reader not implemented: %w", ParquetNYIException)
	}
}

// var BoolReader = NewTypedColumnReader(BooleanType)
// var Int32Reader = NewTypedColumnReader(Int32Type)
// var Int64Reader = NewTypedColumnReader(Int64Type)
// var Int96Reader = NewTypedColumnReader(Int96Type)
// var FloatReader = NewTypedColumnReader(FloatType)
// var DoubleReader = NewTypedColumnReader(DoubleType)
// var ByteArrayReader = NewTypedColumnReader(ByteArrayType)
// var FixedLenByteArrayReader = NewTypedColumnReader(FLBAType)

type RecordReader interface {
	// Attempt to read indicated number of records from column chunk
	// return number of records read
	ReadRecords(numRecords int64) (int64, error)

	// Pre-allocate space for data. Results in better flat read performance
	Reserve(numValues int64)

	// Clear consumed values and repetition/definition levels as the
	// result of calling ReadRecords
	Reset()

	// Transfer filled values buffer to caller. A new one will be
	// allocated in subsequent ReadRecords calls
	ReleaseValues() (*memory.Buffer, error)

	// Transfer filled validity bitmap buffer to caller. A new one will
	// be allocated in subsequent ReadRecords calls
	ReleaseIsValid() (*memory.Buffer, error)

	// Return true if the record reader has more internal data yet to
	// process
	HasMoreData() bool

	// Advance record reader to the next row group
	SetPageReader(reader PageReader)

	// Print the state to a string which is returned or an error.
	DebugPrintState() (string, error)

	// Decoded definition levels
	DefLevels() []int16

	// Decoded repetition levels
	RepLevels() []int16

	// Decoded values, including nulls, if any
	Values() []byte

	// Number of values written including nulls (if any)
	ValuesWritten() int64

	// Number of definition / repetition levels (from those that have
	// been decoded) that have been consumed inside the reader.
	LevelsPosition() int64

	// Number of definition / repetition levels that have been written
	// internally in the reader
	LevelsWritten() int64

	// Number of nulls in the leaf
	NullCount() int64

	// True if the leaf values are nullable
	NullableValues() bool

	// True if reading directly as Arrow dictionary-encoded
	ReadDictionary() bool
}

type BinaryRecordReader interface {
	RecordReader
	GetBuilderChunks() []array.Interface
}

// Read records directly to dictionary-encoded Arrow form (int32
// indices). Only valid for BYTE_ARRAY columns
type DictionaryRecordReader interface {
	RecordReader
	GetResult() *array.Chunked
}

var _ PageReader = (*SerializedPageReader)(nil)
var _ TypedColumnReader = (*typedColumnReaderImpl)(nil)
var _ ColumnReader = (*typedColumnReaderImpl)(nil)
