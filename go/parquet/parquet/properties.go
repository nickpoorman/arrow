package parquet

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	arrowio "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/io"
	"github.com/nickpoorman/arrow-parquet-go/parquet/compress"
)

// Determines use of Parquet Format version >= 2.0.0 logical types. For
// example, when writing from Arrow data structures, PARQUET_2_0 will enable
// use of INT_* and UINT_* converted types as well as nanosecond timestamps
// stored physically as INT64. Since some Parquet implementations do not
// support the logical types added in the 2.0.0 format version, if you want to
// maximize compatibility of your files you may want to use PARQUET_1_0.
//
// Note that the 2.x format version series also introduced new serialized
// data page metadata and on disk data page layout. To enable this, use
// ParquetDataPageVersion.
type ParquetVersion int

const (
	_ ParquetVersion = iota
	ParquetVersion_PARQUET_1_0
	ParquetVersion_PARQUET_2_0
)

// Controls serialization format of data pages.  parquet-format v2.0.0
// introduced a new data page metadata type DataPageV2 and serialized page
// structure (for example, encoded levels are no longer compressed). Prior to
// the completion of PARQUET-457 in 2020, this library did not implement
// DataPageV2 correctly, so if you use the V2 data page format, you may have
// forward compatibility issues (older versions of the library will be unable
// to read the files). Note that some Parquet implementations do not implement
// DataPageV2 at all.
type ParquetDataPageVersion int

const (
	_ ParquetDataPageVersion = iota
	ParquetDataPageVersion_V1
	ParquetDataPageVersion_V2
)

// Align the default buffer size to a small multiple of a page size.
const kDefaultBufferSize = 4096 * 4

type ReaderProperties struct {
	pool                     MemoryPool
	bufferSize               int
	bufferedStreamEnabled    bool
	FileDecriptionProperties *FileDecryptionProperties
}

func NewReaderProperties(pool MemoryPool) *ReaderProperties {
	if pool == nil {
		pool = memory.DefaultAllocator
	}
	return &ReaderProperties{
		pool: pool,
	}
}

func (r *ReaderProperties) GetStream(source ArrowInputFile, start int64, numBytes int64) {}

func (r *ReaderProperties) IsBufferedStreamEnabled() bool { return r.bufferedStreamEnabled }
func (r *ReaderProperties) EnableBufferedStream()         { r.bufferedStreamEnabled = true }
func (r *ReaderProperties) DisableBufferedStream()        { r.bufferedStreamEnabled = false }
func (r *ReaderProperties) BufferSize() int               { return r.bufferSize }
func (r *ReaderProperties) SetBufferSize(size int)        { r.bufferSize = size }

const kDefaultDataPageSize int64 = 1024 * 1024
const DEFAULT_IS_DICTIONARY_ENABLED = true
const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT int64 = kDefaultDataPageSize
const DEFAULT_WRITE_BATCH_SIZE int64 = 1024
const DEFAULT_MAX_ROW_GROUP_LENGTH int64 = 64 * 1024 * 1024
const DEFAULT_ARE_STATISTICS_ENABLED = true
const DEFAULT_MAX_STATISTICS_SIZE int64 = 4096
const DEFAULT_ENCODING EncodingType = EncodingType_PLAIN
const DEFAULT_CREATED_BY string = CREATED_BY_VERSION
const DEFAULT_COMPRESSION_TYPE compress.CompressionCodec = compress.CompressionCodec_UNCOMPRESSED

type ColumnProperties struct {
	encoding          EncodingType
	codec             compress.CompressionCodec
	dictionaryEnabled bool
	statisticsEnabled bool
	maxStatsSize      int
	compressionLevel  int
}

func NewColumnProperties(
	encoding EncodingType,
	codec compress.CompressionCodec,
	dictionaryEnabled bool,
	statisticsEnabled bool,
	maxStatsSize int,
) *ColumnProperties {
	return &ColumnProperties{
		encoding:          encoding,
		codec:             codec,
		dictionaryEnabled: dictionaryEnabled,
		statisticsEnabled: statisticsEnabled,
		maxStatsSize:      maxStatsSize,
		compressionLevel:  compress.UseDefaultCompressionLevel(),
	}
}

func (c *ColumnProperties) SetEncoding(encoding EncodingType) {
	c.encoding = encoding
}

func (c *ColumnProperties) SetCompression(codec compress.CompressionCodec) {
	c.codec = codec
}

func (c *ColumnProperties) SetDictionaryEnabled(dictionaryEnabled bool) {
	c.dictionaryEnabled = dictionaryEnabled
}

func (c *ColumnProperties) SetStatisticsEnabled(statisticsEnabled bool) {
	c.statisticsEnabled = statisticsEnabled
}

func (c *ColumnProperties) SetMaxStatisticsSize(maxStatsSize int) {
	c.maxStatsSize = maxStatsSize
}

func (c *ColumnProperties) SetCompressionLevel(compressionLevel int) {
	c.compressionLevel = compressionLevel
}

func (c *ColumnProperties) Encoding() EncodingType                 { return c.encoding }
func (c *ColumnProperties) Compression() compress.CompressionCodec { return c.codec }
func (c *ColumnProperties) DictionaryEnabled() bool                { return c.dictionaryEnabled }
func (c *ColumnProperties) StatisticsEnabled() bool                { return c.statisticsEnabled }
func (c *ColumnProperties) MaxStatisticsSize() int                 { return c.maxStatsSize }
func (c *ColumnProperties) CompressionLevel() int                  { return c.compressionLevel }

type WriterProperties struct {
	pool                    MemoryPool
	dictionaryPagesizeLimit int64
	writeBatchSize          int64
	maxRowGroupLength       int64
	pagesize                int64
	parquetDataPageVersion  ParquetDataPageVersion
	parquetVersion          ParquetVersion
	parquetCreatedBy        string

	fileEncryptionProperties *FileEncryptionProperties

	defaultColumnProperties *ColumnProperties
	columnProperties        map[string]*ColumnProperties
}

func NewWriterProperties(
	pool MemoryPool,
	dictionaryPagesizeLimit int64,
	writeBatchSize int64,
	maxRowGroupLength int64,
	pagesize int64,
	version ParquetVersion,
	createdBy string,
	fileEncryptionProperties *FileEncryptionProperties,
	defaultColumnProperties *ColumnProperties,
	columnProperties map[string]*ColumnProperties,
	dataPageVersion ParquetDataPageVersion,
) *WriterProperties {
	return &WriterProperties{
		pool:                     pool,
		dictionaryPagesizeLimit:  dictionaryPagesizeLimit,
		writeBatchSize:           writeBatchSize,
		maxRowGroupLength:        maxRowGroupLength,
		pagesize:                 pagesize,
		parquetDataPageVersion:   dataPageVersion,
		parquetVersion:           version,
		parquetCreatedBy:         createdBy,
		fileEncryptionProperties: fileEncryptionProperties,
		defaultColumnProperties:  defaultColumnProperties,
		columnProperties:         columnProperties,
	}
}

func (w *WriterProperties) MemoryPool() MemoryPool { return w.pool }

func (w *WriterProperties) DictionaryPagesizeLimit() int64 {
	return w.dictionaryPagesizeLimit
}

func (w *WriterProperties) WriteBatchSize() int64 {
	return w.writeBatchSize
}

func (w *WriterProperties) MaxRowGroupLength() int64 {
	return w.maxRowGroupLength
}

func (w *WriterProperties) DataPagesize() int64 {
	return w.pagesize
}

func (w *WriterProperties) DataPageVersion() ParquetDataPageVersion {
	return w.parquetDataPageVersion
}

func (w *WriterProperties) Version() ParquetVersion {
	return w.parquetVersion
}

func (w *WriterProperties) CreatedBy() string {
	return w.parquetCreatedBy
}

func (w *WriterProperties) DictionaryIndexEncoded() EncodingType {
	if w.parquetVersion == ParquetVersion_PARQUET_1_0 {
		return EncodingType_PLAIN_DICTIONARY
	} else {
		return EncodingType_RLE_DICTIONARY
	}
}

func (w *WriterProperties) DictionaryPageEncoded() EncodingType {
	if w.parquetVersion == ParquetVersion_PARQUET_1_0 {
		return EncodingType_PLAIN_DICTIONARY
	} else {
		return EncodingType_PLAIN
	}
}

func (w *WriterProperties) ColumnProperties(path ColumnPath) *ColumnProperties {
	if colProp, ok := w.columnProperties[path.ToDotString()]; ok {
		return colProp
	}
	return w.defaultColumnProperties
}

func (w *WriterProperties) Encoding(path ColumnPath) EncodingType {
	prop := w.ColumnProperties(path)
	return prop.Encoding()
}

func (w *WriterProperties) Compression(path ColumnPath) compress.CompressionCodec {
	prop := w.ColumnProperties(path)
	return prop.Compression()
}

func (w *WriterProperties) DictionaryEnabled(path ColumnPath) bool {
	prop := w.ColumnProperties(path)
	return prop.DictionaryEnabled()
}

func (w *WriterProperties) StatisticsEnabled(path ColumnPath) bool {
	prop := w.ColumnProperties(path)
	return prop.StatisticsEnabled()
}

func (w *WriterProperties) MaxStatisticsSize(path ColumnPath) int {
	prop := w.ColumnProperties(path)
	return prop.MaxStatisticsSize()
}

func (w *WriterProperties) FileEncryptionProperties() *FileEncryptionProperties {
	return w.fileEncryptionProperties
}

func (w *WriterProperties) ColumnEncryptionProperties(
	path ColumnPath) *ColumnEncryptionProperties {
	if w.fileEncryptionProperties != nil {
		return w.fileEncryptionProperties.ColumnEncryptionProperties(path)
	} else {
		return nil
	}
}

type WriterPropertiesBuilder struct {
	pool                    MemoryPool
	dictionaryPagesizeLimit int64
	writeBatchSize          int64
	maxRowGroupLength       int64
	pagesize                int64
	version                 ParquetVersion
	dataPageVersion         ParquetDataPageVersion
	createdBy               string

	fileEncryptionProperties *FileEncryptionProperties

	// Settings used for each column unless overridden in any of the maps below
	defaultColumnProperties *ColumnProperties
	encodings               map[string]EncodingType
	codecs                  map[string]compress.CompressionCodec
	codecsCompressionLevel  map[string]int
	dictionaryEnabled       map[string]bool
	statisticsEnabled       map[string]bool
}

func NewWriterPropertiesBuilder() *WriterPropertiesBuilder {
	return &WriterPropertiesBuilder{
		pool:                    memory.DefaultAllocator,
		dictionaryPagesizeLimit: DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
		writeBatchSize:          DEFAULT_WRITE_BATCH_SIZE,
		maxRowGroupLength:       DEFAULT_MAX_ROW_GROUP_LENGTH,
		pagesize:                kDefaultDataPageSize,
		version:                 ParquetVersion_PARQUET_1_0,
		dataPageVersion:         ParquetDataPageVersion_V1,
		createdBy:               DEFAULT_CREATED_BY,
	}
}

func (w *WriterPropertiesBuilder) MemoryPool(
	pool MemoryPool) *WriterPropertiesBuilder {
	w.pool = pool
	return w
}

func (w *WriterPropertiesBuilder) EnableDictionary() *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetDictionaryEnabled(true)
	return w
}

func (w *WriterPropertiesBuilder) DisableDictionary() *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetDictionaryEnabled(false)
	return w
}

func (w *WriterPropertiesBuilder) EnableDictionaryPathString(
	path string) *WriterPropertiesBuilder {
	w.dictionaryEnabled[path] = true
	return w
}

func (w *WriterPropertiesBuilder) EnableDictionaryPath(
	path ColumnPath) *WriterPropertiesBuilder {
	w.EnableDictionaryPathString(path.ToDotString())
	return w
}

func (w *WriterPropertiesBuilder) DisableDictionaryPathString(
	path string) *WriterPropertiesBuilder {
	w.dictionaryEnabled[path] = false
	return w
}

func (w *WriterPropertiesBuilder) DisableDictionaryPath(
	path ColumnPath) *WriterPropertiesBuilder {
	w.DisableDictionaryPathString(path.ToDotString())
	return w
}

func (w *WriterPropertiesBuilder) DictionaryPagesizeLimit(
	dictionaryPsizeLimit int64) *WriterPropertiesBuilder {
	w.dictionaryPagesizeLimit = dictionaryPsizeLimit
	return w
}

func (w *WriterPropertiesBuilder) WriteBatchSize(
	writeBatchSize int64) *WriterPropertiesBuilder {
	w.writeBatchSize = writeBatchSize
	return w
}

func (w *WriterPropertiesBuilder) MaxRowGroupLength(
	maxRowGroupLength int64) *WriterPropertiesBuilder {
	w.maxRowGroupLength = maxRowGroupLength
	return w
}

func (w *WriterPropertiesBuilder) DataPagesize(
	pgSize int64) *WriterPropertiesBuilder {
	w.pagesize = pgSize
	return w
}

func (w *WriterPropertiesBuilder) DataPageVersion(
	dataPageVersion ParquetDataPageVersion) *WriterPropertiesBuilder {
	w.dataPageVersion = dataPageVersion
	return w
}

func (w *WriterPropertiesBuilder) Version(
	version ParquetVersion) *WriterPropertiesBuilder {
	w.version = version
	return w
}

func (w *WriterPropertiesBuilder) CreatedBy(
	createdBy string) *WriterPropertiesBuilder {
	w.createdBy = createdBy
	return w
}

/**
 * Define the encoding that is used when we don't utilise dictionary encoding.
 *
 * This either apply if dictionary encoding is disabled or if we fallback
 * as the dictionary grew too large.
 */
func (w *WriterPropertiesBuilder) Encoding(
	encodingType EncodingType) (*WriterPropertiesBuilder, error) {
	if encodingType == EncodingType_PLAIN_DICTIONARY ||
		encodingType == EncodingType_RLE_DICTIONARY {
		fmt.Errorf(
			"Can't use dictionary encoding as fallback encoding: %w",
			ParquetException,
		)
	}

	w.defaultColumnProperties.SetEncoding(encodingType)
	return w, nil
}

/**
 * Define the encoding that is used when we don't utilise dictionary encoding.
 *
 * This either apply if dictionary encoding is disabled or if we fallback
 * as the dictionary grew too large.
 */
func (w *WriterPropertiesBuilder) EncodingPathString(
	path string,
	encodingType EncodingType) (*WriterPropertiesBuilder, error) {
	if encodingType == EncodingType_PLAIN_DICTIONARY ||
		encodingType == EncodingType_RLE_DICTIONARY {
		fmt.Errorf(
			"Can't use dictionary encoding as fallback encoding: %w",
			ParquetException,
		)
	}

	w.encodings[path] = encodingType
	return w, nil
}

/**
 * Define the encoding that is used when we don't utilise dictionary encoding.
 *
 * This either apply if dictionary encoding is disabled or if we fallback
 * as the dictionary grew too large.
 */
func (w *WriterPropertiesBuilder) EncodingPath(
	path ColumnPath,
	encodingType EncodingType) (*WriterPropertiesBuilder, error) {
	return w.EncodingPathString(path.ToDotString(), encodingType)
}

func (w *WriterPropertiesBuilder) Compression(
	codec compress.CompressionCodec) *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetCompression(codec)
	return w
}

func (w *WriterPropertiesBuilder) MaxStatisticsSize(
	maxStatsSz int) *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetMaxStatisticsSize(maxStatsSz)
	return w
}

func (w *WriterPropertiesBuilder) CompressionPathString(
	path string,
	codec compress.CompressionCodec) *WriterPropertiesBuilder {
	w.codecs[path] = codec
	return w
}

func (w *WriterPropertiesBuilder) CompressionPath(
	path ColumnPath,
	codec compress.CompressionCodec) *WriterPropertiesBuilder {
	w.CompressionPathString(path.ToDotString(), codec)
	return w
}

// Specify the default compression level for the compressor in
// every column.  In case a column does not have an explicitly specified
// compression level, the default one would be used.
//
// The provided compression level is compressor specific. The user would
// have to familiarize oneself with the available levels for the selected
// compressor.  If the compressor does not allow for selecting different
// compression levels, calling this function would not have any effect.
// Parquet and Arrow do not validate the passed compression level.  If no
// level is selected by the user or if the special
// math.MinInt32 value is passed, then Arrow selects the
// compression level.
func (w *WriterPropertiesBuilder) CompressionLevel(
	compressionLevel int) *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetCompressionLevel(compressionLevel)
	return w
}

// Specify a compression level for the compressor for the column
// described by path.
//
// The provided compression level is compressor specific. The user would
// have to familiarize oneself with the available levels for the selected
// compressor.  If the compressor does not allow for selecting different
// compression levels, calling this function would not have any effect.
// Parquet and Arrow do not validate the passed compression level.  If no
// level is selected by the user or if the special
// math.MinInt32 value is passed, then Arrow selects the
// compression level.
func (w *WriterPropertiesBuilder) CompressionLevelPathString(
	path string,
	compressionLevel int) *WriterPropertiesBuilder {
	w.codecsCompressionLevel[path] = compressionLevel
	return w
}

// Specify a compression level for the compressor for the column
// described by path.
//
// The provided compression level is compressor specific. The user would
// have to familiarize oneself with the available levels for the selected
// compressor.  If the compressor does not allow for selecting different
// compression levels, calling this function would not have any effect.
// Parquet and Arrow do not validate the passed compression level.  If no
// level is selected by the user or if the special
// math.MinInt32 value is passed, then Arrow selects the
// compression level.
func (w *WriterPropertiesBuilder) CompressionLevelPath(
	path ColumnPath,
	compressionLevel int) *WriterPropertiesBuilder {
	w.CompressionLevelPathString(path.ToDotString(), compressionLevel)
	return w
}

func (w *WriterPropertiesBuilder) Encryption(
	fileEncryptionProperties *FileEncryptionProperties) *WriterPropertiesBuilder {
	w.fileEncryptionProperties = fileEncryptionProperties
	return w
}

func (w *WriterPropertiesBuilder) EnableStatistics() *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetStatisticsEnabled(true)
	return w
}

func (w *WriterPropertiesBuilder) DisableStatistics() *WriterPropertiesBuilder {
	w.defaultColumnProperties.SetStatisticsEnabled(false)
	return w
}

func (w *WriterPropertiesBuilder) EnableStatisticsPathString(
	path string) *WriterPropertiesBuilder {
	w.statisticsEnabled[path] = true
	return w
}

func (w *WriterPropertiesBuilder) EnableStatisticsPath(
	path ColumnPath) *WriterPropertiesBuilder {
	w.EnableStatisticsPathString(path.ToDotString())
	return w
}

func (w *WriterPropertiesBuilder) DisableStatisticsPathString(
	path string) *WriterPropertiesBuilder {
	w.statisticsEnabled[path] = false
	return w
}

func (w *WriterPropertiesBuilder) DisableStatisticsPath(
	path ColumnPath) *WriterPropertiesBuilder {
	w.DisableStatisticsPathString(path.ToDotString())
	return w
}

func (w *WriterPropertiesBuilder) Build() *WriterProperties {
	columnProperties := make(map[string]*ColumnProperties)
	get := func(key string) *ColumnProperties {
		if prop, ok := columnProperties[key]; ok {
			// Copy the properties so we don't end up modifying the same one
			propCopy := *w.defaultColumnProperties
			columnProperties[key] = &propCopy
			return columnProperties[key]
		} else {
			return prop
		}
	}

	for key, v := range w.encodings {
		get(key).SetEncoding(v)
	}
	for key, v := range w.codecs {
		get(key).SetCompression(v)
	}
	for key, v := range w.codecsCompressionLevel {
		get(key).SetCompressionLevel(v)
	}
	for key, v := range w.dictionaryEnabled {
		get(key).SetDictionaryEnabled(v)
	}
	for key, v := range w.statisticsEnabled {
		get(key).SetStatisticsEnabled(v)
	}

	return NewWriterProperties(
		w.pool,
		w.dictionaryPagesizeLimit,
		w.writeBatchSize,
		w.maxRowGroupLength,
		w.pagesize,
		w.version,
		w.createdBy,
		w.fileEncryptionProperties,
		w.defaultColumnProperties,
		columnProperties,
		w.dataPageVersion,
	)
}

// ----------------------------------------------------------------------
// Properties specific to Apache Arrow columnar read and write

const kArrowDefaultUseThreads = false

// Default number of rows to read when using arrow.RecordBatchReader
const kArrowDefaultBatchSize = 64 * 1024

// EXPERIMENTAL: Properties for configuring FileReader behavior.
type ArrowReaderProperties struct {
	useThreads       bool
	readDictIndicies map[int]struct{}
	batchSize        int64
	preBuffer        bool
	// asyncContext arrowio.AsyncContext
	cacheOptions *arrowio.CacheOptions
}

func NewArrowReaderProperties(useThreads bool) *ArrowReaderProperties {
	return &ArrowReaderProperties{
		useThreads:       useThreads,
		readDictIndicies: make(map[int]struct{}),
		batchSize:        kArrowDefaultBatchSize,
		preBuffer:        false,
		cacheOptions:     arrowio.NewCacheOptionsDefaults(),
	}
}

func (p *ArrowReaderProperties) SetUseThreads(useThreads bool) {
	p.useThreads = useThreads
}

func (p *ArrowReaderProperties) UseThreads() bool {
	return p.useThreads
}

func (p *ArrowReaderProperties) SetReadDictionary(columnIndex int, readDict bool) {
	if readDict {
		p.readDictIndicies[columnIndex] = struct{}{}
	} else {
		delete(p.readDictIndicies, columnIndex)
	}
}

func (p *ArrowReaderProperties) ReadDictionary(columnIndex int) bool {
	_, ok := p.readDictIndicies[columnIndex]
	return ok
}

func (p *ArrowReaderProperties) SetBatchSize(batchSize int64) {
	p.batchSize = batchSize
}

func (p *ArrowReaderProperties) BatchSize() int64 {
	return p.batchSize
}

// Enable read coalescing.
//
// When enabled, the Arrow reader will pre-buffer necessary regions
// of the file in-memory. This is intended to improve performance on
// high-latency filesystems (e.g. Amazon S3).
func (p *ArrowReaderProperties) SetPreBuffer(preBuffer bool) {
	p.preBuffer = preBuffer
}

// Set options for read coalescing. This can be used to tune the
// implementation for characteristics of different filesystems.
func (p *ArrowReaderProperties) SetCacheOptions(options *arrowio.CacheOptions) {
	p.cacheOptions = options
}
func (p *ArrowReaderProperties) CacheOptions() *arrowio.CacheOptions {
	return p.cacheOptions
}

// Set execution context for read coalescing.
// func (p *ArrowReaderProperties) SetAsyncContext(ctx arrowio.AsyncContext) {
// 	p.asyncContext = ctx
// }
// func (p *ArrowReaderProperties) AsyncContext() arrowio.AsyncContext {
// 	return p.asyncContext
// }

func DefaultArrowReaderProperties() *ArrowReaderProperties {
	return NewArrowReaderProperties(kArrowDefaultUseThreads)
}

type ArrowWriterProperties_EngineVersion int

const (
	_ ArrowWriterProperties_EngineVersion = iota
	// Supports only nested lists
	ArrowWriterProperties_EngineVersion_V1
	// Full support for all nesting combinations
	ArrowWriterProperties_EngineVersion_V2
)

type ArrowWriterProperties struct {
	writeTimestampsAsInt96     bool
	coerceTimestampsEnabled    bool
	coerceTimestampsUnit       arrow.TimeUnit
	truncatedTimestampsAllowed bool
	storeSchema                bool
	compliantNestedTypes       bool
	engineVersion              ArrowWriterProperties_EngineVersion
}

func NewArrowWriterProperties(
	writeNanosAsInt96 bool,
	coerceTimestampsEnabled bool,
	coerceTimestampsUnit arrow.TimeUnit,
	truncatedTimestampsAllowed bool,
	storeSchema bool,
	compliantNestedTypes bool,
	engineVersion ArrowWriterProperties_EngineVersion,
) *ArrowWriterProperties {
	return &ArrowWriterProperties{
		writeTimestampsAsInt96:     writeNanosAsInt96,
		coerceTimestampsEnabled:    coerceTimestampsEnabled,
		coerceTimestampsUnit:       coerceTimestampsUnit,
		truncatedTimestampsAllowed: truncatedTimestampsAllowed,
		storeSchema:                storeSchema,
		compliantNestedTypes:       compliantNestedTypes,
		engineVersion:              engineVersion,
	}
}

func (p *ArrowWriterProperties) SupportDeprecatedInt96Timestamps() bool {
	return p.writeTimestampsAsInt96
}

func (p *ArrowWriterProperties) CoerceTimestampsEnabled() bool {
	return p.coerceTimestampsEnabled
}

func (p *ArrowWriterProperties) CoerceTimestampsUnit() bool {
	return p.CoerceTimestampsUnit()
}

func (p *ArrowWriterProperties) TruncatedTimestampsAllowed() bool {
	return p.truncatedTimestampsAllowed
}

func (p *ArrowWriterProperties) StoreSchema() bool {
	return p.storeSchema
}

// Enable nested type naming according to the parquet specification.
//
// Older versions of arrow wrote out field names for nested lists based on the name
// of the field.  According to the parquet specification they should always be
// "element".
func (p *ArrowWriterProperties) CompliantNestedTypes() bool {
	return p.compliantNestedTypes
}

// The underlying engine version to use when writing Arrow data.
//
// V2 is currently the latest V1 is considered deprecated but left in
// place in case there are bugs detected in V2.
func (p *ArrowWriterProperties) EngineVersion() ArrowWriterProperties_EngineVersion {
	return p.engineVersion
}

type ArrowWriterPropertiesBuilder struct {
	writeTimestampsAsInt96 bool

	coerceTimestampsEnabled    bool
	coerceTimestampsUnit       arrow.TimeUnit
	truncatedTimestampsAllowed bool

	storeSchema          bool
	compliantNestedTypes bool
	engineVersion        ArrowWriterProperties_EngineVersion
}

func NewArrowWriterPropertiesBuilder() *ArrowWriterPropertiesBuilder {
	return &ArrowWriterPropertiesBuilder{
		writeTimestampsAsInt96:     false,
		coerceTimestampsEnabled:    false,
		coerceTimestampsUnit:       arrow.Second,
		truncatedTimestampsAllowed: false,
		storeSchema:                false,
		// TODO: At some point we should flip this.
		compliantNestedTypes: false,
		engineVersion:        ArrowWriterProperties_EngineVersion_V2,
	}
}

func (b *ArrowWriterPropertiesBuilder) DisableDeprecatedInt96Timestamps() *ArrowWriterPropertiesBuilder {
	b.writeTimestampsAsInt96 = false
	return b
}

func (b *ArrowWriterPropertiesBuilder) EnableDeprecatedInt96Timestamps() *ArrowWriterPropertiesBuilder {
	b.writeTimestampsAsInt96 = true
	return b
}

func (b *ArrowWriterPropertiesBuilder) CoerceTimestamps(
	unit arrow.TimeUnit) *ArrowWriterPropertiesBuilder {
	b.coerceTimestampsEnabled = true
	b.coerceTimestampsUnit = unit
	return b
}

func (b *ArrowWriterPropertiesBuilder) AllowTruncatedTimestamps() *ArrowWriterPropertiesBuilder {
	b.truncatedTimestampsAllowed = true
	return b
}

func (b *ArrowWriterPropertiesBuilder) DisallowTruncatedTimestamps() *ArrowWriterPropertiesBuilder {
	b.truncatedTimestampsAllowed = false
	return b
}

// EXPERIMENTAL: Write binary serialized Arrow schema to the file,
// to enable certain read options (like "read_dictionary") to be set
// automatically
func (b *ArrowWriterPropertiesBuilder) StoreSchema() *ArrowWriterPropertiesBuilder {
	b.storeSchema = true
	return b
}

func (b *ArrowWriterPropertiesBuilder) EnableCompliantNestedTypes() *ArrowWriterPropertiesBuilder {
	b.compliantNestedTypes = true
	return b
}

func (b *ArrowWriterPropertiesBuilder) DisableCompliantNestedTypes() *ArrowWriterPropertiesBuilder {
	b.compliantNestedTypes = false
	return b
}

func (b *ArrowWriterPropertiesBuilder) SetEngineVersion(
	version ArrowWriterProperties_EngineVersion) *ArrowWriterPropertiesBuilder {
	b.engineVersion = version
	return b
}

func (b *ArrowWriterPropertiesBuilder) Build() *ArrowWriterProperties {
	return NewArrowWriterProperties(
		b.writeTimestampsAsInt96,
		b.coerceTimestampsEnabled,
		b.coerceTimestampsUnit,
		b.truncatedTimestampsAllowed,
		b.storeSchema,
		b.compliantNestedTypes,
		b.engineVersion,
	)
}

func DefaultArrowWriterProperties() *ArrowWriterProperties {
	return NewArrowWriterPropertiesBuilder().Build()
}

// State object used for writing Arrow data directly to a Parquet
// column chunk. API possibly not stable
type ArrowWriteContext struct {
	memoryPool MemoryPool
	properties *ArrowWriterProperties

	// Buffer used for storing the data of an array converted to the physical type
	// as expected by parquet-go.
	dataBuffer *ResizableBuffer

	defLevelsBuffer *ResizableBuffer
}

func NewArrowWriteContext() *ArrowWriteContext {
	return &ArrowWriteContext{}
}
