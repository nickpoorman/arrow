package parquet

import "os"

// TODO: Will probably need to be an interface
type Contents struct {
}

func NewContents() *Contents {
	return &Contents{}
}

func (c *Contents) GetColumnPageReader(i int) *PageReader {}
func (c *Contents) Metadata() *RowGroupMetaData           {}
func (c *Contents) Properties() *ReaderProperties         {}

type RowGroupReader struct {
	// Holds a pointer to an instance of Contents implementation
	contexts *Contents
}

func NewRowGroupReader(contents *Contents) *RowGroupReader {
	return &RowGroupReader{
		contents: contents,
	}
}

// Returns the rowgroup metadata
func (r *RowGroupReader) Metadata() {}

// Construct a ColumnReader for the indicated row group-relative
// column. Ownership is shared with the RowGroupReader.
func (r *RowGroupReader) Column(i int) *ColumnReader {}

func (r *RowGroupReader) GetColumnPageReader(i int) *PageReader {}

type ParquetFileReader struct {
	// Holds a pointer to an instance of Contents implementation
	contents *Contents
}

// TODO: Figure out signature for this
func NewParquetFileReader(path string) *ParquetFileReader {
	return &ParquetFileReader{}
}

func (f *ParquetFileReader) Open(contents *Contents) {}
func (f *ParquetFileReader) Close()                  {}

// The RowGroupReader is owned by the FileReader
func (f *ParquetFileReader) RowGroup(i int) *RowGroupReader {}

// Returns the file metadata. Only one instance is ever created
func (f *ParquetFileReader) Metadata() *FileMetaData {}

// Pre-buffer the specified column indices in all row groups.
//
// Readers can optionally call this to cache the necessary slices
// of the file in-memory before deserialization. Arrow readers can
// automatically do this via an option. This is intended to
// increase performance when reading from high-latency filesystems
// (e.g. Amazon S3).
//
// After calling this, creating readers for row groups/column
// indices that were not buffered may fail. Creating multiple
// readers for the a subset of the buffered regions is
// acceptable. This may be called again to buffer a different set
// of row groups/columns.
//
// If memory usage is a concern, note that data will remain
// buffered in memory until either \a PreBuffer() is called again,
// or the reader itself is destructed. Reading - and buffering -
// only one row group at a time may be useful.
func (f *ParquetFileReader) PreBuffer(
	rowGroups []int,
	columnIndicies []int,
	// ctx arrow.io.AsyncContext,
	// options arrow.io.CacheOptions,
) {
}

// Read only Parquet file metadata
func ReadMetaData(source os.File) *FileMetaData {}

// Scan all values in file. Useful for performance testing
// columns the column numbers to scan. If empty scans all
// columnBatchSize number of values to read at a time when scanning column
// reader a ParquetFileReader instance
// return number of semantic rows in file
func ScanFileContents(columns []int, columnBatchSize int32, reader *ParquetFileReader) {}