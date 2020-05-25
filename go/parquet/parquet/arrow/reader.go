package arrow

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/parquet"
)

/// Arrow read adapter class for deserializing Parquet files as Arrow row batches.
///
/// This interfaces caters for different use cases and thus provides different
/// interfaces. In its most simplistic form, we cater for a user that wants to
/// read the whole Parquet at once with the `FileReader::ReadTable` method.
///
/// More advanced users that also want to implement parallelism on top of each
/// single Parquet files should do this on the RowGroup level. For this, they can
/// call `FileReader::RowGroup(i)->ReadTable` to receive only the specified
/// RowGroup as a table.
///
/// In the most advanced situation, where a consumer wants to independently read
/// RowGroups in parallel and consume each column individually, they can call
/// `FileReader::RowGroup(i)->Column(j)->Read` and receive an `arrow::Column`
/// instance.
///
// TODO(wesm): nested data does not always make sense with this user
// interface unless you are only reading a single leaf node from a branch of
// a table. For example:
//
// repeated group data {
//   optional group record {
//     optional int32 val1;
//     optional byte_array val2;
//     optional bool val3;
//   }
//   optional int32 val4;
// }
//
// In the Parquet file, there are 3 leaf nodes:
//
// * data.record.val1
// * data.record.val2
// * data.record.val3
// * data.val4
//
// When materializing this data in an Arrow array, we would have:
//
// data: list<struct<
//   record: struct<
//    val1: int32,
//    val2: string (= list<uint8>),
//    val3: bool,
//   >,
//   val4: int32
// >>
//
// However, in the Parquet format, each leaf node has its own repetition and
// definition levels describing the structure of the intermediate nodes in
// this array structure. Thus, we will need to scan the leaf data for a group
// of leaf nodes part of the same type tree to create a single result Arrow
// nested array structure.
//
// This is additionally complicated "chunky" repeated fields or very large byte
// arrays
type FileReader struct {
}

// Factory function to create a FileReader from a ParquetFileReader and properties
func NewFileReader(pool memory.Allocator,
	reader *parquet.ParquetFileReader,
	properties *ArrowReaderProperties,
) *FileReader {
	return &FileReader{}
}

// Since the distribution of columns amongst a Parquet file's row groups may
// be uneven (the number of values in each column chunk can be different), we
// provide a column-oriented read interface. The ColumnReader hides the
// details of paging through the file's row groups and yielding
// fully-materialized arrow::Array instances
//
// Returns error status if the column of interest is not flat.
func (f *FileReader) GetColumn(i int, out *ColumnReader) {}

// Return arrow schema for all the columns.
func (f *FileReader) GetSchema(out *arrow.Schema) {}

// Read column as a whole into a chunked array.
//
// The indicated column index is relative to the schema
func (f *FileReader) ReadColumn(i int, out *array.Chunked) {}

// NOTE: Experimental API
// Reads a specific top level schema field into an Array
// The index i refers the index of the top level schema field, which may
// be nested or flat - e.g.
//
// 0 foo.bar
//   foo.bar.baz
//   foo.qux
// 1 foo2
// 2 foo3
//
// i=0 will read the entire foo struct, i=1 the foo2 primitive column etc
func (f *FileReader) ReadSchemaField(i int, out *array.Chunked) {}

// Return a RecordBatchReader of row groups selected from row_group_indices, the
//    ordering in row_group_indices matters.
// returns error Status if row_group_indices contains invalid index
func (f *FileReader) GetRecordBatchReader(rowGroupIndicies []int, out *arrow.RecordBatchReader) {}

// Return a RecordBatchReader of row groups selected from
//     row_group_indices, whose columns are selected by column_indices. The
//     ordering in row_group_indices and column_indices matter.
// returns error Status if either row_group_indices or column_indices
//    contains invalid index
func GetRecordBatchReaderWithColumnIndicies(
	rowGroupIndicies []int, out *arrow.RecordBatchReader, columnIndicies []int) {
}

// Read all columns into a Table
func (f *FileReader) ReadTable(out *array.Table) error {}

// Read the given columns into a Table
//
// The indicated column indices are relative to the schema
func (f *FileReader) ReadTableWithColumnIndicies(columnIndicies []int, out *array.Table) error {}

func (f *FileReader) ReadRowGroup(i int, out *array.Table) error {}
func (f *FileReader) ReadRowGroupWithColumnIndicies(i int, columnIndicies []int, out *array.Table) error {
}

func (f *FileReader) ReadRowGroups(rowGroups []int, out *array.Table) error {}
func (f *FileReader) ReadRowGroupsWithColumnIndicies(rowGroups []int, columnIndicies []int, out *array.Table) error {
}

// Scan file contents with one thread, return number of rows
func (f *FileReader) ScanContents(columns, columnBatchSize int32, numRows *int64) error {}

//  Return a reader for the RowGroup, this object must not outlive the
//   FileReader.
func (f *FileReader) RowGroup(rowGroupIndex int) *RowGroupReader {}

// The number of row groups in the file
func (f *FileReader) numRowGroups() int {}

func (f *FileReader) parquetReader() *parquet.ParquetFileReader {}

// Set whether to use multiple goroutines during reads of multiple columns.
// By default only one goroutine is used.
func (f *FileReader) setUseGoroutines(useGoroutines bool) {}

type RowGroupReader struct {
	// iterator
}

func NewRowGroupReader() *RowGroupReader {
	return &RowGroupReader{}
}

func (r *RowGroupReader) Column(columnIndex int) {}

func (r *RowGroupReader) ReadTable(out *array.Table) {}

func (r *RowGroupReader) readTableWithColumnIndicies(
	columnIndicies []int, out *array.Table) {
}

type ColumnChunkReader struct {
}

func NewColumnChunkReader() *ColumnChunkReader {
	return &ColumnChunkReader{}
}

func (r *ColumnChunkReader) Read(out *array.Chunked) error {}

// At this point, the column reader is a stream iterator. It only knows how to
// read the next batch of values for a particular column from the file until it
// runs out.
//
// We also do not expose any internal Parquet details, such as row groups. This
// might change in the future.
type ColumnReader struct {
}

func NewColumnReader() *ColumnReader {
	return &ColumnReader{}
}

// Scan the next array of the indicated size. The actual size of the
// returned array may be less than the passed size depending how much data is
// available in the file.
//
// When all the data in the file has been exhausted, the result is set to
// nullptr.
//
// Returns Status::OK on a successful read, including if you have exhausted
// the data available in the file.
func (c *ColumnReader) NextBatch(batchSize int64, out *array.Chunked) error {}
