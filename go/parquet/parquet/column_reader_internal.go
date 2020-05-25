package parquet

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

// Stateful column reader that delimits semantic records for both flat
// and nested columns
//
// \note API EXPERIMENTAL
// \since 1.3.0

type RecordReader struct {
	nullableValues bool

	atRecordStart bool
	recordsRead   int64

	valuesWritten  int64
	valuesCapacity int64
	nullCount      int64

	levelsWritten  int64
	levelsPosition int64
	levelsCapacity int64

	values *memory.Buffer
	// In the case of false, don't allocate the values buffer (when we directly read into
	// builder classes).
	usesValues bool

	validBits *memory.Buffer
	defLevels *memory.Buffer
	repLevels *memory.Buffer

	readDictionary bool
}

func NewRecordReader(
	descr ColumnDescriptor,
	pool memory.Allocator,
	readDictionary bool, /* default: false */
) *RecordReader {
	return &RecordReader{}
}

// Attempt to read indicated number of records from column chunk
// return number of records read
func (r *RecordReader) ReadRecords(numRecords int64) int64 {}

// Pre-allocate space for data. Results in better flat read performance
func (r *RecordReader) Reserve(numValues int64) int64 {}

// Clear consumed values and repetition/definition levels as the
// result of calling ReadRecords
func (r *RecordReader) Reset() {}

// Transfer filled values buffer to caller. A new one will be
// allocated in subsequent ReadRecords calls
func (r *RecordReader) ReleaseValues() *memory.Buffer {}

// Transfer filled validity bitmap buffer to caller. A new one will
// be allocated in subsequent ReadRecords calls
func (r *RecordReader) ReleaseIsValid() *memory.Buffer {}

// Return true if the record reader has more internal data yet to
// process
func (r *RecordReader) HasMoreData() bool {}

// Advance record reader to the next row group
// reader obtained from RowGroupReader::GetColumnPageReader
func (r *RecordReader) SetPageReader(reader *PageReader) {}

func (r *RecordReader) DebugPrintState() {}

// Decoded definition levels
func (r *RecordReader) DefLevels() []int16 {
	return arrow.Int16Traits.CastFromBytes(r.defLevels.Bytes())
}

// Decoded repetition levels
func (r *RecordReader) RepLevels() []int16 {
	return arrow.Int16Traits.CastFromBytes(r.repLevels.Bytes())
}

// Decoded values, including nulls, if any
func (r *RecordReader) Values() []byte {
	return r.values.Bytes()
}

// Number of values written including nulls (if any)
func (r *RecordReader) ValuesWritten() int64 {
	return r.valuesWritten
}

// Number of definition / repetition levels (from those that have
// been decoded) that have been consumed inside the reader.
func (r *RecordReader) LevelsPosition() int64 {
	return r.levelsPosition
}

// Number of definition / repetition levels that have been written
// internally in the reader
func (r *RecordReader) LevelsWritten() int64 {
	return r.levelsWritten
}

// Number of nulls in the leaf
func (r *RecordReader) NullCount() int64 {
	return r.nullCount
}

// True if the leaf values are nullable
func (r *RecordReader) NullableValues() bool {
	return r.nullableValues
}

// True if reading directly as Arrow dictionary-encoded
func (r *RecordReader) ReadDictionary() bool {
	return r.readDictionary
}

type BinaryRecordReader struct {
	RecordReader
}

func NewBinaryRecordReader() *BinaryRecordReader {
	return &BinaryRecordReader{}
}

func (b *BinaryRecordReader) GetBuilderChunks() []array.Interface {}

// Read records directly to dictionary-encoded Arrow form (int32
// indices). Only valid for BYTE_ARRAY columns
type DictionaryRecordReader struct {
	RecordReader
}

func NewDictionaryRecordReader() *DictionaryRecordReader {
	return &DictionaryRecordReader{}
}

func (b *DictionaryRecordReader) GetResult() []array.Chunked {}

func DefinitionLevelsToBitmap(
	defLevels []int16, numDefLevels int64,
	maxDefinitionLevel int16, maxRepetitionLevel int16,
	valuesRead *int64,
	nullCount *int64,
	validBits []byte,
	validBitsOffset int64,
) error {
	// We assume here that valid_bits is large enough to accommodate the
	// additional definition levels and the ones that have already been written
	validBitsWriter := bitutil.NewBitmapWriter(
		validBits, int(validBitsOffset), int(numDefLevels))

	// TODO(itaiin): As an interim solution we are splitting the code path here
	// between repeated+flat column reads, and non-repeated+nested reads.
	// Those paths need to be merged in the future
	for i := 0; i < int(numDefLevels); i++ {
		if defLevels[i] == maxDefinitionLevel {
			validBitsWriter.Set()
		} else if maxRepetitionLevel > 0 {
			// repetition+flat case
			if defLevels[i] == (maxDefinitionLevel - 1) {
				validBitsWriter.Clear()
				*nullCount += 1
			} else {
				continue
			}
		} else {
			// non-repeated+nested case
			if defLevels[i] < maxDefinitionLevel {
				validBitsWriter.Clear()
				*nullCount += 1
			} else {
				fmt.Errorf(
					"definition level exceeds maximum: %w",
					ParquetException,
				)
			}
		}

		validBitsWriter.Next()
	}
	validBitsWriter.Finish()
	*valuesRead = int64(validBitsWriter.Position())
	return nil
}

// TODO(itaiin): another code path split to merge when the general case is done
func HasSpacedValues(descr *ColumnDescriptor) bool {
	if descr.MaxRepetitionLevel > 0 {
		// repeated+flat case
		return descr.schemaNode().isRequired()
	} else {
		// non-repeated+nested case
		// Find if a node forces nulls in the lowest level along the hierarchy
		node := descr.schemaNode()
		for node != nil {
			if node.isOptional() {
				return true
			}
			node = node.Parent()
		}
		return false
	}
}
