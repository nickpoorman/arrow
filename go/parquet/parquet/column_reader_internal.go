package parquet

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	u "github.com/nickpoorman/arrow-parquet-go/internal/util"
	arrowext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

// The minimum number of repetition/definition levels to decode at a time, for
// better vectorized performance when doing many smaller record reads
const kMinLevelBatchSize int64 = 1024

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

// TODO: Remove...
// type BinaryRecordReader struct {
// 	RecordReader
// }

// func NewBinaryRecordReader() *BinaryRecordReader {
// 	return &BinaryRecordReader{}
// }

// func (b *BinaryRecordReader) GetBuilderChunks() []array.Interface {}

// Read records directly to dictionary-encoded Arrow form (int32
// indices). Only valid for BYTE_ARRAY columns
type DictionaryRecordReader struct {
	RecordReader
}

func NewDictionaryRecordReader() *DictionaryRecordReader {
	return &DictionaryRecordReader{}
}

func (b *DictionaryRecordReader) GetResult() []array.Chunked {}

// TODO(nickpoorman): another code path split to merge when the general case is done
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

type TypedRecordReader struct {
	dtype PhysicalType
	columnReaderImplBase
	RecordReader
}

func NewTypedRecordReader(dtype PhysicalType,
	descr *ColumnDescriptor, pool memory.Allocator) *TypedRecordReader {

	tr := &TypedRecordReader{
		columnReaderImplBase: *newColumnReaderImplBase(dtype, descr, pool),
		RecordReader:         *NewRecordReader(*descr, pool, false),
	}

	tr.nullableValues = HasSpacedValues(descr)
	tr.atRecordStart = true
	tr.usesValues = !(descr.PhysicalType() == Type_BYTE_ARRAY)

	if tr.usesValues {
		tr.values = AllocateBuffer(pool, 0)
	}
	tr.validBits = AllocateBuffer(pool, 0)
	tr.defLevels = AllocateBuffer(pool, 0)
	tr.repLevels = AllocateBuffer(pool, 0)
	tr.Reset()

	return tr
}

func (t *TypedRecordReader) AvailableValuesCurrentPage() int64 {
	return t.numBufferedValues - t.numDecodedValues
}

// Compute the values capacity in bytes for the given number of elements
func (t *TypedRecordReader) BytesForValues(nitems int64) (int64, error) {
	typeSize := int64(GetTypeByteSize(t.descr.PhysicalType()))
	if arrowext.HasMultiplyOverflowInt64(nitems, typeSize) {
		return 0, fmt.Errorf(
			"Total size of items too large: %w",
			ParquetException,
		)
	}
	return nitems * typeSize, nil
}

func (t *TypedRecordReader) ReadRecords(numRecords int64) (int64, error) {
	// Delimit records, then read values at the end
	var recordsRead int64

	if t.levelsPosition < t.levelsWritten {
		recordsRead += t.ReadRecordData(numRecords)
	}

	levelBatchSize := u.MaxInt64(kMinLevelBatchSize, numRecords)

	// If we are in the middle of a record, we continue until reaching the
	// desired number of records or the end of the current record if we've found
	// enough records
	for !t.atRecordStart || recordsRead < numRecords {
		// Is there more data to read in this row group?
		hasNextInternal, err := t.HasNextInternal()
		if err != nil {
			return 0, err
		}
		if !hasNextInternal {
			if !t.atRecordStart {
				// We ended the row group while inside a record that we haven't seen
				// the end of yet. So increment the record count for the last record in
				// the row group
				recordsRead++
				t.atRecordStart = true
			}
			break
		}

		// We perform multiple batch reads until we either exhaust the row group
		// or observe the desired number of records
		batchSize := u.MinInt64(levelBatchSize, t.AvailableValuesCurrentPage())

		// No more data in column
		if batchSize == 0 {
			break
		}

		if t.maxDefLevel > 0 {
			t.ReserveLevels(batchSize)

			defLevels := t.DefLevels()[t.levelsWritten:]
			repLevels := t.RepLevels()[t.levelsWritten:]

			// Not present for non-repeated fields
			var levelsRead int64
			if t.maxRepLevel > 0 {
				levelsRead = t.ReadDefinitionLevels(batchSize, defLevels)
				if t.ReadRepetitionLevels(batchSize, repLevels) != levelsRead {
					return 0, fmt.Errorf(
						"Number of decoded rep / def levels did not match: %w",
						ParquetException,
					)
				}
			} else if t.maxDefLevel > 0 {
				levelsRead = t.ReadDefinitionLevels(batchSize, defLevels)
			}

			// Exhausted column chunk
			if levelsRead == 0 {
				break
			}

			t.levelsWritten += levelsRead
			recordsRead += t.ReadRecordData(numRecords - recordsRead)
		} else {
			// No repetition or definition levels
			batchSize := u.MinInt64(numRecords-recordsRead, batchSize)
			recordsRead += t.ReadRecordData(batchSize)
		}
	}

	return recordsRead, nil
}

// We may outwardly have the appearance of having exhausted a column chunk
// when in fact we are in the middle of processing the last batch
func (t *TypedRecordReader) hasNoValuesToProcess() bool {
	return t.levelsPosition < t.levelsWritten
}

func (t *TypedRecordReader) ReleaseValues() (*memory.Buffer, error) {
	if t.usesValues {
		b, err := t.BytesForValues(t.valuesWritten)
		if err != nil {
			return nil, err
		}
		t.values.Resize(int(b))
		t.values = AllocateBuffer(t.pool, 0)
		return t.values, nil
	} else {
		return nil, nil
	}
}

func (t *TypedRecordReader) ReleaseIsValid() (*memory.Buffer, error) {
	if t.nullableValues {
		t.validBits.Resize(int(bitutil.BytesForBits(t.valuesWritten)))
		t.validBits = AllocateBuffer(t.pool, 0)
		return t.validBits, nil
	} else {
		return nil, nil
	}
}

// Process written repetition/definition levels to reach the end of
// records. Process no more levels than necessary to delimit the indicated
// number of logical records. Updates internal state of RecordReader
//
// \return Number of records delimited
func (t *TypedRecordReader) DelimitRecords(
	numRecords int64, valuesSeen *int64) int64 {

	var valuesToRead int64
	var recordsRead int64

	defLevels := t.DefLevels()[t.levelsPosition:]
	repLevels := t.RepLevels()[t.levelsPosition:]

	debug.AssertGT(t.maxRepLevel, 0)

	// Count logical records and number of values to read
	for t.levelsPosition < t.levelsWritten {
		rl := repLevels[0]
		repLevels = repLevels[1:]
		if rl == 0 {
			// If at_record_start_ is true, we are seeing the start of a record
			// for the second time, such as after repeated calls to
			// DelimitRecords. In this case we must continue until we find
			// another record start or exhausting the ColumnChunk
			if !t.atRecordStart {
				// We've reached the end of a record; increment the record count.
				t.recordsRead++
				if recordsRead == numRecords {
					// We've found the number of records we were looking for. Set
					// at_record_start_ to true and break
					t.atRecordStart = true
					break
				}
			}
		}

		// We have decided to consume the level at this position; therefore we
		// must advance until we find another record boundary
		t.atRecordStart = false

		dl := defLevels[0]
		defLevels = defLevels[1:]
		if dl == t.maxDefLevel {
			valuesToRead++
		}
		t.levelsPosition++
	}
	*valuesSeen = valuesToRead
	return recordsRead
}

func (t *TypedRecordReader) Reserve(capacity int64) {
	t.ReserveLevels(capacity)
	t.ReserveValues(capacity)
}

func (t *TypedRecordReader) UpdateCapacity(
	capacity int64, size int64, extraSize int64) (int64, error) {

	if extraSize < 0 {
		return 0, fmt.Errorf(
			"Negative size (corrupt file?): %w",
			ParquetException,
		)
	}
	if arrowext.HasAdditionOverflow(size, extraSize) {
		return 0, fmt.Errorf(
			"Allocation size too large (corrupt file?): %w",
			ParquetException,
		)
	}
	targetSize := size + extraSize
	if targetSize >= int64(1)<<62 {
		return 0, fmt.Errorf(
			"Allocation size too large (corrupt file?): %w",
			ParquetException,
		)
	}
	if capacity >= targetSize {
		return capacity, nil
	}
	return int64(bitutil.NextPowerOf2(int(targetSize))), nil
}

func (t *TypedRecordReader) ReserveLevels(extraLevels int64) error {
	if t.maxDefLevel > 0 {
		newLevelsCapacity, err := t.UpdateCapacity(
			t.levelsCapacity, t.levelsWritten, extraLevels)
		if err != nil {
			return err
		}
		if newLevelsCapacity > t.levelsCapacity {
			const kItemSize = int64(unsafe.Sizeof(int64(0)))
			if arrowext.HasMultiplyOverflowInt64(
				newLevelsCapacity, kItemSize,
			) {
				return fmt.Errorf(
					"Allocation size too large (corrupt file?): %w",
					ParquetException,
				)
			}
			t.defLevels.ResizeNoShrink(int(newLevelsCapacity * kItemSize))
			if t.maxRepLevel > 0 {
				t.repLevels.ResizeNoShrink(int(newLevelsCapacity + kItemSize))
			}
			t.levelsCapacity = newLevelsCapacity
		}
	}
	return nil
}

func (t *TypedRecordReader) ReserveValues(extraValues int64) error {
	newValuesCapacity, err := t.UpdateCapacity(
		t.valuesCapacity, t.valuesWritten, extraValues,
	)
	if err != nil {
		return err
	}
	if newValuesCapacity > t.valuesCapacity {
		// XXX(nickpoorman): A hack to avoid memory allocation when reading directly
		// into builder classes
		if t.usesValues {
			b, err := t.BytesForValues(newValuesCapacity)
			if err != nil {
				return err
			}
			t.values.ResizeNoShrink(int(b))
		}
		t.valuesCapacity = newValuesCapacity
	}
	if t.nullableValues {
		validBytesNew := bitutil.BytesForBits(t.valuesCapacity)
		if int64(t.validBits.Len()) < validBytesNew {
			validBytesOld := bitutil.BytesForBits(t.valuesWritten)
			t.validBits.ResizeNoShrink(int(validBytesNew))

			memory.Set(t.validBits.Buf()[validBytesOld:validBytesNew], 0)
		}
	}
	return nil
}

func (t *TypedRecordReader) Reset() error {
	t.ResetValues()

	if t.levelsWritten > 0 {
		levelsRemaining := t.levelsWritten - t.levelsPosition
		// Shift remaining levels to beginning of buffer and trim to only the number
		// of decoded levels remaining
		defData := t.DefLevels()
		repData := t.RepLevels()

		copy(defData, defData[t.levelsPosition:t.levelsWritten])
		t.defLevels.ResizeNoShrink(int(levelsRemaining * int64(unsafe.Sizeof(int16(0)))))

		if t.maxRepLevel > 0 {
			copy(repData, repData[t.levelsPosition:t.levelsWritten])
			t.repLevels.ResizeNoShrink(int(levelsRemaining * int64(unsafe.Sizeof(int16(0)))))
		}

		t.levelsWritten -= t.levelsPosition
		t.levelsPosition = 0
		t.levelsCapacity = levelsRemaining
	}

	t.recordsRead = 0
	// Call Finish on the binary builders to reset them

	return nil
}

func (t *TypedRecordReader) SetPageReader(reader PageReader) {
	t.atRecordStart = true
	t.pager = reader
	t.ResetDecoders()
}

func (t *TypedRecordReader) HasMoreData() bool {
	return t.pager != nil
}

func (t *TypedRecordReader) ResetDecoders() {
	t.decoders = make(map[int]TypedDecoderInterface)
}

func (t *TypedRecordReader) ReadValuesSpaced(valuesWithNulls int64, nullCount int64) error {
	validBits := t.validBits.Buf()
	validBitsOffset := t.valuesWritten

	numDecoded, err := t.currentDecoder.DecodeSpaced(
		t.ValuesHead(), int(valuesWithNulls),
		int(nullCount), validBits, validBitsOffset,
	)
	if err != nil {
		return err
	}
	debug.Assert(
		int64(numDecoded) == valuesWithNulls,
		"Assert: int64(numDecoded) == valuesWithNulls")

	return nil
}

func (t *TypedRecordReader) ReadValuesDense(valuesToRead int64) error {
	numDecoded, err := t.currentDecoder.Decode(t.ValuesHead(), int(valuesToRead))
	if err != nil {
		return err
	}
	debug.Assert(int64(numDecoded) == valuesToRead, "Assert: numDecoded == valuesToRead")
	return nil
}

// Return number of logical records read
func (t *TypedRecordReader) ReadRecordData(numRecords int64) (int64, error) {
	// Conservative upper bound
	possibleNumValues := u.MaxInt64(numRecords, t.levelsWritten-t.levelsPosition)
	if err := t.ReserveValues(possibleNumValues); err != nil {
		return 0, err
	}

	startLevelsPosition := t.levelsPosition

	var valuesToRead int64
	var recordsRead int64
	if t.maxRepLevel > 0 {
		recordsRead = t.DelimitRecords(numRecords, &valuesToRead)
	} else if t.maxDefLevel > 0 {
		// No repetition levels, skip delimiting logic. Each level represents a
		// null or not null entry
		recordsRead = u.MinInt64(t.levelsWritten-t.levelsPosition, numRecords)

		// This is advanced by DelimitRecords, which we skipped
		t.levelsPosition += recordsRead
	} else {
		valuesToRead = numRecords
		recordsRead = valuesToRead
	}

	var nullCount int64
	if t.nullableValues {
		var valuesWithNulls int64
		if err := DefinitionLevelsToBitmap(
			t.DefLevels()[startLevelsPosition:], t.levelsPosition-startLevelsPosition,
			t.maxDefLevel, t.maxRepLevel, &valuesWithNulls, &nullCount,
			t.validBits.Buf(), t.valuesWritten,
		); err != nil {
			return 0, err
		}
		valuesToRead = valuesWithNulls - nullCount
		debug.AssertGE(valuesToRead, 0)
		if err := t.ReadValuesSpaced(valuesWithNulls, nullCount); err != nil {
			return 0, err
		}
	} else {
		debug.AssertGE(valuesToRead, 0)
		if err := t.ReadValuesDense(valuesToRead); err != nil {
			return 0, err
		}
	}
	if t.maxDefLevel > 0 {
		// Optional, repeated, or some mix thereof
		t.consumeBufferedValues(t.levelsPosition - startLevelsPosition)
	} else {
		// Flat, non-repeated
		t.consumeBufferedValues(valuesToRead)
	}
	// Total values, including null spaces, if any
	t.valuesWritten += valuesToRead + nullCount
	t.nullCount += nullCount

	return recordsRead, nil
}

func (t *TypedRecordReader) DebugPrintState() (string, error) {
	defLevels := t.DefLevels()
	repLevels := t.RepLevels()
	totalLevelsRead := t.levelsPosition

	vals, err := t.dtype.ReinterpretCastToPrimitiveType(t.Values())
	if err != nil {
		return "", err
	}
	rVals := reflect.ValueOf(vals)

	out := "def levels: "
	for i := int64(0); i < totalLevelsRead; i++ {
		out = fmt.Sprintf("%s%d ", out, defLevels[i])
	}
	out = fmt.Sprintf("%s\n", out)

	out = fmt.Sprintf("%srep levels: ", out)
	for i := int64(0); i < totalLevelsRead; i++ {
		fmt.Sprintf("%s%d ", out, repLevels[i])
	}
	out = fmt.Sprintf("%s\n", out)

	out = fmt.Sprintf("%values: ", out)
	for i := int64(0); i < t.ValuesWritten(); i++ {
		fmt.Sprintf("%s%v ", out, rVals.Index(int(i)))
	}
	out = fmt.Sprintf("%s\n", out)

	return out, nil
}

func (t *TypedRecordReader) ResetValues() {
	if t.valuesWritten > 0 {
		// Resize to 0, but do not shrink to fit
		if t.usesValues {
			t.values.ResizeNoShrink(0)
		}
		t.validBits.ResizeNoShrink(0)
		t.valuesWritten = 0
		t.valuesCapacity = 0
		t.nullCount = 0
	}
}

func (t *TypedRecordReader) ValuesHead() (interface{}, error) {
	vals, err := t.dtype.ReinterpretCastToPrimitiveType(t.values.Buf())
	if err != nil {
		return "", err
	}
	rVals := reflect.ValueOf(vals)
	return rVals.Slice(int(t.valuesWritten), rVals.Len()).Interface(), nil
}

type FLBARecordReader struct {
	TypedRecordReader
	BinaryRecordReader

	builder *array.FixedSizeBinaryBuilder
}

func NewFLBARecordReader(
	descr *ColumnDescriptor, pool memory.Allocator) *FLBARecordReader {

	rr := &FLBARecordReader{
		TypedRecordReader:  *NewTypedRecordReader(FLBAType, descr, pool),
		BinaryRecordReader: *NewBinaryRecordReader(),
	}
	debug.Assert(
		descr.PhysicalType() == Type_FIXED_LEN_BYTE_ARRAY,
		"Assert: descr.PhysicalType() == Type_FIXED_LEN_BYTE_ARRAY",
	)
	byteWidth := descr.typeLength()
	rr.builder = array.NewFixedSizeBinaryBuilder(pool,
		&arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
	return rr
}

// Override for faster implementation.
// We know we're dealing with bytes so we can skip the reflect.
func (f *FLBARecordReader) ValuesHead() []byte {
	return f.TypedRecordReader.values.Buf()[f.TypedRecordReader.valuesWritten:]
}

func (f *FLBARecordReader) GetBuilderChunks() []array.Interface {
	return []array.Interface{f.builder.NewFixedSizeBinaryArray()}
}

func (f *FLBARecordReader) ReadValuesDense(valuesToRead int64) error {
	values := f.ValuesHead()
	numDecoded, err := f.currentDecoder.Decode(values, int(valuesToRead))
	if err != nil {
		return err
	}
	debug.Assert(int64(numDecoded) == valuesToRead,
		"Assert: numDecoded == valuesToRead")

	f.builder.Append(values[:numDecoded])

	f.ResetValues()
	return nil
}

func (f *FLBARecordReader) ReadValuesSpaced(valuesToRead int64, nullCount int64) error {
	validBits := f.validBits.Buf()
	validBitsOffset := f.valuesWritten

	values := f.ValuesHead()

	numDecoded, err := f.currentDecoder.DecodeSpaced(
		values, int(valuesToRead), int(nullCount), validBits, validBitsOffset,
	)
	if err != nil {
		return err
	}
	debug.Assert(int64(numDecoded) == valuesToRead,
		"Assert: numDecoded == valuesToRead")

	for i := 0; i < numDecoded; i++ {
		if bitutilext.GetBit(validBits, uint64(validBitsOffset+int64(i))) {
			f.builder.Append(values[i : i+1])
		} else {
			f.builder.AppendNull()
		}
	}
	f.ResetValues()
	return nil
}

type ByteArrayChunkedRecordReader struct {
	// TODO: Implement...
}
