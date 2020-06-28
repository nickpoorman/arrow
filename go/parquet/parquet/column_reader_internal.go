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

type recordReaderBase struct {
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

func newRecordReaderBase(
	descr ColumnDescriptor,
	pool memory.Allocator,
	readDictionary bool, /* default: false */
) *recordReaderBase {
	return &recordReaderBase{
		readDictionary: readDictionary,
	}
}

// Decoded definition levels
func (r *recordReaderBase) DefLevels() []int16 {
	return arrow.Int16Traits.CastFromBytes(r.defLevels.Bytes())
}

// Decoded repetition levels
func (r *recordReaderBase) RepLevels() []int16 {
	return arrow.Int16Traits.CastFromBytes(r.repLevels.Bytes())
}

// Decoded values, including nulls, if any
func (r *recordReaderBase) Values() []byte {
	return r.values.Bytes()
}

// Number of values written including nulls (if any)
func (r *recordReaderBase) ValuesWritten() int64 {
	return r.valuesWritten
}

// Number of definition / repetition levels (from those that have
// been decoded) that have been consumed inside the reader.
func (r *recordReaderBase) LevelsPosition() int64 {
	return r.levelsPosition
}

// Number of definition / repetition levels that have been written
// internally in the reader
func (r *recordReaderBase) LevelsWritten() int64 {
	return r.levelsWritten
}

// Number of nulls in the leaf
func (r *recordReaderBase) NullCount() int64 {
	return r.nullCount
}

// True if the leaf values are nullable
func (r *recordReaderBase) NullableValues() bool {
	return r.nullableValues
}

// True if reading directly as Arrow dictionary-encoded
func (r *recordReaderBase) ReadDictionary() bool {
	return r.readDictionary
}

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
	recordReaderBase
}

func NewTypedRecordReader(dtype PhysicalType,
	descr *ColumnDescriptor, pool memory.Allocator) *TypedRecordReader {

	tr := &TypedRecordReader{
		columnReaderImplBase: *newColumnReaderImplBase(descr, pool),
		recordReaderBase:     *newRecordReaderBase(*descr, pool, false),
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
		rr, err := t.ReadRecordData(numRecords)
		if err != nil {
			return recordsRead, err
		}
		recordsRead += rr
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
			rr, err := t.ReadRecordData(numRecords - recordsRead)
			if err != nil {
				return recordsRead, err
			}
			rr += recordsRead
		} else {
			// No repetition or definition levels
			batchSize := u.MinInt64(numRecords-recordsRead, batchSize)
			rr, err := t.ReadRecordData(batchSize)
			if err != nil {
				return recordsRead, err
			}
			rr += recordsRead
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

func (t *TypedRecordReader) Reset() {
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

	valuesHead, err := t.ValuesHead()
	if err != nil {
		return err
	}
	numDecoded, err := t.currentDecoder.DecodeSpaced(
		valuesHead, int(valuesWithNulls),
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
	valuesHead, err := t.ValuesHead()
	if err != nil {
		return err
	}
	numDecoded, err := t.currentDecoder.Decode(valuesHead, int(valuesToRead))
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

	builder *array.FixedSizeBinaryBuilder
}

func NewFLBARecordReader(
	descr *ColumnDescriptor, pool memory.Allocator) *FLBARecordReader {

	rr := &FLBARecordReader{
		TypedRecordReader: *NewTypedRecordReader(FLBAType, descr, pool),
		// BinaryRecordReader: *NewBinaryRecordReader(),
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

// ByteArrayChunkedRecordReader
type ByteArrayChunkedRecordReader struct {
	TypedRecordReader
	// Helper data structure for accumulating builder chunks
	accumulator *Accumulator
}

// NewByteArrayChunkedRecordReader creates a new ByteArrayChunkedRecordReader struct.
func NewByteArrayChunkedRecordReader(
	descr *ColumnDescriptor,
	pool memory.Allocator) *ByteArrayChunkedRecordReader {
	debug.Assert(descr.PhysicalType() == Type_BYTE_ARRAY,
		"Assert: descr.PhysicalType() == Type_BYTE_ARRAY")

	return &ByteArrayChunkedRecordReader{
		TypedRecordReader: *NewTypedRecordReader(ByteArrayType, descr, pool),
		accumulator: ByteArrayEncodingTraits.Accumulator(
			pool, arrow.BinaryTypes.Binary),
	}
}

// GetBuilderChunks
func (b *ByteArrayChunkedRecordReader) GetBuilderChunks() []array.Interface {
	result := b.accumulator.Chunks
	if len(result) == 0 || b.accumulator.Builder.Len() > 0 {
		lastChunk := b.accumulator.Builder.NewArray()
		result = append(result, lastChunk)
	}
	b.accumulator.Chunks = make([]array.Interface, 0)
	return result
}

// ReadValuesDense
func (b *ByteArrayChunkedRecordReader) ReadValuesDense(
	valuesToRead int64, nullCount int64) error {
	numDecoded, err := b.currentDecoder.DecodeArrowNonNull(
		int(valuesToRead), b.accumulator,
	)
	if err != nil {
		return err
	}
	debug.Assert(int64(numDecoded) == valuesToRead-nullCount,
		"Assert: int64(numDecoded) == valuesToRead-nullCount",
	)
	b.ResetValues()
	return nil
}

// ReadValuesSpaced
func (b *ByteArrayChunkedRecordReader) ReadValuesSpaced(
	valuesToRead int64, nullCount int64) error {

	numDecoded, err := b.currentDecoder.DecodeArrow(
		int(valuesToRead), int(nullCount),
		b.validBits.Buf(), int(b.valuesWritten), b.accumulator,
	)
	if err != nil {
		return err
	}
	debug.Assert(int64(numDecoded) == valuesToRead-nullCount,
		"Assert: int64(numDecoded) == valuesToRead - nullCount")
	b.ResetValues()
	return nil
}

// ByteArrayDictionaryRecordReader
type ByteArrayDictionaryRecordReader struct {
	TypedRecordReader
	// DictionaryRecordReader

	accumulator  *Accumulator
	resultChunks []array.Interface
	dtype        arrow.BinaryDataType
}

// NewByteArrayDictionaryRecordReader creates a new ByteArrayDictionaryRecordReader struct.
func NewByteArrayDictionaryRecordReader(
	descr *ColumnDescriptor,
	pool memory.Allocator) *ByteArrayDictionaryRecordReader {
	br := &ByteArrayDictionaryRecordReader{
		TypedRecordReader: *NewTypedRecordReader(ByteArrayType, descr, pool),
		// DictionaryRecordReader: NewDictionaryRecordReader(),

		// Should this maybe be  arrow.BinaryTypes.String since we are dealing
		// with a Dictionary?
		accumulator: ByteArrayEncodingTraits.Accumulator(pool, arrow.BinaryTypes.Binary),
	}
	br.readDictionary = true
	return br
}

// builder
func (b *ByteArrayDictionaryRecordReader) builder() *array.BinaryBuilder {
	return b.accumulator.Builder.(*array.BinaryBuilder)
}

// GetResult
func (b *ByteArrayDictionaryRecordReader) GetResult() *array.Chunked {
	b.FlushBuilder()
	result := b.resultChunks
	b.resultChunks = make([]array.Interface, 0)
	return array.NewChunked(arrow.BinaryTypes.Binary, result)
}

// FlushBuilder
func (b *ByteArrayDictionaryRecordReader) FlushBuilder() {
	if b.builder().Len() > 0 {
		chunk := b.builder().NewArray()
		b.resultChunks = append(b.resultChunks, chunk)
	}
}

// MaybeWriteNewDictionary
func (b *ByteArrayDictionaryRecordReader) MaybeWriteNewDictionary() {
	if b.newDictionary {
		panic(ParquetNYIException)
		// If there is a new dictionary, we may need to flush the builder, then
		// insert the new dictionary values
		// b.FlushBuilder()
		// b.builder.ResetFull()
		// decoder := b.currentDecoder.(*BinaryDictDecoder)
		// decoder.InsertDictionary(b.builder)
		// b.newDictionary = false
	}
}

// ReadValuesDense
func (b *ByteArrayDictionaryRecordReader) ReadValuesDense(valuesToRead int64) error {
	var numDecoded int64
	if b.currentEncoding == EncodingType_RLE_DICTIONARY {
		b.MaybeWriteNewDictionary()
		decoder := b.currentDecoder //.(*BinaryDictDecoder)
		n, err := decoder.DecodeIndices(int(valuesToRead), b.builder)
		if err != nil {
			return err
		}
		numDecoded = int64(n)
	} else {
		n, err := b.currentDecoder.DecodeArrowNonNull(
			int(valuesToRead), b.accumulator,
		)
		if err != nil {
			return err
		}
		numDecoded = int64(n)

		// Flush values since they have been copied into the builder
		b.ResetValues()
	}

	debug.Assert(numDecoded == valuesToRead, "Assert: numDecoded == valuesToRead")
	return nil
}

// ReadValuesSpaced
func (b *ByteArrayDictionaryRecordReader) ReadValuesSpaced(
	valuesToRead int64, nullCount int64) error {
	var numDecoded int64
	if b.currentEncoding == EncodingType_RLE_DICTIONARY {
		b.MaybeWriteNewDictionary()
		decoder := b.currentDecoder //.(*BinaryDictDecoder)
		n, err := decoder.DecodeIndicesSpaced(
			int(valuesToRead), int(nullCount),
			b.validBits.Buf(), int(b.valuesWritten), b.accumulator)
		if err != nil {
			return err
		}
		numDecoded = int64(n)
	} else {
		n, err := b.currentDecoder.DecodeArrow(
			int(valuesToRead), int(nullCount),
			b.validBits.Buf(), int(b.valuesWritten), b.accumulator,
		)
		if err != nil {
			return err
		}
		numDecoded = int64(n)

		// Flush values since they have been copied into the builder
		b.ResetValues()
	}

	debug.Assert(numDecoded == valuesToRead-nullCount,
		"Assert: numDecoded == valuesToRead - nullCount")
	return nil
}

func NewByteArrayRecordReader(
	descr *ColumnDescriptor, pool memory.Allocator, readDictionary bool) RecordReader {
	if readDictionary {
		return NewByteArrayDictionaryRecordReader(descr, pool)
	} else {
		return NewByteArrayChunkedRecordReader(descr, pool)
	}
}

var (
	_ RecordReader           = (*TypedRecordReader)(nil)
	_ BinaryRecordReader     = (*FLBARecordReader)(nil)
	_ BinaryRecordReader     = (*ByteArrayChunkedRecordReader)(nil)
	_ DictionaryRecordReader = (*ByteArrayDictionaryRecordReader)(nil)
)
