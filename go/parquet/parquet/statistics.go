package parquet

import (
	"fmt"
	"math"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// ----------------------------------------------------------------------
// Value comparator interfaces

// Base class for value comparators. Generally used with TypedComparator
type Comparator interface {
}

// Create a comparator explicitly from physical type and sort order
// physical_type the physical type for the typed comparator
// sortOrder either SortOrder_SIGNED or SortOrder_UNSIGNED
// typeLength for FIXED_LEN_BYTE_ARRAY only, default: -1
func NewComparator(physicalType Type, sortOrder SortOrder, typeLength int) Comparator {
	panic("NewComparator not implemented")
}

// Create typed comparator inferring default sort order from ColumnDescriptor
func NewComparatorFromColumnDescriptor(descr *ColumnDescriptor) Comparator {
	panic("NewComparatorFromColumnDescriptor not implemented")
}

// Interface for comparison of physical types according to the emantics of a
// particular logical type.
// TODO: This uses a generic template to generate these
type TypedComparator struct {
	dtype Type
	comp  Comparator
}

// Scalar comparison of two elements, return true if first is strictly less than
// the second
func (t *TypedComparator) Compare(a interface{}, b interface{}) bool {
	panic("Compare not implemented")
}

// Compute maximum and minimum elements in a batch of elements without any nulls
func (t *TypedComparator) GetMinMax(values interface{}, length int64) (interface{}, interface{}) {
	panic("GetMinMax not implemented")
}

// Compute minimum and maximum elements from an Arrow array. Only valid for
// certain Parquet Type / Arrow Type combinations, like BYTE_ARRAY
// arrow::BinaryArray
func (t *TypedComparator) GetMinMaxFromArrowArray(values array.Interface) (interface{}, interface{}) {
	panic("GetMinMaxFromArrowArray not implemented")
}

func (t *TypedComparator) GetMinMaxSpaced(values interface{}, length int64,
	validBits *uint8, validBitsOffset int64) (interface{}, interface{}) {
	panic("GetMinMaxSpaced not implemented")
}

// Create a comparator explicitly from physical type and sort order
// physical_type the physical type for the typed comparator
// sortOrder either SortOrder_SIGNED or SortOrder_UNSIGNED
// typeLength for FIXED_LEN_BYTE_ARRAY only, default: -1
// Typed version of Comparator::Make
func NewTypedComparator(physicalType Type, sortOrder SortOrder, typeLength int) *TypedComparator {
	c := NewComparator(physicalType, sortOrder, typeLength)
	return c.(*TypedComparator)
}

// Create typed comparator inferring default sort order from ColumnDescriptor
// Typed version of Comparator::Make
func NewTypedComparatorFromColumnDescriptor(descr *ColumnDescriptor) *TypedComparator {
	c := NewComparatorFromColumnDescriptor(descr)
	return c.(*TypedComparator)
}

// ----------------------------------------------------------------------

// Structure represented encoded statistics to be written to and from Parquet
// serialized metadata
type EncodedStatistics struct {
	max      string
	min      string
	isSigned bool

	nullCount     int64
	distinctCount int64

	hasMin           bool
	hasMax           bool
	hasNullCount     bool
	hasDistinctCount bool
}

func NewEncodedStatistics() *EncodedStatistics {
	return &EncodedStatistics{}
}

// From parquet-mr
// Don't write stats larger than the max size rather than truncating. The
// rationale is that some engines may use the minimum value in the page as
// the true minimum for aggregations and there is no way to mark that a
// value has been truncated and is a lower bound and not in the page.

func (e *EncodedStatistics) ApplyStatSizeLimits(length int) {
	if len(e.max) > length {
		e.hasMax = false
	}
	if len(e.min) > length {
		e.hasMin = false
	}
}

func (e *EncodedStatistics) isSet() bool {
	return e.hasMin || e.hasMax || e.hasNullCount || e.hasDistinctCount
}

func (e *EncodedStatistics) setMax(value string) *EncodedStatistics {
	e.max = value
	e.hasMax = true
	return e
}

func (e *EncodedStatistics) setMin(value string) *EncodedStatistics {
	e.min = value
	e.hasMin = true
	return e
}

func (e *EncodedStatistics) setNullCount(value int64) *EncodedStatistics {
	e.nullCount = value
	e.hasNullCount = true
	return e
}

func (e *EncodedStatistics) setDistinctCount(value int64) *EncodedStatistics {
	e.distinctCount = value
	e.hasDistinctCount = true
	return e
}

// TODO: Maybe this needs to be an interface?
// Base type for computing column statistics while writing a file
type Statistics interface {
}

// ----------------------------------------------------------------------
// Public factory functions

// Create a new statistics instance given a column schema
// definition
// descr the column schema
// pool a memory pool to use for any memory allocations, optional
func NewStatistics(descr *ColumnDescriptor) (Statistics, error) {
	return NewStatisticsWithPool(descr, memory.DefaultAllocator)
}

// Create a new statistics instance given a column schema
// definition
// descr the column schema
// pool a memory pool to use for any memory allocations, optional
func NewStatisticsWithPool(descr *ColumnDescriptor, pool memory.Allocator) (Statistics, error) {
	switch descr.PhysicalType() {
	// case Type_BOOLEAN:
	// 	return TypedStatisticsBoolean{descr, pool}, nil
	// case Type_INT32:
	// 	return TypedStatisticsInt32{descr, pool}, nil
	// case Type_INT96:
	// 	return TypedStatisticsInt64{descr, pool}, nil
	// case Type_FLOAT:
	// 	return TypedStatisticsFloat{descr, pool}, nil
	// case Type_DOUBLE:
	// 	return TypedStatisticsDouble{descr, pool}, nil
	// case Type_BYTE_ARRAY:
	// 	return TypedStatisticsByteArray{descr, pool}, nil
	// case Type_FIXED_LEN_BYTE_ARRAY:
	// 	return TypedStatisticsFLBA{descr, pool}, nil
	default:
		return nil, fmt.Errorf("Statistics not implemented: %w", ParquetNYIException)
	}
}

// Create a new statistics instance given a column schema definition and
// pre-existing state
// descr the column schema
// encoded_min the encoded minimum value
// encoded_max the encoded maximum value
// num_values total number of values
// null_count number of null values
// distinct_count number of distinct values
// has_min_max whether the min/max statistics are set
// pool a memory pool to use for any memory allocations, optional
func NewStatisticsWithState(descr *ColumnDescriptor, encodedMin string,
	encodedMax string, numValues int64, nullCount int64,
	distinctCount int64, hasMinMax bool) (Statistics, error) {
	panic("NewStatisticsWithState not implemented")
	// return nil, nil
}

// A typed implementation of Statistics
// TODO: This uses a generic template to generate these
type TypedStatistics interface {
}

// ----------------------------------------------------------------------
// Comparator implementations

type CompareHelper struct {
	dtype    Type
	isSigned bool
}

func (c CompareHelper) DefaultMin() interface{} {
	panic("not yet implemented")
	// return std::numeric_limits<T>::max();
}

func (c CompareHelper) DefaultMax() interface{} {
	panic("not yet implemented")
	// return std::numeric_limits<T>::lowest();
}

func (c CompareHelper) Coalesce(val interface{}, fallback interface{}) interface{} {
	switch v := val.(type) {
	case float64:
		if math.IsNaN(v) {
			return fallback
		}
		return val
	default:
		return val
	}
}

func (c CompareHelper) Compare(typeLength int, a interface{}, b interface{}) bool {
	panic("not yet implemented")
	// return a < b;
}

func (c CompareHelper) Min(typeLength int, a interface{}, b interface{}) bool {
	panic("not yet implemented")
	// return a < b ? a : b;
}

func (c CompareHelper) Max(typeLength int, a interface{}, b interface{}) bool {
	panic("not yet implemented")
	// return a < b ? b : a;
}

// type TypedStatisticsBoolean struct {
// }

// type TypedStatisticsInt32 struct {
// }

// type TypedStatisticsInt64 struct {
// }

// type TypedStatisticsFloat struct {
// }

// type TypedStatisticsDouble struct {
// }

// type TypedStatisticsByteArray struct {
// }

// type TypedStatisticsFLBA struct {
// }

type TypedStatisticsImpl struct {
	dtype      Type
	descr      *ColumnDescriptor
	hasMinMax  bool
	min        interface{}
	max        interface{}
	pool       memory.Allocator
	numValues  int64
	statistics *EncodedStatistics
	comparator *TypedComparator
	minBuffer  *memory.Buffer
	maxBuffer  *memory.Buffer
}

func NewTypedStatisticsImpl(dtype Type, descr *ColumnDescriptor, pool memory.Allocator) (*TypedStatisticsImpl, error) {
	ts := &TypedStatisticsImpl{
		dtype:     dtype,
		descr:     descr,
		pool:      pool,
		minBuffer: AllocateBuffer(pool, 0),
		maxBuffer: AllocateBuffer(pool, 0),
	}
	comp := NewComparatorFromColumnDescriptor(descr)
	ts.comparator = &TypedComparator{
		dtype: dtype,
		comp:  comp,
	}
	return ts, nil
}

func NewTypedStatisticsImplWithState(dtype Type, descr *ColumnDescriptor, encodedMin string,
	encodedMax string, numValues int64, nullCount int64,
	distinctCount int64, hasMinMax bool,
	pool memory.Allocator) (*TypedStatisticsImpl, error) {
	ts, err := NewTypedStatisticsImpl(dtype, descr, pool)
	if err != nil {
		return nil, err
	}
	ts.IncrementNumValues(numValues)
	ts.IncrementNullCount(nullCount)
	ts.IncrementDistinctCount(distinctCount)

	if encodedMin != "" {
		ts.PlainDecode(encodedMin, &ts.min)
	}
	if encodedMax != "" {
		ts.PlainDecode(encodedMax, &ts.max)
	}
	ts.hasMinMax = hasMinMax
	return ts, nil
}

func (ts *TypedStatisticsImpl) IncrementNullCount(n int64) {
	ts.statistics.nullCount += n
}

func (ts *TypedStatisticsImpl) IncrementNumValues(n int64) {
	ts.numValues += n
}

func (ts *TypedStatisticsImpl) IncrementDistinctCount(n int64) {
	ts.statistics.distinctCount += n
}

func (ts *TypedStatisticsImpl) PlainDecode(src string, dst *interface{}) {
	panic("PlainDecode not implemented")
	// decoder := NewTypedDecoder(ts.dtype, EncodingType_PLAIN, ts.descr)
	// decoder.SetData(1, src)
	// decoder.Decode(dst, 1)
}
