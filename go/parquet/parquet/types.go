// This is implemented based off of:
// https://github.com/apache/arrow/blob/bd08d0ecbe355b9e0de7d07e8b9ff6ccdb150e73/cpp/src/parquet/types.h
// and
// https://github.com/apache/arrow/blob/bd08d0ecbe355b9e0de7d07e8b9ff6ccdb150e73/cpp/src/parquet/types.cc
package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/parquet/compress"
	"github.com/xitongsys/parquet-go/parquet"
)

// ----------------------------------------------------------------------
// Metadata enums to match Thrift metadata
//
// This includes some boilerplate to convert between our types and Parquet's
// Thrift types.
//
// We can also add special values like NONE to distinguish between metadata
// values being set and not set. As an example consider ConvertedType and
// CompressionCodec

// Mirrors parquet::Type
type Type int

const (
	Type_BOOLEAN Type = iota
	Type_INT32
	Type_INT64
	Type_INT96
	Type_FLOAT
	Type_DOUBLE
	Type_BYTE_ARRAY
	Type_FIXED_LEN_BYTE_ARRAY
	// Should always be last element.
	Type_UNDEFINED
)

// Mirrors parquet::ConvertedType
type ConvertedType int

const (
	ConvertedType_NONE ConvertedType = iota
	ConvertedType_UTF8
	ConvertedType_MAP
	ConvertedType_MAP_KEY_VALUE
	ConvertedType_LIST
	ConvertedType_ENUM
	ConvertedType_DECIMAL
	ConvertedType_DATE
	ConvertedType_TIME_MILLIS
	ConvertedType_TIME_MICROS
	ConvertedType_TIMESTAMP_MILLIS
	ConvertedType_TIMESTAMP_MICROS
	ConvertedType_UINT_8
	ConvertedType_UINT_16
	ConvertedType_UINT_32
	ConvertedType_UINT_64
	ConvertedType_INT_8
	ConvertedType_INT_16
	ConvertedType_INT_32
	ConvertedType_INT_64
	ConvertedType_JSON
	ConvertedType_BSON
	ConvertedType_INTERVAL
	ConvertedType_NA
	// Should always be last element.
	ConvertedType_UNDEFINED
)

type RepetitionType int

const (
	RepetitionType_REQUIRED RepetitionType = iota
	RepetitionType_OPTIONAL
	RepetitionType_REPEATED
	// Should always be last element.
	RepetitionType_UNDEFINED
)

// Reference:
// parquet-mr/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java
// Sort order for page and column statistics. Types are associated with sort
// orders (e.g., UTF8 columns should use UNSIGNED) and column stats are
// aggregated using a sort order. As of parquet-format version 2.3.1, the order
// used to aggregate stats is always SIGNED and is not stored in the Parquet
// file. These stats are discarded for types that need unsigned. See
// PARQUET-686.

type SortOrder int

const (
	SortOrder_UNKNOWN SortOrder = iota
	SortOrder_SIGNED
	SortOrder_UNSIGNED
)

type DecimalMetadata struct {
	isSet     bool
	scale     int
	precision int
}

// Implementation of parquet.thrift LogicalType types.
type LogicalTypeKind int

const (
	LogicalTypeKind_UNKNOWN LogicalTypeKind = iota
	LogicalTypeKind_STRING
	LogicalTypeKind_MAP
	LogicalTypeKind_LIST
	LogicalTypeKind_ENUM
	LogicalTypeKind_DECIMAL
	LogicalTypeKind_DATE
	LogicalTypeKind_TIME
	LogicalTypeKind_TIMESTAMP
	LogicalTypeKind_INTERVAL
	LogicalTypeKind_INT
	LogicalTypeKind_NIL // Thrift NullType
	LogicalTypeKind_JSON
	LogicalTypeKind_BSON
	LogicalTypeKind_UUID
	LogicalTypeKind_NONE
)

type LogicalTypeTimeUnit int

const (
	_ LogicalTypeTimeUnit = iota
	LogicalTypeTimeUnit_UNKNOWN
	LogicalTypeTimeUnit_MILLIS
	LogicalTypeTimeUnit_MICROS
	LogicalTypeTimeUnit_NANOS
)

/*
 * The logical type implementation classes are built in four layers: (1) the base
 * layer, which establishes the interface and provides generally reusable implementations
 * for the ToJSON() and Equals() methods; (2) an intermediate derived layer for the
 * "compatibility" methods, which provides implementations for is_compatible() and
 * ToConvertedType(); (3) another intermediate layer for the "applicability" methods
 * that provides several implementations for the is_applicable() method; and (4) the
 * final derived classes, one for each logical type, which supply implementations
 * for those methods that remain virtual (usually just ToString() and ToThrift()) or
 * otherwise need to be overridden.
 */

type LogicalType interface {
	Kind() LogicalTypeKind
	SortOrder() SortOrder
	Equals(other LogicalType) bool

	isString() bool
	isMap() bool
	isList() bool
	isEnum() bool
	isDecimal() bool
	isDate() bool
	isTime() bool
	isTimestamp() bool
	isInterval() bool
	isInt() bool
	isNull() bool
	isJSON() bool
	isBSON() bool
	isUUID() bool
	isNone() bool
	isValid() bool
	isInvalid() bool
	isNested() bool
	isNonnested() bool
	isSerialized() bool

	isCompatible(convertedType ConvertedType, convertedDecimalMetadata DecimalMetadata) bool
	isApplicable(primitiveType Type, primitiveLength int32) bool

	ToConvertedType(outDecimalMetadata *DecimalMetadata) ConvertedType
	ToString() (string, error)
	ToThrift() (*parquet.LogicalType, error)
	ToJSON() (string, error)
}

func NewLogicalTypeFromConvertedType(convertedType ConvertedType,
	convertedDecimalMetadata DecimalMetadata) (LogicalType, error) {
	switch convertedType {
	case ConvertedType_UTF8:
		return NewStringLogicalType(), nil
	case ConvertedType_MAP_KEY_VALUE, ConvertedType_MAP:
		return NewMapLogicalType(), nil
	case ConvertedType_LIST:
		return NewListLogicalType(), nil
	case ConvertedType_ENUM:
		return NewEnumLogicalType(), nil
	case ConvertedType_DECIMAL:
		return NewDecimalLogicalType(
			convertedDecimalMetadata.precision,
			convertedDecimalMetadata.scale,
		)
	case ConvertedType_DATE:
		return NewDateLogicalType(), nil
	case ConvertedType_TIME_MILLIS:
		return NewTimeLogicalType(true, LogicalTypeTimeUnit_MILLIS)
	case ConvertedType_TIME_MICROS:
		return NewTimeLogicalType(true, LogicalTypeTimeUnit_MICROS)
	case ConvertedType_TIMESTAMP_MILLIS:
		return NewTimestampLogicalType(true, LogicalTypeTimeUnit_MILLIS,
			true,  // isFromConvertedType
			false, // forceSetConvertedType
		)
	case ConvertedType_TIMESTAMP_MICROS:
		return NewTimestampLogicalType(true, LogicalTypeTimeUnit_MICROS,
			true,  // isFromConvertedType
			false, // forceSetConvertedType
		)
	case ConvertedType_INTERVAL:
		return NewIntervalLogicalType(), nil
	case ConvertedType_INT_8:
		return NewIntLogicalType(8, true)
	case ConvertedType_INT_16:
		return NewIntLogicalType(16, true)
	case ConvertedType_INT_32:
		return NewIntLogicalType(32, true)
	case ConvertedType_INT_64:
		return NewIntLogicalType(64, true)
	case ConvertedType_UINT_8:
		return NewIntLogicalType(8, false)
	case ConvertedType_UINT_16:
		return NewIntLogicalType(16, false)
	case ConvertedType_UINT_32:
		return NewIntLogicalType(32, false)
	case ConvertedType_UINT_64:
		return NewIntLogicalType(64, false)
	case ConvertedType_JSON:
		return NewJSONLogicalType(), nil
	case ConvertedType_BSON:
		return NewBSONLogicalType(), nil
	case ConvertedType_NONE:
		return NewNoLogicalType(), nil
	case ConvertedType_NA, ConvertedType_UNDEFINED:
		return NewUnknownLogicalType(), nil
	}

	return NewUnknownLogicalType(), nil
}

/*
 * Base type to embed in implemented types
 */

type logicalTypeBase struct {
	kind      LogicalTypeKind
	sortOrder SortOrder
}

func (l logicalTypeBase) Kind() LogicalTypeKind {
	return l.kind
}

func (l logicalTypeBase) SortOrder() SortOrder {
	return l.sortOrder
}

func (l logicalTypeBase) Equals(other LogicalType) bool {
	return other != nil && l.kind == other.Kind()
}

func (l logicalTypeBase) isString() bool {
	return l.kind == LogicalTypeKind_STRING
}

func (l logicalTypeBase) isMap() bool {
	return l.kind == LogicalTypeKind_MAP
}

func (l logicalTypeBase) isList() bool {
	return l.kind == LogicalTypeKind_LIST
}

func (l logicalTypeBase) isEnum() bool {
	return l.kind == LogicalTypeKind_ENUM
}

func (l logicalTypeBase) isDecimal() bool {
	return l.kind == LogicalTypeKind_DECIMAL
}

func (l logicalTypeBase) isDate() bool {
	return l.kind == LogicalTypeKind_DATE
}

func (l logicalTypeBase) isTime() bool {
	return l.kind == LogicalTypeKind_TIME
}

func (l logicalTypeBase) isTimestamp() bool {
	return l.kind == LogicalTypeKind_TIMESTAMP
}

func (l logicalTypeBase) isInterval() bool {
	return l.kind == LogicalTypeKind_INTERVAL
}

func (l logicalTypeBase) isInt() bool {
	return l.kind == LogicalTypeKind_INT
}

func (l logicalTypeBase) isNull() bool {
	return l.kind == LogicalTypeKind_NIL
}

func (l logicalTypeBase) isJSON() bool {
	return l.kind == LogicalTypeKind_JSON
}

func (l logicalTypeBase) isBSON() bool {
	return l.kind == LogicalTypeKind_BSON
}

func (l logicalTypeBase) isUUID() bool {
	return l.kind == LogicalTypeKind_UUID
}

func (l logicalTypeBase) isNone() bool {
	return l.kind == LogicalTypeKind_NONE
}

func (l logicalTypeBase) isValid() bool {
	return l.kind != LogicalTypeKind_UNKNOWN
}

func (l logicalTypeBase) isInvalid() bool {
	return !l.isValid()
}

func (l logicalTypeBase) isNested() bool {
	return l.kind == LogicalTypeKind_LIST ||
		l.kind == LogicalTypeKind_MAP
}

func (l logicalTypeBase) isNonnested() bool {
	return !l.isNested()
}

func (l logicalTypeBase) isSerialized() bool {
	return !(l.kind == LogicalTypeKind_NONE ||
		l.kind == LogicalTypeKind_UNKNOWN)
}

func setDecimalMetadata(d *DecimalMetadata, i bool, p int, s int) {
	if d != nil {
		d.isSet = i
		d.scale = s
		d.precision = p
	}
}

func resetDecimalMetadata(d *DecimalMetadata) {
	setDecimalMetadata(d, false, -1, -1)
}

/*
 * Embeddable behaviors
 */

// For logical types that always translate to the same converted type.
type SimpleCompatible struct {
	ConvertedType ConvertedType
}

func (s SimpleCompatible) isCompatible(convertedType ConvertedType, convertedDecimalMetadata DecimalMetadata) bool {
	return (convertedType == s.ConvertedType) && !convertedDecimalMetadata.isSet
}

func (s SimpleCompatible) ToConvertedType(outDecimalMetadata *DecimalMetadata) ConvertedType {
	resetDecimalMetadata(outDecimalMetadata)
	return s.ConvertedType
}

// For logical types that can apply only to a single physical type.
type SimpleApplicable struct {
	Type Type
}

func (s SimpleApplicable) isApplicable(primitiveType Type, primitiveLength int32) bool {
	return primitiveType == s.Type
}

// For logical types that can never apply to any primitive physical type.
type Inapplicable struct {
}

func (Inapplicable) isApplicable(primitiveType Type, primitiveLength int32) bool {
	return false
}

// For logical types that can apply only to a particular physical type and
// physical length combination.
type TypeLengthApplicable struct {
	Type   Type
	Length int32
}

func (l TypeLengthApplicable) isApplicable(primitiveType Type, primitiveLength int32) bool {
	return primitiveType == l.Type && primitiveLength == l.Length
}

// For logical types that have no corresponding converted type.
type Incompatible struct{}

func (Incompatible) isCompatible(convertedType ConvertedType, convertedDecimalMetadata DecimalMetadata) bool {
	return (convertedType == ConvertedType_NONE ||
		convertedType == ConvertedType_NA) &&
		!convertedDecimalMetadata.isSet
}

func (Incompatible) ToConvertedType(outDecimalMetadata *DecimalMetadata) ConvertedType {
	resetDecimalMetadata(outDecimalMetadata)
	return ConvertedType_NONE
}

// For logical types that can apply to any physical type
type UniversalApplicable struct{}

func (UniversalApplicable) isApplicable(primitiveType Type, primitiveLength int32) bool {
	return true
}

// Allowed for physical type BYTE_ARRAY, must be encoded as UTF-8.
type StringLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	SimpleApplicable
}

func NewStringLogicalType() StringLogicalType {
	return StringLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_STRING,
			sortOrder: SortOrder_UNSIGNED,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_UTF8,
		},
		SimpleApplicable: SimpleApplicable{
			Type: Type_BYTE_ARRAY,
		},
	}
}

func (StringLogicalType) ToString() (string, error) {
	return "String", nil
}

func (s StringLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subtype := parquet.NewStringType()
	logicalType.STRING = subtype
	return logicalType, nil
}

func (s StringLogicalType) ToJSON() (string, error) {
	str, err := s.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for group nodes only.
type MapLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	Inapplicable
}

func NewMapLogicalType() MapLogicalType {
	return MapLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_MAP,
			sortOrder: SortOrder_UNKNOWN,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_MAP,
		},
		Inapplicable: Inapplicable{},
	}
}

// override SimpleCompatible.isCompatible()
func (m MapLogicalType) isCompatible(convertedType ConvertedType, convertedDecimalMetadata DecimalMetadata) bool {
	return (convertedType == m.ConvertedType ||
		convertedType == ConvertedType_MAP_KEY_VALUE) &&
		!convertedDecimalMetadata.isSet
}

func (MapLogicalType) ToString() (string, error) {
	return "Map", nil
}

func (m MapLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subtype := parquet.NewMapType()
	logicalType.MAP = subtype
	return logicalType, nil
}

func (m MapLogicalType) ToJSON() (string, error) {
	str, err := m.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for group nodes only.
type ListLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	Inapplicable
}

func NewListLogicalType() ListLogicalType {
	return ListLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_LIST,
			sortOrder: SortOrder_UNKNOWN,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_LIST,
		},
	}
}

func (ListLogicalType) ToString() (string, error) {
	return "List", nil
}

func (l ListLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subtype := parquet.NewListType()
	logicalType.LIST = subtype
	return logicalType, nil
}

func (l ListLogicalType) ToJSON() (string, error) {
	str, err := l.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for physical type BYTE_ARRAY, must be encoded as UTF-8.
type EnumLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	SimpleApplicable
}

func NewEnumLogicalType() EnumLogicalType {
	return EnumLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_ENUM,
			sortOrder: SortOrder_UNSIGNED,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_ENUM,
		},
		SimpleApplicable: SimpleApplicable{
			Type: Type_BYTE_ARRAY,
		},
	}
}

func (EnumLogicalType) ToString() (string, error) {
	return "Enum", nil
}

func (e EnumLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subtype := parquet.NewEnumType()
	logicalType.ENUM = subtype
	return logicalType, nil
}

func (e EnumLogicalType) ToJSON() (string, error) {
	str, err := e.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// The parameterized logical types (currently Decimal, Time, Timestamp, and Int)
// generally can't reuse the simple method implementations available in the base
// and intermediate classes and must (re)implement them all.

// Allowed for physical type INT32, INT64, FIXED_LEN_BYTE_ARRAY, or BYTE_ARRAY,
// depending on the precision.
type DecimalLogicalType struct {
	logicalTypeBase

	Precision int
	Scale     int
}

func NewDecimalLogicalType(precision int, scale int) (DecimalLogicalType, error) {
	if precision < 1 {
		return DecimalLogicalType{}, fmt.Errorf(
			"Precision must be greater than or equal to 1 for Decimal logical type: %w",
			ParquetException,
		)
	}
	if scale < 0 || scale > precision {
		return DecimalLogicalType{}, fmt.Errorf(
			"Scale must be a non-negative integer that does not exceed precision for Decimal logical type: %w",
			ParquetException,
		)
	}
	return DecimalLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_DECIMAL,
			sortOrder: SortOrder_SIGNED,
		},
		Precision: precision,
		Scale:     scale,
	}, nil
}

func (d DecimalLogicalType) isApplicable(primitiveType Type, primitiveLength int32) bool {
	switch primitiveType {
	case Type_INT32:
		return (1 <= d.Precision) && (d.Precision <= 9)
	case Type_INT64:
		ok := (1 <= d.Precision) && (d.Precision <= 18)
		if d.Precision < 10 {
			debug.Warn("DecimalLogicalType.isApplicable: INT32 could be used instead of INT64")
		}
		return ok
	case Type_FIXED_LEN_BYTE_ARRAY:
		return d.Precision <= int(math.Floor(math.Log10(math.Pow(2.0, (8.0*float64(primitiveLength))-1.0))))
	case Type_BYTE_ARRAY:
		return true
	default:
		return false
	}
}

func (d DecimalLogicalType) isCompatible(convertedType ConvertedType,
	convertedDecimalMetadata DecimalMetadata) bool {
	return convertedType == ConvertedType_DECIMAL &&
		(convertedDecimalMetadata.isSet &&
			convertedDecimalMetadata.scale == d.Scale &&
			convertedDecimalMetadata.precision == d.Precision)
}

func (d DecimalLogicalType) ToConvertedType(outDecimalMetadata *DecimalMetadata) ConvertedType {
	setDecimalMetadata(outDecimalMetadata, true, d.Precision, d.Scale)
	return ConvertedType_DECIMAL
}

func (d DecimalLogicalType) ToString() (string, error) {
	return fmt.Sprintf("Decimal(precision=%d, scale=%d)", d.Precision, d.Scale), nil
}

func (d DecimalLogicalType) ToJSON() (string, error) {
	return fmt.Sprintf(
		`{"Type": "Decimal", "precision": %d, "scale": %d}`,
		d.Precision,
		d.Scale,
	), nil
}

func (d DecimalLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	decimalType := parquet.NewDecimalType()
	decimalType.Precision = int32(d.Precision)
	decimalType.Scale = int32(d.Scale)
	logicalType.DECIMAL = decimalType
	return logicalType, nil
}

func (d DecimalLogicalType) Equals(other LogicalType) bool {
	if other.isDecimal() {
		otherDecimal, ok := other.(DecimalLogicalType)
		return ok && d.Precision == otherDecimal.Precision &&
			d.Scale == otherDecimal.Scale
	}

	return false
}

// Allowed for physical type INT32.
type DateLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	SimpleApplicable
}

func NewDateLogicalType() DateLogicalType {
	return DateLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_DATE,
			sortOrder: SortOrder_SIGNED,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_DATE,
		},
		SimpleApplicable: SimpleApplicable{
			Type: Type_INT32,
		},
	}
}

func (DateLogicalType) ToString() (string, error) {
	return "Date", nil
}

func (d DateLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subtype := parquet.NewDateType()
	logicalType.DATE = subtype
	return logicalType, nil
}

func (d DateLogicalType) ToJSON() (string, error) {
	str, err := d.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

func timeUnitString(u LogicalTypeTimeUnit) string {
	switch u {
	case LogicalTypeTimeUnit_MILLIS:
		return "milliseconds"
	case LogicalTypeTimeUnit_MICROS:
		return "microseconds"
	case LogicalTypeTimeUnit_NANOS:
		return "nanoseconds"
	default:
		return "unknown"
	}
}

// Allowed for physical type INT32 (for MILLIS) or INT64 (for MICROS and NANOS).
type TimeLogicalType struct {
	logicalTypeBase

	IsAdjustedToUTC bool
	TimeUnit        LogicalTypeTimeUnit
}

func NewTimeLogicalType(isAdjustedToUTC bool, timeUnit LogicalTypeTimeUnit) (TimeLogicalType, error) {
	if timeUnit != LogicalTypeTimeUnit_MILLIS &&
		timeUnit != LogicalTypeTimeUnit_MICROS &&
		timeUnit != LogicalTypeTimeUnit_NANOS {
		return TimeLogicalType{}, fmt.Errorf("TimeUnit must be one of MILLIS, MICROS, or NANOS for Time logical type: %w", ParquetException)
	}

	return TimeLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_TIME,
			sortOrder: SortOrder_SIGNED,
		},
		IsAdjustedToUTC: isAdjustedToUTC,
		TimeUnit:        timeUnit,
	}, nil
}

func (d TimeLogicalType) isApplicable(primitiveType Type, primitiveLength int32) bool {
	return (primitiveType == Type_INT32 &&
		d.TimeUnit == LogicalTypeTimeUnit_MILLIS) ||
		(primitiveType == Type_INT64 &&
			(d.TimeUnit == LogicalTypeTimeUnit_MICROS ||
				d.TimeUnit == LogicalTypeTimeUnit_NANOS))
}

func (d TimeLogicalType) isCompatible(convertedType ConvertedType,
	convertedDecimalMetadata DecimalMetadata) bool {
	if convertedDecimalMetadata.isSet {
		return false
	} else if d.IsAdjustedToUTC && d.TimeUnit == LogicalTypeTimeUnit_MILLIS {
		return convertedType == ConvertedType_TIME_MILLIS
	} else if d.IsAdjustedToUTC && d.TimeUnit == LogicalTypeTimeUnit_MICROS {
		return convertedType == ConvertedType_TIME_MICROS
	} else {
		return (convertedType == ConvertedType_NONE) ||
			(convertedType == ConvertedType_NA)
	}
}

func (d TimeLogicalType) ToConvertedType(outDecimalMetadata *DecimalMetadata) ConvertedType {
	resetDecimalMetadata(outDecimalMetadata)
	if d.IsAdjustedToUTC {
		if d.TimeUnit == LogicalTypeTimeUnit_MILLIS {
			return ConvertedType_TIME_MILLIS
		} else if d.TimeUnit == LogicalTypeTimeUnit_MICROS {
			return ConvertedType_TIME_MICROS
		}
	}
	return ConvertedType_NONE
}

func (d TimeLogicalType) ToString() (string, error) {
	return fmt.Sprintf(
		"Time(isAdjustedToUTC=%t, timeUnit=%s)",
		d.IsAdjustedToUTC,
		timeUnitString(d.TimeUnit),
	), nil
}

func (d TimeLogicalType) ToJSON() (string, error) {
	return fmt.Sprintf(
		`{"Type": "Time", "isAdjustedToUTC": %t, "timeUnit": "%s"}`,
		d.IsAdjustedToUTC,
		timeUnitString(d.TimeUnit),
	), nil
}

func (d TimeLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	timeType := parquet.NewTimeType()
	timeUnit := parquet.NewTimeUnit()
	debug.Assert(d.TimeUnit != LogicalTypeTimeUnit_UNKNOWN, "TimeLogicalType: ToThrift: logical time unit is UNKNOWN")
	if d.TimeUnit == LogicalTypeTimeUnit_MILLIS {
		millis := parquet.NewMilliSeconds()
		timeUnit.MILLIS = millis
	} else if d.TimeUnit == LogicalTypeTimeUnit_MICROS {
		micros := parquet.NewMicroSeconds()
		timeUnit.MICROS = micros
	} else if d.TimeUnit == LogicalTypeTimeUnit_NANOS {
		nanos := parquet.NewNanoSeconds()
		timeUnit.NANOS = nanos
	}
	timeType.IsAdjustedToUTC = d.IsAdjustedToUTC
	timeType.Unit = timeUnit
	logicalType.TIME = timeType
	return logicalType, nil
}

func (d TimeLogicalType) Equals(other LogicalType) bool {
	if other.isTime() {
		otherTime, ok := other.(TimeLogicalType)
		return ok && d.IsAdjustedToUTC == otherTime.IsAdjustedToUTC &&
			d.TimeUnit == otherTime.TimeUnit
	}

	return false
}

// Allowed for physical type INT64.
type TimestampLogicalType struct {
	logicalTypeBase
	SimpleApplicable

	IsAdjustedToUTC bool
	TimeUnit        LogicalTypeTimeUnit
	// If true, will not set LogicalType in Thrift metadata
	IsFromConvertedType bool
	// If true, will set ConvertedType for micros and millis resolution in
	// legacy ConvertedType Thrift metadata
	ForceSetConvertedType bool
}

func NewTimestampLogicalType(
	isAdjustedToUTC bool, timeUnit LogicalTypeTimeUnit,
	isFromConvertedType bool, forceSetConvertedType bool) (TimestampLogicalType, error) {

	if timeUnit != LogicalTypeTimeUnit_MILLIS &&
		timeUnit != LogicalTypeTimeUnit_MICROS &&
		timeUnit != LogicalTypeTimeUnit_NANOS {
		return TimestampLogicalType{}, fmt.Errorf("TimeUnit must be one of MILLIS, MICROS, or NANOS for Timestamp logical type: %w", ParquetException)
	}

	return TimestampLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_TIMESTAMP,
			sortOrder: SortOrder_SIGNED,
		},
		SimpleApplicable: SimpleApplicable{
			Type: Type_INT64,
		},
		IsAdjustedToUTC:       isAdjustedToUTC,
		TimeUnit:              timeUnit,
		IsFromConvertedType:   isFromConvertedType,
		ForceSetConvertedType: forceSetConvertedType,
	}, nil
}

func (t TimestampLogicalType) isSerialized() bool {
	return !t.IsFromConvertedType
}

func (t TimestampLogicalType) isCompatible(convertedType ConvertedType,
	convertedDecimalMetadata DecimalMetadata) bool {
	if convertedDecimalMetadata.isSet {
		return false
	} else if t.TimeUnit == LogicalTypeTimeUnit_MILLIS {
		if t.IsAdjustedToUTC || t.ForceSetConvertedType {
			return convertedType == ConvertedType_TIMESTAMP_MILLIS
		} else {
			return convertedType == ConvertedType_NONE ||
				convertedType == ConvertedType_NA
		}
	} else if t.TimeUnit == LogicalTypeTimeUnit_MICROS {
		if t.IsAdjustedToUTC || t.ForceSetConvertedType {
			return convertedType == ConvertedType_TIMESTAMP_MICROS
		} else {
			return convertedType == ConvertedType_NONE ||
				convertedType == ConvertedType_NA
		}
	} else {
		return convertedType == ConvertedType_NONE ||
			convertedType == ConvertedType_NA
	}
}

func (t TimestampLogicalType) ToConvertedType(
	outDecimalMetadata *DecimalMetadata) ConvertedType {
	resetDecimalMetadata(outDecimalMetadata)
	if t.IsAdjustedToUTC || t.ForceSetConvertedType {
		if t.TimeUnit == LogicalTypeTimeUnit_MILLIS {
			return ConvertedType_TIMESTAMP_MILLIS
		} else if t.TimeUnit == LogicalTypeTimeUnit_MICROS {
			return ConvertedType_TIMESTAMP_MICROS
		}
	}
	return ConvertedType_NONE
}

func (t TimestampLogicalType) ToString() (string, error) {
	return fmt.Sprintf(
		"Timestamp(isAdjustedToUTC=%t, timeUnit=%s, is_from_converted_type=%t, force_set_converted_type=%t)",
		t.IsAdjustedToUTC,
		timeUnitString(t.TimeUnit),
		t.IsFromConvertedType,
		t.ForceSetConvertedType,
	), nil
}

func (t TimestampLogicalType) ToJSON() (string, error) {
	return fmt.Sprintf(
		`{"Type": "Timestamp", "isAdjustedToUTC": %t, "timeUnit": "%s", "is_from_converted_type": %t, "force_set_converted_type": %t}`,
		t.IsAdjustedToUTC,
		timeUnitString(t.TimeUnit),
		t.IsFromConvertedType,
		t.ForceSetConvertedType,
	), nil
}

func (t TimestampLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	timestampType := parquet.NewTimestampType()
	timeUnit := parquet.NewTimeUnit()
	debug.Assert(t.TimeUnit != LogicalTypeTimeUnit_UNKNOWN, "TimeLogicalType: ToThrift: logical time unit is UNKNOWN")
	if t.TimeUnit == LogicalTypeTimeUnit_MILLIS {
		millis := parquet.NewMilliSeconds()
		timeUnit.MILLIS = millis
	} else if t.TimeUnit == LogicalTypeTimeUnit_MICROS {
		micros := parquet.NewMicroSeconds()
		timeUnit.MICROS = micros
	} else if t.TimeUnit == LogicalTypeTimeUnit_NANOS {
		nanos := parquet.NewNanoSeconds()
		timeUnit.NANOS = nanos
	}
	timestampType.IsAdjustedToUTC = t.IsAdjustedToUTC
	timestampType.Unit = timeUnit
	logicalType.TIMESTAMP = timestampType
	return logicalType, nil
}

func (t TimestampLogicalType) Equals(other LogicalType) bool {
	if other.isTimestamp() {
		otherTimestamp, ok := other.(TimestampLogicalType)
		return ok && t.IsAdjustedToUTC == otherTimestamp.IsAdjustedToUTC &&
			t.TimeUnit == otherTimestamp.TimeUnit
	}

	return false
}

// Allowed for physical type FIXED_LEN_BYTE_ARRAY with length 12
type IntervalLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	TypeLengthApplicable
}

func NewIntervalLogicalType() IntervalLogicalType {
	return IntervalLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_INTERVAL,
			sortOrder: SortOrder_UNKNOWN,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_INTERVAL,
		},
		TypeLengthApplicable: TypeLengthApplicable{
			Type:   Type_FIXED_LEN_BYTE_ARRAY,
			Length: 12,
		},
	}
}

func (i IntervalLogicalType) ToString() (string, error) {
	return "Interval", nil
}

func (i IntervalLogicalType) ToThrift() (*parquet.LogicalType, error) {
	// FIXME(nickpoorman): uncomment the following line to enable serialization after
	// parquet.thrift recognizes IntervalType as a ConvertedType.
	str, err := i.ToString()
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("Logical type %s should not be serialized: %w", str, ParquetException)
}

func (i IntervalLogicalType) ToJSON() (string, error) {
	str, err := i.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for physical type INT32 (for bit widths 8, 16, and 32) and INT64
// (for bit width 64).
type IntLogicalType struct {
	logicalTypeBase

	Width  int
	Signed bool
}

func NewIntLogicalType(bitWidth int, isSigned bool) (IntLogicalType, error) {
	if bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64 {
		return IntLogicalType{}, fmt.Errorf("NewIntLogicalType: bitWith must be one of: 8, 16, 32, 64: %w", ParquetException)
	}

	sortOrder := SortOrder_SIGNED
	if !isSigned {
		sortOrder = SortOrder_UNSIGNED
	}
	return IntLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_INT,
			sortOrder: sortOrder,
		},
		Width:  bitWidth,
		Signed: isSigned,
	}, nil
}

func (i IntLogicalType) isApplicable(primitiveType Type,
	primitiveLength int32) bool {
	return (primitiveType == Type_INT32 && i.Width <= 32) ||
		(primitiveType == Type_INT64 && i.Width == 64)
}

func (i IntLogicalType) isCompatible(convertedType ConvertedType,
	convertedDecimalMetadata DecimalMetadata) bool {
	if convertedDecimalMetadata.isSet {
		return false
	} else if i.Signed && i.Width == 8 {
		return convertedType == ConvertedType_INT_8
	} else if i.Signed && i.Width == 16 {
		return convertedType == ConvertedType_INT_16
	} else if i.Signed && i.Width == 32 {
		return convertedType == ConvertedType_INT_32
	} else if i.Signed && i.Width == 64 {
		return convertedType == ConvertedType_INT_64
	} else if !i.Signed && i.Width == 8 {
		return convertedType == ConvertedType_UINT_8
	} else if !i.Signed && i.Width == 16 {
		return convertedType == ConvertedType_UINT_16
	} else if !i.Signed && i.Width == 32 {
		return convertedType == ConvertedType_UINT_32
	} else if !i.Signed && i.Width == 64 {
		return convertedType == ConvertedType_UINT_64
	} else {
		return false
	}
}

func (i IntLogicalType) ToConvertedType(
	outDecimalMetadata *DecimalMetadata) ConvertedType {
	resetDecimalMetadata(outDecimalMetadata)
	if i.Signed {
		switch i.Width {
		case 8:
			return ConvertedType_INT_8
		case 16:
			return ConvertedType_INT_16
		case 32:
			return ConvertedType_INT_32
		case 64:
			return ConvertedType_INT_64
		}
	} else {
		switch i.Width {
		case 8:
			return ConvertedType_UINT_8
		case 16:
			return ConvertedType_UINT_16
		case 32:
			return ConvertedType_UINT_32
		case 64:
			return ConvertedType_UINT_64
		}
	}
	return ConvertedType_NONE
}

func (i IntLogicalType) ToString() (string, error) {
	return fmt.Sprintf("Int(bitWidth=%d, isSigned=%t)", i.Width, i.Signed), nil
}

func (i IntLogicalType) ToJSON() (string, error) {
	return fmt.Sprintf(
		`{"Type": "Int", "bitWidth": %d, "isSigned": %t}`,
		i.Width, i.Signed,
	), nil
}

func (i IntLogicalType) ToThrift() (*parquet.LogicalType, error) {
	debug.Assert(i.Width == 64 || i.Width == 32 || i.Width == 16 || i.Width == 8,
		fmt.Sprintf("IntLogicalType: ToThrift: Width must be one of: 64, 32, 16, 8. Is: %d", i.Width))

	logicalType := parquet.NewLogicalType()
	intType := parquet.NewIntType()
	intType.BitWidth = int8(i.Width)
	intType.IsSigned = i.Signed
	logicalType.INTEGER = intType
	return logicalType, nil
}

func (i IntLogicalType) Equals(other LogicalType) bool {
	if other.isInt() {
		otherInt, ok := other.(IntLogicalType)
		return ok &&
			i.Width == otherInt.Width &&
			i.Signed == otherInt.Signed
	}
	return false
}

// Allowed for any physical type.
type NullLogicalType struct {
	logicalTypeBase
	Incompatible
	UniversalApplicable
}

func NewNullLogicalType() NullLogicalType {
	return NullLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_NIL,
			sortOrder: SortOrder_UNKNOWN,
		},
	}
}

func (n NullLogicalType) ToString() (string, error) {
	return fmt.Sprintf("Null"), nil
}

func (n NullLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subType := parquet.NewNullType()
	logicalType.UNKNOWN = subType
	return logicalType, nil
}

func (n NullLogicalType) ToJSON() (string, error) {
	str, err := n.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for physical type BYTE_ARRAY.
type JSONLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	SimpleApplicable
}

func NewJSONLogicalType() JSONLogicalType {
	return JSONLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_JSON,
			sortOrder: SortOrder_UNSIGNED,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_JSON,
		},
		SimpleApplicable: SimpleApplicable{
			Type: Type_BYTE_ARRAY,
		},
	}
}

func (JSONLogicalType) ToString() (string, error) {
	return "JSON", nil
}

func (j JSONLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subType := parquet.NewJsonType()
	logicalType.JSON = subType
	return logicalType, nil
}

func (j JSONLogicalType) ToJSON() (string, error) {
	str, err := j.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for physical type BYTE_ARRAY.
type BSONLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	SimpleApplicable
}

func NewBSONLogicalType() BSONLogicalType {
	return BSONLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_BSON,
			sortOrder: SortOrder_UNSIGNED,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_BSON,
		},
		SimpleApplicable: SimpleApplicable{
			Type: Type_BYTE_ARRAY,
		},
	}
}

func (BSONLogicalType) ToString() (string, error) {
	return "BSON", nil
}

func (b BSONLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subType := parquet.NewBsonType()
	logicalType.BSON = subType
	return logicalType, nil
}

func (b BSONLogicalType) ToJSON() (string, error) {
	str, err := b.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for physical type FIXED_LEN_BYTE_ARRAY with length 16, must encode
// raw UUID bytes.
type UUIDLogicalType struct {
	logicalTypeBase
	Incompatible
	TypeLengthApplicable
}

func NewUUIDLogicalType() UUIDLogicalType {
	return UUIDLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_UUID,
			sortOrder: SortOrder_UNSIGNED,
		},
		TypeLengthApplicable: TypeLengthApplicable{
			Type:   Type_FIXED_LEN_BYTE_ARRAY,
			Length: 16,
		},
	}
}

func (UUIDLogicalType) ToString() (string, error) {
	return "UUID", nil
}

func (u UUIDLogicalType) ToThrift() (*parquet.LogicalType, error) {
	logicalType := parquet.NewLogicalType()
	subType := parquet.NewUUIDType()
	logicalType.UUID = subType
	return logicalType, nil
}

func (u UUIDLogicalType) ToJSON() (string, error) {
	str, err := u.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for any physical type.
type NoLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	UniversalApplicable
}

func NewNoLogicalType() NoLogicalType {
	return NoLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_NONE,
			sortOrder: SortOrder_UNKNOWN,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_NONE,
		},
	}
}

func (NoLogicalType) ToString() (string, error) {
	return "None", nil
}

func (n NoLogicalType) ToThrift() (*parquet.LogicalType, error) {
	str, err := n.ToString()
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("Logical type %s should not be serialized: %w", str, ParquetException)
}

func (n NoLogicalType) ToJSON() (string, error) {
	str, err := n.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Allowed for any type.
type UnknownLogicalType struct {
	logicalTypeBase
	SimpleCompatible
	UniversalApplicable
}

func NewUnknownLogicalType() UnknownLogicalType {
	return UnknownLogicalType{
		logicalTypeBase: logicalTypeBase{
			kind:      LogicalTypeKind_UNKNOWN,
			sortOrder: SortOrder_UNKNOWN,
		},
		SimpleCompatible: SimpleCompatible{
			ConvertedType: ConvertedType_NA,
		},
	}
}

func (UnknownLogicalType) ToString() (string, error) {
	return "Unknown", nil
}

func (u UnknownLogicalType) ToThrift() (*parquet.LogicalType, error) {
	str, err := u.ToString()
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("Logical type %s should not be serialized: %w", str, ParquetException)
}

func (u UnknownLogicalType) ToJSON() (string, error) {
	str, err := u.ToString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Type": "%s"}`, str), nil
}

// Data encodings. Mirrors parquet::Encoding
type EncodingType int

const (
	EncodingType_PLAIN                   = 0
	EncodingType_PLAIN_DICTIONARY        = 2
	EncodingType_RLE                     = 3
	EncodingType_BIT_PACKED              = 4
	EncodingType_DELTA_BINARY_PACKED     = 5
	EncodingType_DELTA_LENGTH_BYTE_ARRAY = 6
	EncodingType_DELTA_BYTE_ARRAY        = 7
	EncodingType_RLE_DICTIONARY          = 8
	EncodingType_UNKNOWN                 = 999
)

// IsCodecSupported returns true if Parquet supports indicated compression type.
func IsCodecSupported(codec compress.CompressionCodec) bool {
	switch codec {
	case compress.CompressionCodec_UNCOMPRESSED:
		return true
	case compress.CompressionCodec_SNAPPY:
		return true
	case compress.CompressionCodec_GZIP:
		return true
	case compress.CompressionCodec_BROTLI:
		return false
	case compress.CompressionCodec_ZSTD:
		return true
	case compress.CompressionCodec_LZ4:
		return false
	case compress.CompressionCodec_LZO:
		return false
	case compress.CompressionCodec_BZ2:
		return false
	}

	return false
}

func GetCodec(codec compress.CompressionCodec) (compress.Compressor, error) {
	return GetCodecLevel(codec, compress.UseDefaultCompressionLevel())
}

func GetCodecLevel(codec compress.CompressionCodec, compressionLevel int) (compress.Compressor, error) {
	if !IsCodecSupported(codec) {
		return nil, fmt.Errorf("Codec type %s not supported in Parquet format: %w", compress.GetCodecAsString(codec), ParquetException)
	}
	return compress.CodecCreate(codec, compressionLevel)
}

type ParquetCipherTypeEnum int

const (
	ParquetCipherTypeEnum_AES_GCM_V1     = 0
	ParquetCipherTypeEnum_AES_GCM_CTR_V1 = 1
)

type ParquetCipher struct {
	Type ParquetCipherTypeEnum
}

type AadMetadata struct {
	AadPrefix       string
	AadFileUnique   string
	SupplyAadPrefix bool
}

type EncryptionAlgorithm struct {
	Algorithm ParquetCipher
	Aad       AadMetadata
}

// parquet::PageType
type PageType int

const (
	_ PageType = iota
	PageType_DATA_PAGE
	PageType_INDEX_PAGE
	PageType_DICTIONARY_PAGE
	PageType_DATA_PAGE_V2
)

type ColumnOrder int

const (
	_ ColumnOrder = iota
	ColumnOrder_UNDEFINED
	ColumnOrder_TYPE_DEFINED_ORDER
)

// TODO: Remove
// type ColumnOrder struct {
// 	ColumnOrder ColumnOrderEnum
// }

// func NewColumnOrder() ColumnOrder {
// 	return ColumnOrder{
// 		// Default to Type Defined Order
// 		ColumnOrder: ColumnOrderEnum_TYPE_DEFINED_ORDER,
// 	}
// }

// func NewColumnOrderFromType(columnOrder ColumnOrderEnum) ColumnOrder {
// 	return ColumnOrder{
// 		ColumnOrder: columnOrder,
// 	}
// }

// func NewColumnOrderUndefined() ColumnOrder {
// 	return ColumnOrder{
// 		// Default to Type Defined Order
// 		ColumnOrder: ColumnOrderEnum_UNDEFINED,
// 	}
// }

// func NewColumnOrderTypeDefined() ColumnOrder {
// 	return ColumnOrder{
// 		// Default to Type Defined Order
// 		ColumnOrder: ColumnOrderEnum_TYPE_DEFINED_ORDER,
// 	}
// }

// ----------------------------------------------------------------------

// https://github.com/apache/arrow/blob/41753ace481a82dea651c54639ec4adbae169187/cpp/src/parquet/types.h#L506
type ByteArray struct {
	len int
	ptr []byte
}

func NewByteArrayEmpty() ByteArray {
	var b []byte
	return ByteArray{
		len: 0,
		ptr: b,
	}
}

func NewByteArray(len int, ptr []byte) ByteArray {
	return ByteArray{
		len: len,
		ptr: ptr,
	}
}

func NewByteArrayImplicit(view string) ByteArray {
	// https://github.com/apache/arrow/blob/41753ace481a82dea651c54639ec4adbae169187/cpp/src/parquet/types.h#L510
	b := []byte(view)
	return ByteArray{
		len: len(b),
		ptr: b,
	}
}

func (left *ByteArray) Equals(right ByteArray) bool {
	return left.len == right.len &&
		(left.len == 0 || bytes.Equal(left.ptr[0:left.len], right.ptr[0:right.len]))
}

func (left *ByteArray) NotEquals(right ByteArray) bool {
	return !left.Equals(right)
}

type FixedLenByteArray struct {
	ptr []byte
}

func NewFixedLenByteArrayEmpty() FixedLenByteArray {
	var b []byte
	return FixedLenByteArray{
		ptr: b,
	}
}

func NewFixedLenByteArray(ptr []byte) FixedLenByteArray {
	var b []byte
	return FixedLenByteArray{
		ptr: b,
	}
}

type FLBA = FixedLenByteArray

// Julian day at unix epoch.
//
// The Julian Day Number (JDN) is the integer assigned to a whole solar day in
// the Julian day count starting from noon Universal time, with Julian day
// number 0 assigned to the day starting at noon on Monday, January 1, 4713 BC,
// proleptic Julian calendar (November 24, 4714 BC, in the proleptic Gregorian
// calendar).
const (
	kJulianToUnixEpochDays = int64(2440588)
	kSecondsPerDay         = int64(60 * 60 * 24)
	kMillisecondsPerDay    = kSecondsPerDay * int64(1000)
	kMicrosecondsPerDay    = kMillisecondsPerDay * int64(1000)
	kNanosecondsPerDay     = kMicrosecondsPerDay * int64(1000)
)

type Int96 string

// They implemented a bunch of Int96 stuff here. But I have no interest in doing that as it's been deprecated.
// https://github.com/apache/arrow/blob/41753ace481a82dea651c54639ec4adbae169187/cpp/src/parquet/types.h#L547

func ByteArrayToString(a *ByteArray) string {
	return string(a.ptr[:a.len])
}

func FixedLenByteArrayToString(a []byte, len int) string {
	return string(a[0:len])
}

type typeTraits struct {
	valueType     reflect.Type
	valueByteSize int
	printfCode    string
}

// TODO: ... implement the rest of the traits
var typeTraitsBoolean = typeTraits{reflect.TypeOf(bool(false)), 1, "d"}
var typeTraitsInt32 = typeTraits{reflect.TypeOf(int32(0)), 4, "d"}
var typeTraitsInt64 = typeTraits{reflect.TypeOf(int64(0)), 8, "d"}
var typeTraitsInt96 = typeTraits{reflect.TypeOf(Int96("")), 12, "s"}
var typeTraitsFloat = typeTraits{reflect.TypeOf(float32(0)), 4, "f"}
var typeTraitsDouble = typeTraits{reflect.TypeOf(float64(0)), 8, "f"}
var typeTraitsByteArray = typeTraits{reflect.TypeOf(ByteArray{}), int(binary.Size(ByteArray{})), "s"}
var typeTraitsFixedLenByteArray = typeTraits{reflect.TypeOf(FixedLenByteArray{}), int(binary.Size(FixedLenByteArray{})), "s"}

type PhysicalType struct {
	typeTraits
	dtype Type
	name  string
}

func (p PhysicalType) ctype() reflect.Type {
	return p.valueType
}

func (p PhysicalType) typeNum() Type {
	return p.dtype
}

func (p PhysicalType) Name() string {
	return p.name
}

var BooleanType = PhysicalType{typeTraitsBoolean, Type_BOOLEAN, "BooleanType"}
var Int32Type = PhysicalType{typeTraitsInt32, Type_INT32, "Int32Type"}
var Int64Type = PhysicalType{typeTraitsInt64, Type_INT64, "Int64Type"}
var Int96Type = PhysicalType{typeTraitsInt96, Type_INT96, "Int96Type"}
var FloatType = PhysicalType{typeTraitsFloat, Type_FLOAT, "FloatType"}
var DoubleType = PhysicalType{typeTraitsDouble, Type_DOUBLE, "DoubleType"}
var ByteArrayType = PhysicalType{typeTraitsByteArray, Type_BYTE_ARRAY, "ByteArrayType"}
var FLBAType = PhysicalType{typeTraitsFixedLenByteArray, Type_FIXED_LEN_BYTE_ARRAY, "FLBAType"}

func EncodingToString(t EncodingType) string {
	switch t {
	case EncodingType_PLAIN:
		return "PLAIN"
	case EncodingType_PLAIN_DICTIONARY:
		return "PLAIN_DICTIONARY"
	case EncodingType_RLE:
		return "RLE"
	case EncodingType_BIT_PACKED:
		return "BIT_PACKED"
	case EncodingType_DELTA_BINARY_PACKED:
		return "DELTA_BINARY_PACKED"
	case EncodingType_DELTA_LENGTH_BYTE_ARRAY:
		return "DELTA_LENGTH_BYTE_ARRAY"
	case EncodingType_DELTA_BYTE_ARRAY:
		return "DELTA_BYTE_ARRAY"
	case EncodingType_RLE_DICTIONARY:
		return "RLE_DICTIONARY"
	default:
		return "UNKNOWN"
	}
}

func ConvertedTypeToString(t ConvertedType) string {
	switch t {
	case ConvertedType_NONE:
		return "NONE"
	case ConvertedType_UTF8:
		return "UTF8"
	case ConvertedType_MAP:
		return "MAP"
	case ConvertedType_MAP_KEY_VALUE:
		return "MAP_KEY_VALUE"
	case ConvertedType_LIST:
		return "LIST"
	case ConvertedType_ENUM:
		return "ENUM"
	case ConvertedType_DECIMAL:
		return "DECIMAL"
	case ConvertedType_DATE:
		return "DATE"
	case ConvertedType_TIME_MILLIS:
		return "TIME_MILLIS"
	case ConvertedType_TIME_MICROS:
		return "TIME_MICROS"
	case ConvertedType_TIMESTAMP_MILLIS:
		return "TIMESTAMP_MILLIS"
	case ConvertedType_TIMESTAMP_MICROS:
		return "TIMESTAMP_MICROS"
	case ConvertedType_UINT_8:
		return "UINT_8"
	case ConvertedType_UINT_16:
		return "UINT_16"
	case ConvertedType_UINT_32:
		return "UINT_32"
	case ConvertedType_UINT_64:
		return "UINT_64"
	case ConvertedType_INT_8:
		return "INT_8"
	case ConvertedType_INT_16:
		return "INT_16"
	case ConvertedType_INT_32:
		return "INT_32"
	case ConvertedType_INT_64:
		return "INT_64"
	case ConvertedType_JSON:
		return "JSON"
	case ConvertedType_BSON:
		return "BSON"
	case ConvertedType_INTERVAL:
		return "INTERVAL"
	case ConvertedType_NA, ConvertedType_UNDEFINED:
		return "UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

func TypeToString(t Type) string {
	switch t {
	case Type_BOOLEAN:
		return "BOOLEAN"
	case Type_INT32:
		return "INT32"
	case Type_INT64:
		return "INT64"
	case Type_INT96:
		return "INT96"
	case Type_FLOAT:
		return "FLOAT"
	case Type_DOUBLE:
		return "DOUBLE"
	case Type_BYTE_ARRAY:
		return "BYTE_ARRAY"
	case Type_FIXED_LEN_BYTE_ARRAY:
		return "FIXED_LEN_BYTE_ARRAY"
	case Type_UNDEFINED:
		return "UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

// TODO: REMOVE. This appears to be used to print the value of the type.
// Go fmt does this for us.
// func FormatStatValue(parquetType Type, val string) string {
// var result strings.Builder
// switch parquetType {
// case Type_BOOLEAN:
// case Type_INT32:
// case Type_INT64:
// case Type_INT96:
// case Type_FLOAT:
// case Type_DOUBLE:
// case Type_BYTE_ARRAY:
// case Type_FIXED_LEN_BYTE_ARRAY:
// case Type_UNDEFINED:
// }
// return result.String()
// 	return val
// }

func GetTypeByteSize(parquetType Type) int {
	switch parquetType {
	case Type_BOOLEAN:
		return 1
	case Type_INT32:
		return 4
	case Type_INT64:
		return 8
	case Type_INT96:
		return 12
	case Type_FLOAT:
		return 4
	case Type_DOUBLE:
		return 8
	case Type_BYTE_ARRAY:
		// return 16 // might need to adjust this for Go struct size
		var a ByteArray
		return int(binary.Size(a))
	case Type_FIXED_LEN_BYTE_ARRAY:
		// return 8 // might need to adjust this for Go struct size
		var a FixedLenByteArray
		return int(binary.Size(a))
	case Type_UNDEFINED:
		return 0
	default:
		return 0
	}
}

// DefaultSortOrder returns the Sort Order of the Parquet Physical Types.
func DefaultSortOrder(primitive Type) SortOrder {
	switch primitive {
	case Type_BOOLEAN, Type_INT32, Type_INT64, Type_FLOAT, Type_DOUBLE:
		return SortOrder_SIGNED
	case Type_BYTE_ARRAY, Type_FIXED_LEN_BYTE_ARRAY:
		return SortOrder_UNSIGNED
	case Type_INT96, Type_UNDEFINED:
		return SortOrder_UNKNOWN
	default:
		return SortOrder_UNKNOWN
	}
}

// GetSortOrderConverted returns the SortOrder of the Parquet Types using
// Logical or Physical Types.
func GetSortOrderConverted(converted ConvertedType, primitive Type) SortOrder {
	if converted == ConvertedType_NONE {
		return DefaultSortOrder(primitive)
	}
	switch converted {
	case ConvertedType_INT_8, ConvertedType_INT_16, ConvertedType_INT_32, ConvertedType_INT_64,
		ConvertedType_DATE, ConvertedType_TIME_MICROS, ConvertedType_TIME_MILLIS,
		ConvertedType_TIMESTAMP_MILLIS, ConvertedType_TIMESTAMP_MICROS:
		return SortOrder_SIGNED

	case ConvertedType_UINT_8, ConvertedType_UINT_16, ConvertedType_UINT_32, ConvertedType_UINT_64,
		ConvertedType_ENUM, ConvertedType_UTF8, ConvertedType_BSON, ConvertedType_JSON:
		return SortOrder_UNSIGNED

	case ConvertedType_DECIMAL, ConvertedType_LIST, ConvertedType_MAP, ConvertedType_MAP_KEY_VALUE,
		ConvertedType_INTERVAL, ConvertedType_NONE, ConvertedType_NA, ConvertedType_UNDEFINED:
		return SortOrder_UNKNOWN

	default:
		return SortOrder_UNKNOWN
	}
}

// GetSortOrderConverted returns the SortOrder of the Parquet Types using
// Logical or Physical Types.
func GetSortOrderLogical(logicalType LogicalType, primitive Type) SortOrder {
	if logicalType != nil && logicalType.isValid() {
		if logicalType.isNone() {
			return DefaultSortOrder(primitive)
		} else {
			return logicalType.SortOrder()
		}
	}
	return SortOrder_UNKNOWN
}

func LogicalTypeFromThrift(t parquet.LogicalType) (LogicalType, error) {
	if t.IsSetSTRING() {
		return NewStringLogicalType(), nil
	} else if t.IsSetMAP() {
		return NewMapLogicalType(), nil
	} else if t.IsSetLIST() {
		return NewListLogicalType(), nil
	} else if t.IsSetENUM() {
		return NewEnumLogicalType(), nil
	} else if t.IsSetDECIMAL() {
		return NewDecimalLogicalType(int(t.DECIMAL.Precision), int(t.DECIMAL.Scale))
	} else if t.IsSetDATE() {
		return NewDateLogicalType(), nil
	} else if t.IsSetTIME() {
		var unit LogicalTypeTimeUnit
		if t.TIME.Unit.IsSetMILLIS() {
			unit = LogicalTypeTimeUnit_MILLIS
		} else if t.TIME.Unit.IsSetMICROS() {
			unit = LogicalTypeTimeUnit_MICROS
		} else if t.TIME.Unit.IsSetNANOS() {
			unit = LogicalTypeTimeUnit_NANOS
		} else {
			unit = LogicalTypeTimeUnit_UNKNOWN
		}
		return NewTimeLogicalType(t.TIME.IsAdjustedToUTC, unit)
	} else if t.IsSetTIMESTAMP() {
		var unit LogicalTypeTimeUnit
		if t.TIMESTAMP.Unit.IsSetMILLIS() {
			unit = LogicalTypeTimeUnit_MILLIS
		} else if t.TIMESTAMP.Unit.IsSetMICROS() {
			unit = LogicalTypeTimeUnit_MICROS
		} else if t.TIMESTAMP.Unit.IsSetNANOS() {
			unit = LogicalTypeTimeUnit_NANOS
		} else {
			unit = LogicalTypeTimeUnit_UNKNOWN
		}
		return NewTimestampLogicalType(t.TIMESTAMP.IsAdjustedToUTC, unit, false, false)
		// TODO(nickpoorman): activate the commented code after parquet.thrift
		// recognizes IntervalType as a LogicalType
		//} else if t.IsSetINTERVAL() {
		//  return NewIntervalLogicalType()
	} else if t.IsSetINTEGER() {
		return NewIntLogicalType(int(t.INTEGER.BitWidth), t.INTEGER.IsSigned)
	} else if t.IsSetUNKNOWN() {
		return NewNullLogicalType(), nil
	} else if t.IsSetJSON() {
		return NewJSONLogicalType(), nil
	} else if t.IsSetBSON() {
		return NewBSONLogicalType(), nil
	} else if t.IsSetUUID() {
		return NewUUIDLogicalType(), nil
	} else {
		return nil, fmt.Errorf("Metadata contains Thrift LogicalType that is not recognized: %w", ParquetException)
	}
}

var (
	_ LogicalType = (*StringLogicalType)(nil)
	_ LogicalType = (*MapLogicalType)(nil)
	_ LogicalType = (*ListLogicalType)(nil)
	_ LogicalType = (*EnumLogicalType)(nil)
	_ LogicalType = (*DecimalLogicalType)(nil)
	_ LogicalType = (*DateLogicalType)(nil)
	_ LogicalType = (*TimeLogicalType)(nil)
	_ LogicalType = (*TimestampLogicalType)(nil)
	_ LogicalType = (*IntervalLogicalType)(nil)
	_ LogicalType = (*IntLogicalType)(nil)
	_ LogicalType = (*NullLogicalType)(nil)
	_ LogicalType = (*JSONLogicalType)(nil)
	_ LogicalType = (*BSONLogicalType)(nil)
	_ LogicalType = (*UUIDLogicalType)(nil)
	_ LogicalType = (*NoLogicalType)(nil)
	_ LogicalType = (*UnknownLogicalType)(nil)
)
