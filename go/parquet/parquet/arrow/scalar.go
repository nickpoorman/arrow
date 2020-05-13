package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

type Scalar interface {
	ScalarEqualityComparable
	// Returns a reader to the value. Note this has more overhead than ValueBytes()
	ValueBytes() []byte
}

type ScalarEqualityComparable interface {
	Equals(other Scalar) bool
	NotEquals(other Scalar) bool
}

// TODO(nickpoorman):
// Current Arrow implementation does not yet support these types.

type UnionType struct{}

func (t *UnionType) ID() arrow.Type { return arrow.UNION }
func (t *UnionType) Name() string   { return "union" }
func (t *UnionType) String() string { return "union" }

type MapType struct{}

func (t *MapType) ID() arrow.Type { return arrow.MAP }
func (t *MapType) Name() string   { return "union" }
func (t *MapType) String() string { return "union" }

type LargeBinaryType struct {
	arrow.BinaryType
}

func (t *LargeBinaryType) ID() arrow.Type { return arrow.DURATION + 2 }
func (t *LargeBinaryType) Name() string   { return "large_binary" }
func (t *LargeBinaryType) String() string { return "large_binary" }
func (t *LargeBinaryType) binary()        {}

type LargeListType struct{}

func (t *LargeListType) ID() arrow.Type { return arrow.DURATION + 3 }
func (t *LargeListType) Name() string   { return "union" }
func (t *LargeListType) String() string { return "union" }

var (
	UnsupportedTypes = struct {
		Union       arrow.DataType
		Map         arrow.DataType
		LargeBinary arrow.BinaryDataType
		LargeList   arrow.DataType
	}{
		Union:       &UnionType{},
		Map:         (*MapType)(nil),
		LargeBinary: &LargeBinaryType{},
		LargeList:   (*LargeListType)(nil),
	}
)

type NullScalar struct {
	// The type of the scalar value
	dataType arrow.DataType
}

func NewNullScalar(dataType arrow.DataType) NullScalar {
	return NullScalar{
		dataType: dataType,
	}
}

func (s NullScalar) Equals(other Scalar) bool {
	_, ok := other.(NullScalar)
	if !ok {
		return false
	}
	return true
}

func (s NullScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s NullScalar) ValueBytes() []byte {
	panic("not implemented")
}

type StructScalar struct {
	// The type of the scalar value
	dataType arrow.DataType

	// Whether the value is valid (not null) or not
	isValid bool

	value []Scalar
}

func NewStructScalar(value []Scalar, dataType arrow.DataType, isValid bool) StructScalar {
	scalar := StructScalar{
		isValid:  isValid,
		dataType: dataType,
	}
	return scalar
}

func (s StructScalar) Equals(other Scalar) bool {
	right, ok := other.(StructScalar)
	if !ok {
		return false
	}
	for i := range right.value {
		if s.value[i] != right.value[i] {
			return false
		}
	}
	return true
}

func (s StructScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s StructScalar) ValueBytes() []byte {
	panic("not implemented")
}

type UnionScalar struct {
	// The type of the scalar value
	dataType arrow.DataType

	// Whether the value is valid (not null) or not
	isValid bool

	value interface{}
}

func NewUnionScalar(value interface{}, dataType arrow.DataType, isValid bool) UnionScalar {
	scalar := UnionScalar{
		isValid:  isValid,
		dataType: dataType,
	}
	return scalar
}

func (s UnionScalar) Equals(other Scalar) bool {
	right, ok := other.(UnionScalar)
	return ok && s.value == right.value
}

func (s UnionScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s UnionScalar) ValueBytes() []byte {
	panic("not implemented")
}

type DictionaryScalar struct {
	// The type of the scalar value
	dataType arrow.DataType

	// Whether the value is valid (not null) or not
	isValid bool

	// (nickpoorman): this may need to be a slice of Scalars
	value *Scalar
}

func NewDictionaryScalar(value *Scalar, dataType arrow.DataType, isValid bool) DictionaryScalar {
	scalar := DictionaryScalar{
		isValid:  isValid,
		dataType: dataType,
	}
	return scalar
}

func (s DictionaryScalar) Equals(other Scalar) bool {
	right, ok := other.(DictionaryScalar)
	return ok && s.value == right.value
}

func (s DictionaryScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s DictionaryScalar) ValueBytes() []byte {
	panic("not implemented")
}

type ExtensionScalar struct {
	// The type of the scalar value
	dataType arrow.DataType

	// Whether the value is valid (not null) or not
	isValid bool

	value Scalar
}

func NewExtensionScalar(value Scalar, dataType arrow.DataType, isValid bool) ExtensionScalar {
	scalar := ExtensionScalar{
		isValid:  isValid,
		dataType: dataType,
	}
	return scalar
}

func (s ExtensionScalar) Equals(other Scalar) bool {
	right, ok := other.(ExtensionScalar)
	return ok && s.value == right.value
}

func (s ExtensionScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s ExtensionScalar) ValueBytes() []byte {
	panic("not implemented")
}

func MakeNullScalar(dataType arrow.DataType) Scalar {
	panic("not implemented")
}

func CheckBufferLength(t *arrow.FixedSizeBinaryType, b *memory.Buffer) error {
	if t.ByteWidth != b.Len() {
		return fmt.Errorf("buffer length %d is not compatible with %#v", b.Len(), t)
	}
	return nil
}

var (
	_ Scalar = (*NullScalar)(nil)
	_ Scalar = (*StructScalar)(nil)
	_ Scalar = (*UnionScalar)(nil)
	_ Scalar = (*DictionaryScalar)(nil)
	_ Scalar = (*ExtensionScalar)(nil)
)
