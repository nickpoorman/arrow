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
	DataType() arrow.DataType
	IsValid() bool
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
	if dataType == nil {
		dataType = arrow.Null
	}
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

func (s NullScalar) DataType() arrow.DataType {
	return s.dataType
}

func (s NullScalar) IsValid() bool {
	return false
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
	if len(s.value) != len(right.value) {
		return false
	}
	for i := range right.value {
		if s.value[i].NotEquals(right.value[i]) {
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

func (s StructScalar) DataType() arrow.DataType {
	return s.dataType
}

func (s StructScalar) IsValid() bool {
	return s.isValid
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
	panic("not implemented")
	right, ok := other.(UnionScalar)
	return ok && s.value == right.value
}

func (s UnionScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s UnionScalar) ValueBytes() []byte {
	panic("not implemented")
}

func (s UnionScalar) DataType() arrow.DataType {
	return s.dataType
}

func (s UnionScalar) IsValid() bool {
	return s.isValid
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
	if !ok {
		return false
	}
	if s.value == nil && right.value == nil {
		return true
	}
	if s.value == nil || right.value == nil {
		return false
	}
	return (*s.value).Equals(*right.value)
}

func (s DictionaryScalar) NotEquals(other Scalar) bool {
	return !s.Equals(other)
}

func (s DictionaryScalar) ValueBytes() []byte {
	panic("not implemented")
}

func (s DictionaryScalar) DataType() arrow.DataType {
	return s.dataType
}

func (s DictionaryScalar) IsValid() bool {
	return s.isValid
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

func (s ExtensionScalar) DataType() arrow.DataType {
	return s.dataType
}

func (s ExtensionScalar) IsValid() bool {
	return s.isValid
}

func CheckBufferLength(t *arrow.FixedSizeBinaryType, b *memory.Buffer) error {
	if t.ByteWidth != b.Len() {
		return fmt.Errorf("buffer length %d is not compatible with %#v", b.Len(), t)
	}
	return nil
}

func ScalarEquals(left, right Scalar) (bool, error) {
	if left == nil {
		left = NewNullScalar(arrow.Null)
	}
	if right == nil {
		right = NewNullScalar(arrow.Null)
	}
	if &left == &right {
		return true, nil
	} else if !arrow.TypeEqual(left.DataType(), right.DataType()) {
		return false, nil
	} else if left.IsValid() != right.IsValid() {
		return false, nil
	} else {
		visitor := NewScalarEqualsVisitor(right)
		if err := visitor.Visit(left); err != nil {
			return false, err
		}
		return visitor.Result(), nil
	}
}

type ScalarEqualsVisitor struct {
	right  Scalar
	result bool
}

func NewScalarEqualsVisitor(right Scalar) *ScalarEqualsVisitor {
	return &ScalarEqualsVisitor{
		right:  right,
		result: false,
	}
}

func (s *ScalarEqualsVisitor) Visit(left Scalar) error {
	switch left.(type) {
	case NullScalar:
		s.result = true
	case StructScalar:
		s.result = s.right.Equals(left)
	case UnionScalar:
		return fmt.Errorf("union: %w", arrow.ArrowNYIException)
		// s.result = s.right.Equals(left)
	case DictionaryScalar:
		return fmt.Errorf("dictionary: %w", arrow.ArrowNYIException)
		// s.result = s.right.Equals(left)
	case ExtensionScalar:
		return fmt.Errorf("extension: %w", arrow.ArrowNYIException)
		// s.result = s.right.Equals(left)
	default:
		// If none of the above types matched, try the generated ones
		if found := s.visitGenerated(left); !found {
			return fmt.Errorf("ScalarEqualsVisitor Visit: unhandled type: %T", left)
		}
	}

	return nil
}

func (s *ScalarEqualsVisitor) Result() bool {
	return s.result
}

func ScalarIsNaN(s Scalar) bool {
	switch v := s.(type) {
	case Float16Scalar:
		return v.IsNaN()
	case Float32Scalar:
		return v.IsNaN()
	case Float64Scalar:
		return v.IsNaN()
	default:
		return false
	}
}

var (
	_ Scalar = (*NullScalar)(nil)
	_ Scalar = (*StructScalar)(nil)
	_ Scalar = (*UnionScalar)(nil)
	_ Scalar = (*DictionaryScalar)(nil)
	_ Scalar = (*ExtensionScalar)(nil)
)
