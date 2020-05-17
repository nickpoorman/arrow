package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/memory"
)

type Scalar interface {
	ScalarEqualityComparable
	// Returns a reader to the value. Note this has more overhead than ValueBytes()
	ValueBytes() []byte
	DataType() DataType
	IsValid() bool
	// PutValue writes the bytes for value to dst and returns the number of bytes written.
	PutValue(dst []byte) int
	ValueSize() int
}

type ScalarEqualityComparable interface {
	Equals(other Scalar) bool
	NotEquals(other Scalar) bool
}

// TODO(nickpoorman):
// Current Arrow implementation does not yet support these types.

type UnionType struct{}

func (t *UnionType) ID() Type                         { return UNION }
func (t *UnionType) Name() string                     { return "union" }
func (t *UnionType) String() string                   { return "union" }
func (t *UnionType) BuildScalar(v interface{}) Scalar { return NewUnionScalarInterface(v, t) }

type MapType struct{}

func (t *MapType) ID() Type                         { return MAP }
func (t *MapType) Name() string                     { return "union" }
func (t *MapType) String() string                   { return "union" }
func (t *MapType) BuildScalar(v interface{}) Scalar { return NewMapScalarInterface(v, t) }

type LargeBinaryType struct {
	BinaryType
}

func (t *LargeBinaryType) ID() Type       { return DURATION + 2 }
func (t *LargeBinaryType) Name() string   { return "large_binary" }
func (t *LargeBinaryType) String() string { return "large_binary" }
func (t *LargeBinaryType) binary()        {}
func (t *LargeBinaryType) BuildScalar(v interface{}) Scalar {
	return NewLargeBinaryScalarInterface(v, t)
}

type LargeListType struct{}

func (t *LargeListType) ID() Type                         { return DURATION + 3 }
func (t *LargeListType) Name() string                     { return "union" }
func (t *LargeListType) String() string                   { return "union" }
func (t *LargeListType) BuildScalar(v interface{}) Scalar { return NewLargeListScalarInterface(v, t) }

var (
	UnsupportedTypes = struct {
		Union       DataType
		Map         DataType
		LargeBinary BinaryDataType
		LargeList   DataType
	}{
		Union:       &UnionType{},
		Map:         (*MapType)(nil),
		LargeBinary: &LargeBinaryType{},
		LargeList:   (*LargeListType)(nil),
	}
)

type NullScalar struct {
	// The type of the scalar value
	dataType DataType

	value interface{}
}

func NewNullScalar(dataType DataType) NullScalar {
	if dataType == nil {
		dataType = Null
	}
	return NullScalar{
		dataType: dataType,
		value:    nil,
	}
}

func NewNullScalarInterface(value interface{}, dataType DataType) NullScalar {
	if dataType == nil {
		dataType = Null
	}
	return NullScalar{
		dataType: dataType,
		value:    value,
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

func (s NullScalar) PutValue(dst []byte) int {
	panic("not implemented")
}

func (s NullScalar) ValueSize() int {
	panic("not implemented")
}

func (s NullScalar) DataType() DataType {
	return s.dataType
}

func (s NullScalar) IsValid() bool {
	return false
}

type StructScalar struct {
	// The type of the scalar value
	dataType DataType

	// Whether the value is valid (not null) or not
	isValid bool

	value []Scalar
}

func NewStructScalar(value []Scalar, dataType DataType, isValid bool) StructScalar {
	scalar := StructScalar{
		isValid:  isValid,
		dataType: dataType,
		value:    value,
	}
	return scalar
}

func NewStructScalarInterface(value interface{}, dataType DataType) StructScalar {
	if value == nil {
		return NewStructScalar(nil, dataType, false)
	}

	switch v := value.(type) {
	case []Scalar:
		return NewStructScalar(v, dataType, v != nil)
	default:
		panic(fmt.Sprintf("NewStructScalarInterface unsupported value type: %T", v))
	}
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

func (s StructScalar) PutValue(dst []byte) int {
	panic("not implemented")
}

func (s StructScalar) ValueSize() int {
	panic("not implemented")
}

func (s StructScalar) DataType() DataType {
	return s.dataType
}

func (s StructScalar) IsValid() bool {
	return s.isValid
}

type UnionScalar struct {
	// The type of the scalar value
	dataType DataType

	// Whether the value is valid (not null) or not
	isValid bool

	value interface{}
}

func NewUnionScalar(value interface{}, dataType DataType, isValid bool) UnionScalar {
	scalar := UnionScalar{
		isValid:  isValid,
		dataType: dataType,
		value:    value,
	}
	return scalar
}

func NewUnionScalarInterface(value interface{}, dataType DataType) UnionScalar {
	if value == nil {
		return NewUnionScalar(nil, dataType, false)
	}

	switch v := value.(type) {
	case []Scalar:
		return NewUnionScalar(v, dataType, v != nil)
	default:
		panic(fmt.Sprintf("NewUnionScalarInterface unsupported value type: %T", v))
	}
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

func (s UnionScalar) PutValue(dst []byte) int {
	panic("not implemented")
}

func (s UnionScalar) ValueSize() int {
	panic("not implemented")
}

func (s UnionScalar) DataType() DataType {
	return s.dataType
}

func (s UnionScalar) IsValid() bool {
	return s.isValid
}

type DictionaryScalar struct {
	// The type of the scalar value
	dataType DataType

	// Whether the value is valid (not null) or not
	isValid bool

	// (nickpoorman): this may need to be a slice of Scalars
	value *Scalar
}

func NewDictionaryScalar(value *Scalar, dataType DataType, isValid bool) DictionaryScalar {
	scalar := DictionaryScalar{
		isValid:  isValid,
		dataType: dataType,
		value:    value,
	}
	return scalar
}

func NewDictionaryScalarInterface(value interface{}, dataType DataType) DictionaryScalar {
	if value == nil {
		return NewDictionaryScalar(nil, dataType, false)
	}

	switch v := value.(type) {
	case *Scalar:
		return NewDictionaryScalar(v, dataType, v != nil)
	default:
		panic(fmt.Sprintf("NewDictionaryScalarInterface unsupported value type: %T", v))
	}
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

func (s DictionaryScalar) PutValue(dst []byte) int {
	panic("not implemented")
}

func (s DictionaryScalar) ValueSize() int {
	panic("not implemented")
}

func (s DictionaryScalar) DataType() DataType {
	return s.dataType
}

func (s DictionaryScalar) IsValid() bool {
	return s.isValid
}

type ExtensionScalar struct {
	// The type of the scalar value
	dataType DataType

	// Whether the value is valid (not null) or not
	isValid bool

	value Scalar
}

func NewExtensionScalar(value Scalar, dataType DataType, isValid bool) ExtensionScalar {
	scalar := ExtensionScalar{
		isValid:  isValid,
		dataType: dataType,
	}
	return scalar
}

func NewExtensionScalarInterface(value interface{}, dataType DataType) ExtensionScalar {
	if value == nil {
		return NewExtensionScalar(nil, dataType, false)
	}

	switch v := value.(type) {
	case Scalar:
		return NewExtensionScalar(v, dataType, v != nil)
	default:
		panic(fmt.Sprintf("NewExtensionScalarInterface unsupported value type: %T", v))
	}
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

func (s ExtensionScalar) PutValue(dst []byte) int {
	panic("not implemented")
}

func (s ExtensionScalar) ValueSize() int {
	panic("not implemented")
}

func (s ExtensionScalar) DataType() DataType {
	return s.dataType
}

func (s ExtensionScalar) IsValid() bool {
	return s.isValid
}

func CheckBufferLength(t *FixedSizeBinaryType, b *memory.Buffer) error {
	if t.ByteWidth != b.Len() {
		return fmt.Errorf("buffer length %d is not compatible with %#v", b.Len(), t)
	}
	return nil
}

func ScalarEquals(left, right Scalar) (bool, error) {
	if left == nil {
		left = NewNullScalar(Null)
	}
	if right == nil {
		right = NewNullScalar(Null)
	}
	if &left == &right {
		return true, nil
	} else if !TypeEqual(left.DataType(), right.DataType()) {
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
		return fmt.Errorf("union: %w", ArrowNYIException)
		// s.result = s.right.Equals(left)
	case DictionaryScalar:
		return fmt.Errorf("dictionary: %w", ArrowNYIException)
		// s.result = s.right.Equals(left)
	case ExtensionScalar:
		return fmt.Errorf("extension: %w", ArrowNYIException)
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

// ScalarCopy copies the scalar values to dst and returns the bytes written
func ScalarCopyValues(scalars []Scalar, dst []byte) int {
	offset := 0
	for _, scalar := range scalars {
		offset += scalar.PutValue(dst[offset:])
	}
	return offset
}

var (
	_ Scalar = (*NullScalar)(nil)
	_ Scalar = (*StructScalar)(nil)
	_ Scalar = (*UnionScalar)(nil)
	_ Scalar = (*DictionaryScalar)(nil)
	_ Scalar = (*ExtensionScalar)(nil)
)
