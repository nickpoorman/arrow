package parquet

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
	"github.com/xitongsys/parquet-go/parquet"
)

// ----------------------------------------------------------------------
// Factory Helpers

func newPrimitive(name string, repetition parquet.FieldRepetitionType, t parquet.Type, id int) *parquet.SchemaElement {
	elt := parquet.NewSchemaElement()
	elt.Name = name
	elt.RepetitionType = &repetition
	elt.Type = &t

	return elt
}

func newGroup(name string, repetition parquet.FieldRepetitionType, numChilderen int, id int) *parquet.SchemaElement {
	elt := parquet.NewSchemaElement()
	elt.Name = name
	elt.RepetitionType = &repetition
	nc := int32(numChilderen)
	elt.NumChildren = &nc

	return elt
}

func convertToPrimitiveNode(t *testing.T, element *parquet.SchemaElement) *PrimitiveNode {
	t.Helper()
	node, err := NewPrimitiveNodeFromParquet(element, 0)
	if err != nil {
		t.Errorf("NewPrimitiveNodeFromParquet error: %w", err)
	}
	testutil.AssertTrue(t, node.isPrimitive())
	return node.(*PrimitiveNode)
}

func newPrimitiveTestNode(t *testing.T, name string, repetitionType RepetitionType, primitiveType Type) *PrimitiveNode {
	t.Helper()
	return newCheckedPrimitiveTestNode(t, name, repetitionType, primitiveType, ConvertedType_NONE, -1, -1, -1)
}

func newCheckedPrimitiveTestNode(t *testing.T, name string, repetitionType RepetitionType,
	primitiveType Type, convertedType ConvertedType,
	length int, precision int, scale int) *PrimitiveNode {

	t.Helper()
	pn, err := NewPrimitiveNodeFromConvertedType(
		name,
		repetitionType,
		primitiveType,
		convertedType,
		length, precision, scale,
		-1, // id
	)
	if err != nil {
		t.Error(err)
	}

	return pn
}

func convertedTypePtr(ct parquet.ConvertedType) *parquet.ConvertedType {
	return &ct
}

func booleanTestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryBoolean(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func int32TestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryInt32(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func int64TestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryInt64(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func int96TestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryInt96(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func floatTestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryFloat(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func doubleTestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryDouble(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func byteArrayTestFactory(t *testing.T, name string, repetition RepetitionType) Node {
	n, err := PrimitiveFactoryByteArray(name, repetition)
	testutil.AssertNil(t, err)
	return n
}

func newIntLogicalType(t *testing.T, bitWidth int, isSigned bool) LogicalType {
	t.Helper()
	l, err := NewIntLogicalType(bitWidth, isSigned)
	testutil.AssertNil(t, err)
	return l
}

func newTimeLogicalType(t *testing.T,
	isAdjustedToUTC bool, timeUnit LogicalTypeTimeUnit) TimeLogicalType {
	t.Helper()
	l, err := NewTimeLogicalType(isAdjustedToUTC, timeUnit)
	testutil.AssertNil(t, err)
	return l
}

func newTimestampLogicalType(t *testing.T,
	isAdjustedToUTC bool, timeUnit LogicalTypeTimeUnit,
	isFromConvertedType bool, forceSetConvertedType bool) TimestampLogicalType {
	t.Helper()
	l, err := NewTimestampLogicalType(
		isAdjustedToUTC,
		timeUnit,
		isFromConvertedType,
		forceSetConvertedType)
	testutil.AssertNil(t, err)
	return l
}

func newDecimalLogicalType(t *testing.T,
	precision int, scale int) DecimalLogicalType {
	t.Helper()
	l, err := NewDecimalLogicalType(precision, scale)
	testutil.AssertNil(t, err)
	return l
}

// ----------------------------------------------------------------------
// Helpers

func printNode(t *testing.T, node Node) string {
	t.Helper()
	var b strings.Builder
	testutil.AssertNil(t, PrintSchema(node, &b, 0))
	return b.String()
}

func logicalTypeToString(t *testing.T, logicalType LogicalType) string {
	str, err := logicalType.ToString()
	testutil.AssertNil(t, err)
	return str
}

func logicalTypeToJSON(t *testing.T, logicalType LogicalType) string {
	str, err := logicalType.ToJSON()
	testutil.AssertNil(t, err)
	return str
}

// ----------------------------------------------------------------------
// ColumnPath

func TestColumnPath(t *testing.T) {
	var path ColumnPath = []string{"toplevel", "leaf"}
	testutil.AssertEqString(t, path.ToDotString(), "toplevel.leaf")

	pathPtr := NewColumnPathFromDotString("toplevel.leaf")
	testutil.AssertEqString(t, pathPtr.ToDotString(), "toplevel.leaf")

	extended := pathPtr.extend("anotherlevel")
	testutil.AssertEqString(t, extended.ToDotString(), "toplevel.leaf.anotherlevel")
}

// ----------------------------------------------------------------------
// Primitive node

func TestPrimitiveNodeAttrs(t *testing.T) {
	node1 := &PrimitiveNode{
		node:            *newNodeFromConvertedType(NodeType_PRIMITIVE, "foo", RepetitionType_REPEATED, ConvertedType_NONE, -1),
		PhysicalType:    Type_INT32,
		TypeLength:      -1,
		DecimalMetadata: DecimalMetadata{false, -1, -1},
	}

	node2 := &PrimitiveNode{
		node:            *newNodeFromConvertedType(NodeType_PRIMITIVE, "bar", RepetitionType_OPTIONAL, ConvertedType_UTF8, -1),
		PhysicalType:    Type_BYTE_ARRAY,
		TypeLength:      -1,
		DecimalMetadata: DecimalMetadata{false, -1, -1},
	}

	testutil.AssertEqString(t, node1.Name(), "foo")

	testutil.AssertTrue(t, node1.isPrimitive())
	testutil.AssertFalse(t, node1.isGroup())

	if node1.RepetitionType() != RepetitionType_REPEATED {
		t.Errorf("got=%d; want=%d\n", node1.RepetitionType(), RepetitionType_REPEATED)
	}
	if node2.RepetitionType() != RepetitionType_OPTIONAL {
		t.Errorf("got=%d; want=%d\n", node2.RepetitionType(), RepetitionType_OPTIONAL)
	}

	if node1.NodeType() != NodeType_PRIMITIVE {
		t.Errorf("got=%d; want=%d\n", node1.NodeType(), NodeType_PRIMITIVE)
	}

	if node1.PhysicalType != Type_INT32 {
		t.Errorf("got=%d; want=%d\n", node1.PhysicalType, Type_INT32)
	}
	if node2.PhysicalType != Type_BYTE_ARRAY {
		t.Errorf("got=%d; want=%d\n", node2.PhysicalType, Type_BYTE_ARRAY)
	}

	// logical types
	if node1.ConvertedType() != ConvertedType_NONE {
		t.Errorf("got=%d; want=%d\n", node1.ConvertedType(), ConvertedType_NONE)
	}
	if node2.ConvertedType() != ConvertedType_UTF8 {
		t.Errorf("got=%d; want=%d\n", node2.ConvertedType(), ConvertedType_UTF8)
	}

	// repetition
	node3 := &PrimitiveNode{
		node:            *newNodeFromConvertedType(NodeType_PRIMITIVE, "foo", RepetitionType_REPEATED, ConvertedType_NONE, -1),
		PhysicalType:    Type_INT32,
		TypeLength:      -1,
		DecimalMetadata: DecimalMetadata{false, -1, -1},
	}
	node4 := &PrimitiveNode{
		node:            *newNodeFromConvertedType(NodeType_PRIMITIVE, "foo", RepetitionType_REQUIRED, ConvertedType_NONE, -1),
		PhysicalType:    Type_INT32,
		TypeLength:      -1,
		DecimalMetadata: DecimalMetadata{false, -1, -1},
	}
	node5 := &PrimitiveNode{
		node:            *newNodeFromConvertedType(NodeType_PRIMITIVE, "foo", RepetitionType_OPTIONAL, ConvertedType_NONE, -1),
		PhysicalType:    Type_INT32,
		TypeLength:      -1,
		DecimalMetadata: DecimalMetadata{false, -1, -1},
	}

	testutil.AssertTrue(t, node3.isRepeated())
	testutil.AssertFalse(t, node3.isOptional())

	testutil.AssertTrue(t, node4.isRequired())

	testutil.AssertTrue(t, node5.isOptional())
	testutil.AssertFalse(t, node5.isRequired())
}

func TestPrimitiveNodeFromParquet(t *testing.T) {
	name := "name"
	id := 0

	elt := newPrimitive(name, parquet.FieldRepetitionType_OPTIONAL, parquet.Type_INT32, 0)
	primNode := convertToPrimitiveNode(t, elt)

	testutil.AssertEqString(t, name, primNode.Name())
	testutil.AssertEqInt(t, primNode.id, id)
	if RepetitionType_OPTIONAL != primNode.RepetitionType() {
		t.Errorf("got=%d; want=%d\n", primNode.RepetitionType(), RepetitionType_OPTIONAL)
	}
	if Type_INT32 != primNode.PhysicalType {
		t.Errorf("got=%d; want=%d\n", primNode.PhysicalType, Type_INT32)
	}
	if ConvertedType_NONE != primNode.ConvertedType() {
		t.Errorf("got=%d; want=%d\n", primNode.ConvertedType(), ConvertedType_NONE)
	}

	// Test a logical type
	elt = newPrimitive(name, parquet.FieldRepetitionType_REQUIRED, parquet.Type_BYTE_ARRAY, 0)
	elt.ConvertedType = convertedTypePtr(parquet.ConvertedType_UTF8)

	primNode = convertToPrimitiveNode(t, elt)
	if RepetitionType_REQUIRED != primNode.RepetitionType() {
		t.Errorf("got=%d; want=%d\n", primNode.RepetitionType(), RepetitionType_REQUIRED)
	}
	if Type_BYTE_ARRAY != primNode.PhysicalType {
		t.Errorf("got=%d; want=%d\n", primNode.PhysicalType, Type_INT32)
	}
	if ConvertedType_UTF8 != primNode.ConvertedType() {
		t.Errorf("got=%d; want=%d\n", primNode.ConvertedType(), ConvertedType_UTF8)
	}

	// FIXED_LEN_BYTE_ARRAY
	elt = newPrimitive(name, parquet.FieldRepetitionType_OPTIONAL, parquet.Type_FIXED_LEN_BYTE_ARRAY, 0)
	elt.TypeLength = func(tl int32) *int32 { return &tl }(16)

	primNode = convertToPrimitiveNode(t, elt)
	testutil.AssertEqString(t, name, primNode.Name())
	testutil.AssertEqInt(t, primNode.id, id)
	if RepetitionType_OPTIONAL != primNode.RepetitionType() {
		t.Errorf("got=%d; want=%d\n", primNode.RepetitionType(), RepetitionType_OPTIONAL)
	}
	if Type_FIXED_LEN_BYTE_ARRAY != primNode.PhysicalType {
		t.Errorf("got=%d; want=%d\n", primNode.PhysicalType, Type_FIXED_LEN_BYTE_ARRAY)
	}
	testutil.AssertEqInt(t, primNode.TypeLength, 16)

	// format::ConvertedType::Decimal
	elt = newPrimitive(name, parquet.FieldRepetitionType_OPTIONAL, parquet.Type_FIXED_LEN_BYTE_ARRAY, 0)
	elt.ConvertedType = convertedTypePtr(parquet.ConvertedType_DECIMAL)
	elt.TypeLength = func(tl int32) *int32 { return &tl }(6)
	elt.Scale = func(s int32) *int32 { return &s }(2)
	elt.Precision = func(p int32) *int32 { return &p }(12)

	primNode = convertToPrimitiveNode(t, elt)
	if Type_FIXED_LEN_BYTE_ARRAY != primNode.PhysicalType {
		t.Errorf("got=%d; want=%d\n", primNode.PhysicalType, Type_FIXED_LEN_BYTE_ARRAY)
	}
	if ConvertedType_DECIMAL != primNode.ConvertedType() {
		t.Errorf("got=%d; want=%d\n", primNode.ConvertedType(), ConvertedType_DECIMAL)
	}
	testutil.AssertEqInt(t, primNode.TypeLength, 6)
	testutil.AssertEqInt(t, primNode.DecimalMetadata.scale, 2)
	testutil.AssertEqInt(t, primNode.DecimalMetadata.precision, 12)
}

func TestPrimitiveNodeEquals(t *testing.T) {
	node1 := newPrimitiveTestNode(t, "foo", RepetitionType_REQUIRED, Type_INT32)
	node2 := newPrimitiveTestNode(t, "foo", RepetitionType_REQUIRED, Type_INT64)
	node3 := newPrimitiveTestNode(t, "bar", RepetitionType_REQUIRED, Type_INT32)
	node4 := newPrimitiveTestNode(t, "foo", RepetitionType_OPTIONAL, Type_INT32)
	node5 := newPrimitiveTestNode(t, "foo", RepetitionType_REQUIRED, Type_INT32)

	testutil.AssertTrue(t, node1.Equals(node1))
	testutil.AssertFalse(t, node1.Equals(node2))
	testutil.AssertFalse(t, node1.Equals(node3))
	testutil.AssertFalse(t, node1.Equals(node4))
	testutil.AssertTrue(t, node1.Equals(node5))

	flba1 := newCheckedPrimitiveTestNode(
		t,
		"foo",
		RepetitionType_REQUIRED,
		Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL,
		12, 4, 2,
	)

	flba2 := newCheckedPrimitiveTestNode(
		t,
		"foo",
		RepetitionType_REQUIRED,
		Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL,
		1, 4, 2,
	)
	flba2.TypeLength = 12

	flba3 := newCheckedPrimitiveTestNode(
		t,
		"foo",
		RepetitionType_REQUIRED,
		Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL,
		1, 4, 2,
	)
	flba3.TypeLength = 16

	flba4 := newCheckedPrimitiveTestNode(
		t,
		"foo",
		RepetitionType_REQUIRED,
		Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL,
		12, 4, 0,
	)

	flba5 := newCheckedPrimitiveTestNode(
		t,
		"foo",
		RepetitionType_REQUIRED,
		Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_NONE,
		12, 4, 0,
	)

	testutil.AssertTrue(t, flba1.Equals(flba2))
	testutil.AssertFalse(t, flba1.Equals(flba3))
	testutil.AssertFalse(t, flba1.Equals(flba4))
	testutil.AssertFalse(t, flba1.Equals(flba5))
}

func TestPrimitiveNodePhysicalLogicalMapping(t *testing.T) {
	var err error

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_INT32,
		ConvertedType_INT_32, -1, -1, -1, -1,
	)
	testutil.AssertNil(t, err)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_BYTE_ARRAY,
		ConvertedType_JSON, -1, -1, -1, -1,
	)
	testutil.AssertNil(t, err)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_INT32,
		ConvertedType_JSON, -1, -1, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_INT64,
		ConvertedType_TIMESTAMP_MILLIS, -1, -1, -1, -1,
	)
	testutil.AssertNil(t, err)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_INT32,
		ConvertedType_INT_64, -1, -1, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_BYTE_ARRAY,
		ConvertedType_INT_8, -1, -1, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_BYTE_ARRAY,
		ConvertedType_INTERVAL, -1, -1, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_ENUM, -1, -1, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_BYTE_ARRAY,
		ConvertedType_ENUM, -1, -1, -1, -1,
	)
	testutil.AssertNil(t, err)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL, 0, 2, 4, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FLOAT,
		ConvertedType_DECIMAL, 0, 2, 4, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL, 0, 4, 0, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL, 10, 0, 4, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL, 10, 4, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL, 10, 2, 4, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL, 10, 6, 4, -1,
	)
	testutil.AssertNil(t, err)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_INTERVAL, 12, -1, -1, -1,
	)
	testutil.AssertNil(t, err)

	_, err = NewPrimitiveNodeFromConvertedType(
		"foo", RepetitionType_REQUIRED, Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_INTERVAL, 10, -1, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)
}

// ----------------------------------------------------------------------
// Group node

func genGroupNodeFields1(t *testing.T) []Node {
	var fields []Node

	fields = append(fields, int32TestFactory(t, "one", RepetitionType_REQUIRED))
	fields = append(fields, int64TestFactory(t, "two", RepetitionType_OPTIONAL))
	fields = append(fields, doubleTestFactory(t, "three", RepetitionType_OPTIONAL))

	return fields
}

func genGroupNodeFields2(t *testing.T) []Node {
	// Fields with a duplicate name
	var fields []Node

	fields = append(fields, int32TestFactory(t, "duplicate", RepetitionType_REQUIRED))
	fields = append(fields, int64TestFactory(t, "unique", RepetitionType_OPTIONAL))
	fields = append(fields, doubleTestFactory(t, "duplicate", RepetitionType_OPTIONAL))

	return fields
}

func TestGroupNodeAttrs(t *testing.T) {
	fields := genGroupNodeFields1(t)

	node1, err := NewGroupNodeFromConvertedType("foo", RepetitionType_REPEATED, fields, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	node2, err := NewGroupNodeFromConvertedType("bar", RepetitionType_OPTIONAL, fields, ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)

	testutil.AssertEqString(t, node1.Name(), "foo")

	testutil.AssertTrue(t, node1.isGroup())
	testutil.AssertFalse(t, node1.isPrimitive())

	testutil.AssertEqInt(t, node1.FieldCount(), len(fields))

	testutil.AssertTrue(t, node1.isRepeated())
	testutil.AssertTrue(t, node2.isOptional())

	if RepetitionType_REPEATED != node1.RepetitionType() {
		t.Errorf("got=%d; want=%d\n", node1.RepetitionType(), RepetitionType_REPEATED)
	}
	if RepetitionType_OPTIONAL != node2.RepetitionType() {
		t.Errorf("got=%d; want=%d\n", node2.RepetitionType(), RepetitionType_OPTIONAL)
	}

	if NodeType_GROUP != node1.NodeType() {
		t.Errorf("got=%d; want=%d\n", node1.NodeType(), NodeType_GROUP)
	}

	// logical types
	if ConvertedType_NONE != node1.ConvertedType() {
		t.Errorf("got=%d; want=%d\n", node1.ConvertedType(), ConvertedType_NONE)
	}
	if ConvertedType_LIST != node2.ConvertedType() {
		t.Errorf("got=%d; want=%d\n", node2.ConvertedType(), ConvertedType_LIST)
	}
}

func TestGroupNodeEquals(t *testing.T) {
	f1 := genGroupNodeFields1(t)
	f2 := genGroupNodeFields1(t)

	group1, err := NewGroupNodeFromConvertedType("group", RepetitionType_REPEATED, f1, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	group2, err := NewGroupNodeFromConvertedType("group", RepetitionType_REPEATED, f2, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	group3, err := NewGroupNodeFromConvertedType("group2", RepetitionType_REPEATED, f2, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	// This is a reference to the new slice, so this is ok
	f2 = append(f2, floatTestFactory(t, "four", RepetitionType_OPTIONAL))
	group4, err := NewGroupNodeFromConvertedType("group", RepetitionType_REPEATED, f2, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	group5, err := NewGroupNodeFromConvertedType("group", RepetitionType_REPEATED, genGroupNodeFields1(t), ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	testutil.AssertTrue(t, group1.Equals(group1))
	testutil.AssertTrue(t, group1.Equals(group2))
	testutil.AssertFalse(t, group1.Equals(group3))

	testutil.AssertFalse(t, group1.Equals(group4))
	testutil.AssertFalse(t, group5.Equals(group4))
}

func TestGroupNodeFieldIndexByNode(t *testing.T) {
	fields := genGroupNodeFields1(t)
	group, err := NewGroupNodeFromConvertedType("group", RepetitionType_REPEATED, fields, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	for i := range fields {
		field := group.Field(i)
		testutil.AssertEqInt(t, group.FieldIndexByNode(field), i)
	}

	// Test a non field node
	nonFieldAlien := int32TestFactory(t, "alien", RepetitionType_REQUIRED)  // other name
	nonFieldFamiliar := int32TestFactory(t, "one", RepetitionType_REQUIRED) // other node

	testutil.AssertLT(t, group.FieldIndexByNode(nonFieldAlien), 0)
	testutil.AssertLT(t, group.FieldIndexByNode(nonFieldFamiliar), 0)
}

func TestGroupNodeFieldIndexByNodeDuplicateName(t *testing.T) {
	fields := genGroupNodeFields2(t)

	group, err := NewGroupNodeFromConvertedType("group", RepetitionType_REQUIRED, fields, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	for i := range fields {
		field := group.Field(i)
		testutil.AssertEqInt(t, group.FieldIndexByNode(field), i)
	}
}

// ----------------------------------------------------------------------
// Test convert group

type testSchemaConverter struct {
	t *testing.T

	name  string
	group *GroupNode
	node  Node
}

func newTestSchemaConverter(t *testing.T) *testSchemaConverter {
	return &testSchemaConverter{
		t:    t,
		name: "parquet_schema",
	}
}

func (tsc *testSchemaConverter) Convert(elements []parquet.SchemaElement, length int) error {
	tsc.t.Helper()

	converter := NewFlatSchemaConverter(elements, length)
	node, err := converter.Convert()
	if err != nil {
		return err
	}
	tsc.node = node
	testutil.AssertTrue(tsc.t, tsc.node.isGroup())
	tsc.group = tsc.node.(*GroupNode)

	return nil
}

func checkForParentConsistency(node *GroupNode) bool {
	// Each node should have the group as parent
	for i := 0; i < node.FieldCount(); i++ {
		field := node.Field(i)
		if field.Parent() != node {
			return false
		}
		if field.isGroup() {
			group := field.(*GroupNode)
			if !checkForParentConsistency(group) {
				return false
			}
		}
	}
	return true
}

func TestSchemaConverterNestedExample(t *testing.T) {
	tsc := newTestSchemaConverter(t)

	var elements []parquet.SchemaElement
	elements = append(elements, *newGroup(tsc.name, parquet.FieldRepetitionType_REPEATED, 2, 0))

	// A primitive one
	elements = append(elements, *newPrimitive("a", parquet.FieldRepetitionType_REQUIRED, parquet.Type_INT32, 1))

	// A group
	elements = append(elements, *newGroup("bag", parquet.FieldRepetitionType_OPTIONAL, 1, 2))

	// 3-level list encoding, by hand
	elt := newGroup("b", parquet.FieldRepetitionType_REPEATED, 1, 3)
	elt.ConvertedType = convertedTypePtr(parquet.ConvertedType_LIST)
	elements = append(elements, *elt)
	elements = append(elements, *newPrimitive("item", parquet.FieldRepetitionType_OPTIONAL, parquet.Type_INT64, 4))

	testutil.AssertNil(tsc.t, tsc.Convert(elements, len(elements)))

	// Construct the expected schema
	var fields []Node
	fields = append(fields, int32TestFactory(t, "a", RepetitionType_REQUIRED))

	// 3-level list encoding
	item := int64TestFactory(t, "item", RepetitionType_OPTIONAL)
	list, err := NewGroupNodeFromConvertedType("b", RepetitionType_REPEATED, []Node{item}, ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)
	bag, err := NewGroupNodeFromConvertedType("bag", RepetitionType_OPTIONAL, []Node{list}, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	fields = append(fields, bag)

	schema, err := NewGroupNodeFromConvertedType(tsc.name, RepetitionType_REPEATED, fields, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	testutil.AssertTrue(t, schema.Equals(tsc.group))

	// Check that the parent relationship in each node is consistent
	testutil.AssertNil(t, tsc.group.Parent())
	testutil.AssertTrue(t, checkForParentConsistency(tsc.group))
}

func TestSchemaConverterZeroColumns(t *testing.T) {
	// ARROW-3843
	tsc := newTestSchemaConverter(t)
	elements := []parquet.SchemaElement{*newGroup("schema", parquet.FieldRepetitionType_REPEATED, 0, 0)}
	testutil.AssertNil(t, tsc.Convert(elements, 1))
}

func TestSchemaConverterInvalidRoot(t *testing.T) {
	// According to the Parquet specification, the first element in the
	// list<SchemaElement> is a group whose children (and their descendants)
	// contain all of the rest of the flattened schema elements. If the first
	// element is not a group, it is a malformed Parquet file.

	tsc := newTestSchemaConverter(t)

	elements := make([]parquet.SchemaElement, 2)
	elements[0] = *newPrimitive("not-a-group", parquet.FieldRepetitionType_REQUIRED, parquet.Type_INT32, 0)
	testutil.AssertErrorIs(t, tsc.Convert(elements, 2), ParquetException)

	// While the Parquet spec indicates that the root group should have REPEATED
	// repetition type, some implementations may return REQUIRED or OPTIONAL
	// groups as the first element. These tests check that this is okay as a
	// practicality matter.
	elements[0] = *newGroup("not-repeated", parquet.FieldRepetitionType_REQUIRED, 1, 0)
	elements[1] = *newPrimitive("a", parquet.FieldRepetitionType_REQUIRED, parquet.Type_INT32, 1)
	testutil.AssertNil(t, tsc.Convert(elements, 2))

	elements[0] = *newGroup("not-repeated", parquet.FieldRepetitionType_OPTIONAL, 1, 0)
	testutil.AssertNil(t, tsc.Convert(elements, 2))
}

func TestSchemaConverterNotEnoughChildren(t *testing.T) {
	// Throw a ParquetException, but don't core dump or anything
	tsc := newTestSchemaConverter(t)
	var elements []parquet.SchemaElement
	elements = append(elements, *newGroup(tsc.name, parquet.FieldRepetitionType_REPEATED, 2, 0))
	testutil.AssertErrorIs(t, tsc.Convert(elements, 1), ParquetException)
}

// ----------------------------------------------------------------------
// Schema tree flatten / unflatten

type testSchemaFlatten struct {
	t *testing.T

	name     string
	elements []parquet.SchemaElement
}

func newTestSchemaFlatten(t *testing.T) *testSchemaFlatten {
	return &testSchemaFlatten{
		t:        t,
		name:     "parquet_schema",
		elements: make([]parquet.SchemaElement, 0),
	}
}

func (tsf *testSchemaFlatten) Flatten(schema *GroupNode) error {
	return ToParquet(schema, &tsf.elements)
}

func TestSchemaFlattenDecimalMetadata(t *testing.T) {
	// Checks that DecimalMetadata is only set for DecimalTypes
	tsf := newTestSchemaFlatten(t)

	node, err := NewPrimitiveNodeFromConvertedType("decimal", RepetitionType_REQUIRED, Type_INT64, ConvertedType_DECIMAL, -1, 8, 4, -1)
	testutil.AssertNil(t, err)

	group, err := NewGroupNodeFromConvertedType("group", RepetitionType_REPEATED, []Node{node}, ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)
	testutil.AssertNil(t, tsf.Flatten(group))
	testutil.AssertEqString(t, tsf.elements[1].GetName(), "decimal")
	testutil.AssertTrue(t, tsf.elements[1].IsSetPrecision())
	testutil.AssertTrue(t, tsf.elements[1].IsSetScale())

	tsf.elements = make([]parquet.SchemaElement, 0)
	// ... including those created with new logical types
	dlt, err := NewDecimalLogicalType(10, 5)
	testutil.AssertNil(t, err)
	node, err = NewPrimitiveNodeFromLogicalType(
		"decimal", RepetitionType_REQUIRED, dlt,
		Type_INT64, -1, -1,
	)
	testutil.AssertNil(t, err)
	group, err = NewGroupNodeFromLogicalType("group", RepetitionType_REPEATED, []Node{node}, NewListLogicalType(), -1)
	testutil.AssertNil(t, err)
	testutil.AssertNil(t, tsf.Flatten(group))
	testutil.AssertEqString(t, tsf.elements[1].GetName(), "decimal")
	testutil.AssertTrue(t, tsf.elements[1].IsSetPrecision())
	testutil.AssertTrue(t, tsf.elements[1].IsSetScale())

	tsf.elements = make([]parquet.SchemaElement, 0)
	// Not for integers with no logical type
	group, err = NewGroupNodeFromConvertedType(
		"group",
		RepetitionType_REPEATED,
		[]Node{int64TestFactory(t, "int64", RepetitionType_OPTIONAL)},
		ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)
	testutil.AssertNil(t, tsf.Flatten(group))
	testutil.AssertEqString(t, tsf.elements[1].GetName(), "int64")
	testutil.AssertFalse(t, tsf.elements[0].IsSetPrecision())
	testutil.AssertFalse(t, tsf.elements[0].IsSetScale())
}

func TestSchemaFlattenNestedExample(t *testing.T) {
	tsf := newTestSchemaFlatten(t)

	var elements []parquet.SchemaElement
	elements = append(elements, *newGroup(tsf.name, parquet.FieldRepetitionType_REPEATED, 2, 0))

	// A primitive one
	elements = append(elements, *newPrimitive("a", parquet.FieldRepetitionType_REQUIRED, parquet.Type_INT32, 1))

	// A group
	elements = append(elements, *newGroup("bag", parquet.FieldRepetitionType_OPTIONAL, 1, 2))

	// 3-level list encoding, by hand
	elt := *newGroup("b", parquet.FieldRepetitionType_REPEATED, 1, 3)
	elt.ConvertedType = convertedTypePtr(parquet.ConvertedType_LIST)
	ls := parquet.NewListType()
	lt := parquet.NewLogicalType()
	lt.LIST = ls
	elt.LogicalType = lt
	elements = append(elements, elt)
	elements = append(elements, *newPrimitive("item", parquet.FieldRepetitionType_OPTIONAL, parquet.Type_INT64, 4))

	// Construct the schema
	var fields []Node
	fields = append(fields, int32TestFactory(t, "a", RepetitionType_REQUIRED))

	// 3-level list encoding
	item := int64TestFactory(t, "item", RepetitionType_OPTIONAL)
	list, err := NewGroupNodeFromConvertedType("b", RepetitionType_REPEATED, []Node{item}, ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)
	bag, err := NewGroupNodeFromConvertedType("bag", RepetitionType_OPTIONAL, []Node{list}, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	fields = append(fields, bag)

	schema, err := NewGroupNodeFromConvertedType(tsf.name, RepetitionType_REPEATED, fields, ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	testutil.AssertNil(t, tsf.Flatten(schema))
	testutil.AssertEqInt(t, len(tsf.elements), len(elements))
	for i := range tsf.elements {
		AssertEqSchemaElement(t, tsf.elements[i], elements[i])
	}
}

func TestColumnDescriptorTestAttrs(t *testing.T) {
	node := newCheckedPrimitiveTestNode(
		t,
		"name",
		RepetitionType_OPTIONAL,
		Type_BYTE_ARRAY,
		ConvertedType_UTF8,
		-1, -1, -1,
	)
	descr, err := NewColumnDescriptor(node, 4, 1, nil)
	testutil.AssertNil(t, err)

	testutil.AssertEqString(t, descr.Name(), "name")
	testutil.AssertEqInt(t, int(descr.MaxDefinitionLevel), 4)
	testutil.AssertEqInt(t, int(descr.MaxRepetitionLevel), 1)

	AssertEqType(t, descr.PhysicalType(), Type_BYTE_ARRAY)

	testutil.AssertEqInt(t, descr.typeLength(), -1)

	expectedDescr := `column descriptor = {
  name: name,
  path: ,
  physical_type: BYTE_ARRAY,
  converted_type: UTF8,
  logical_type: String,
  max_definition_level: 4,
  max_repetition_level: 1,
}`
	str, err := descr.ToString()
	testutil.AssertNil(t, err)
	testutil.AssertEqString(t, str, expectedDescr)

	// Test FIXED_LEN_BYTE_ARRAY
	node = newCheckedPrimitiveTestNode(
		t,
		"name",
		RepetitionType_OPTIONAL,
		Type_FIXED_LEN_BYTE_ARRAY,
		ConvertedType_DECIMAL,
		12, 10, 4,
	)
	descr2, err := NewColumnDescriptor(node, 4, 1, nil)
	testutil.AssertNil(t, err)

	AssertEqType(t, descr2.PhysicalType(), Type_FIXED_LEN_BYTE_ARRAY)
	testutil.AssertEqInt(t, descr2.typeLength(), 12)

	expectedDescr = `column descriptor = {
  name: name,
  path: ,
  physical_type: FIXED_LEN_BYTE_ARRAY,
  converted_type: DECIMAL,
  logical_type: Decimal(precision=10, scale=4),
  max_definition_level: 4,
  max_repetition_level: 1,
  length: 12,
  precision: 10,
  scale: 4,
}`
	str2, err := descr2.ToString()
	testutil.AssertNil(t, err)
	testutil.AssertEqString(t, str2, expectedDescr)
}

func TestSchemaDescriptorInitNonGroup(t *testing.T) {
	node := newPrimitiveTestNode(
		t,
		"name",
		RepetitionType_OPTIONAL,
		Type_INT32,
	)
	_, err := NewSchemaDescriptor(node)
	testutil.AssertErrorIs(t, err, ParquetException)
}

func TestSchemaDescriptorEquals(t *testing.T) {
	inta := int32TestFactory(t, "a", RepetitionType_REQUIRED)
	intb := int64TestFactory(t, "b", RepetitionType_OPTIONAL)
	intb2 := int64TestFactory(t, "b2", RepetitionType_OPTIONAL)
	intc := byteArrayTestFactory(t, "c", RepetitionType_REPEATED)

	item1 := int64TestFactory(t, "item1", RepetitionType_REQUIRED)
	item2 := booleanTestFactory(t, "item2", RepetitionType_OPTIONAL)
	item3 := int32TestFactory(t, "item3", RepetitionType_REPEATED)

	list, err := NewGroupNodeFromConvertedType(
		"records",
		RepetitionType_REPEATED,
		[]Node{item1, item2, item3},
		ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)

	bag, err := NewGroupNodeFromConvertedType(
		"bag",
		RepetitionType_OPTIONAL,
		[]Node{list},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	bag2, err := NewGroupNodeFromConvertedType(
		"bag",
		RepetitionType_REQUIRED,
		[]Node{list},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	grn1, err := NewGroupNodeFromConvertedType(
		"schema",
		RepetitionType_REPEATED,
		[]Node{inta, intb, intc, bag},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	descr1, err := NewSchemaDescriptor(grn1)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, descr1.Equals(descr1))

	grn2, err := NewGroupNodeFromConvertedType(
		"schema",
		RepetitionType_REPEATED,
		[]Node{inta, intb, intc, bag2},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	descr2, err := NewSchemaDescriptor(grn2)
	testutil.AssertNil(t, err)
	testutil.AssertFalse(t, descr1.Equals(descr2))

	grn3, err := NewGroupNodeFromConvertedType(
		"schema",
		RepetitionType_REPEATED,
		[]Node{inta, intb2, intc, bag},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	descr3, err := NewSchemaDescriptor(grn3)
	testutil.AssertNil(t, err)
	testutil.AssertFalse(t, descr1.Equals(descr3))

	// Robust to name of parent node
	grn4, err := NewGroupNodeFromConvertedType(
		"SCHEMA",
		RepetitionType_REPEATED,
		[]Node{inta, intb, intc, bag},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	descr4, err := NewSchemaDescriptor(grn4)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, descr1.Equals(descr4))

	grn5, err := NewGroupNodeFromConvertedType(
		"schema",
		RepetitionType_REPEATED,
		[]Node{inta, intb, intc, bag, intb2},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	descr5, err := NewSchemaDescriptor(grn5)
	testutil.AssertNil(t, err)
	testutil.AssertFalse(t, descr1.Equals(descr5))

	// Different max repetition / definition levels
	col1, err := NewColumnDescriptor(inta, 5, 1, nil)
	testutil.AssertNil(t, err)
	col2, err := NewColumnDescriptor(inta, 6, 1, nil)
	testutil.AssertNil(t, err)
	col3, err := NewColumnDescriptor(inta, 5, 2, nil)
	testutil.AssertNil(t, err)

	testutil.AssertTrue(t, col1.Equals(col1))
	testutil.AssertFalse(t, col1.Equals(col2))
	testutil.AssertFalse(t, col1.Equals(col3))
}

func TestSchemaDescriptorBuildTree(t *testing.T) {
	var fields []Node
	inta := int32TestFactory(t, "a", RepetitionType_REQUIRED)
	fields = append(fields, inta)
	fields = append(fields, int64TestFactory(t, "b", RepetitionType_OPTIONAL))
	fields = append(fields, byteArrayTestFactory(t, "c", RepetitionType_REPEATED))

	// 3-level list encoding
	item1 := int64TestFactory(t, "item1", RepetitionType_REQUIRED)
	item2 := booleanTestFactory(t, "item2", RepetitionType_OPTIONAL)
	item3 := int32TestFactory(t, "item3", RepetitionType_REPEATED)
	list, err := NewGroupNodeFromConvertedType(
		"records",
		RepetitionType_REPEATED,
		[]Node{item1, item2, item3},
		ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)
	bag, err := NewGroupNodeFromConvertedType(
		"bag",
		RepetitionType_OPTIONAL,
		[]Node{list},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	fields = append(fields, bag)

	schema, err := NewGroupNodeFromConvertedType(
		"schema",
		RepetitionType_REPEATED,
		fields,
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	descr, err := NewSchemaDescriptor(schema)
	testutil.AssertNil(t, err)

	nleaves := 6

	// 6 leaves
	testutil.AssertEqInt(t, descr.numColumns(), nleaves)

	//                             mdef mrep
	// required int32 a            0    0
	// optional int64 b            1    0
	// repeated byte_array c       1    1
	// optional group bag          1    0
	//   repeated group records    2    1
	//     required int64 item1    2    1
	//     optional boolean item2  3    1
	//     repeated int32 item3    3    2
	exMaxDefLevels := [6]int16{0, 1, 1, 2, 3, 3}
	exMaxRepLevels := [6]int16{0, 0, 1, 1, 1, 2}

	for i := 0; i < nleaves; i++ {
		col := descr.Column(i)
		testutil.AssertEqInt(t, int(col.MaxDefinitionLevel), int(exMaxDefLevels[i]))
		if col.MaxDefinitionLevel != exMaxDefLevels[i] {
			t.Errorf("[%d]: got=%d; want=%d\n", i, col.MaxDefinitionLevel, exMaxDefLevels[i])
		}
		if col.MaxRepetitionLevel != exMaxRepLevels[i] {
			t.Errorf("[%d]: got=%d; want=%d\n", i, col.MaxRepetitionLevel, exMaxRepLevels[i])
		}
	}

	testutil.AssertEqString(t, descr.Column(0).path().ToDotString(), "a")
	testutil.AssertEqString(t, descr.Column(1).path().ToDotString(), "b")
	testutil.AssertEqString(t, descr.Column(2).path().ToDotString(), "c")
	testutil.AssertEqString(t, descr.Column(3).path().ToDotString(), "bag.records.item1")
	testutil.AssertEqString(t, descr.Column(4).path().ToDotString(), "bag.records.item2")
	testutil.AssertEqString(t, descr.Column(5).path().ToDotString(), "bag.records.item3")

	for i := 0; i < nleaves; i++ {
		col := descr.Column(i)
		testutil.AssertEqInt(t, descr.GetColumnIndexByNode(col.PrimitiveNode), i)
	}

	// Test non-column nodes find
	nonColumnAlien := int32TestFactory(t, "alien", RepetitionType_REQUIRED) // other path
	nonColumnFamiliar := int32TestFactory(t, "a", RepetitionType_REPEATED)  // other node
	testutil.AssertLT(t, descr.GetColumnIndexByNode(nonColumnAlien.(*PrimitiveNode)), 0)
	testutil.AssertLT(t, descr.GetColumnIndexByNode(nonColumnFamiliar.(*PrimitiveNode)), 0)

	testutil.AssertDeepEq(t, descr.GetColumnRoot(0), inta)
	testutil.AssertDeepEq(t, descr.GetColumnRoot(3), bag)
	testutil.AssertDeepEq(t, descr.GetColumnRoot(4), bag)
	testutil.AssertDeepEq(t, descr.GetColumnRoot(5), bag)

	testutil.AssertDeepEq(t, descr.GroupNode, schema)

	// Init clears the leaves
	oldPtr := fmt.Sprintf("%p", descr.Leaves)
	newPtr := fmt.Sprintf("%p", descr.Leaves)
	if oldPtr != newPtr {
		t.Errorf("Expected Leaves to be equal: got=%s; want=%s\n", oldPtr, newPtr)
	}
	testutil.AssertNil(t, descr.Init(schema)) // now clear leaves
	testutil.AssertEqInt(t, descr.numColumns(), nleaves)
	newPtr = fmt.Sprintf("%p", descr.Leaves)
	// check the Leaves was re-allocated
	if oldPtr == newPtr {
		t.Errorf("Expected Leaves to be reallocated: got=%s; want=%s\n", oldPtr, newPtr)
	}
}

func TestSchemaPrinterExamples(t *testing.T) {
	// Test schema 1
	var fields []Node
	fields = append(fields, int32TestFactory(t, "a", RepetitionType_REQUIRED))

	// 3-level list encoding
	item1 := int64TestFactory(t, "item1", RepetitionType_OPTIONAL)
	item2 := booleanTestFactory(t, "item2", RepetitionType_REQUIRED)
	list, err := NewGroupNodeFromConvertedType(
		"b",
		RepetitionType_REPEATED,
		[]Node{item1, item2},
		ConvertedType_LIST, -1)
	testutil.AssertNil(t, err)
	bag, err := NewGroupNodeFromConvertedType(
		"bag",
		RepetitionType_OPTIONAL,
		[]Node{list},
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)
	fields = append(fields, bag)

	primC, err := NewPrimitiveNodeFromConvertedType(
		"c", RepetitionType_REQUIRED, Type_INT32,
		ConvertedType_DECIMAL, -1, 3, 2, -1,
	)
	testutil.AssertNil(t, err)
	fields = append(fields, primC)

	decD, err := NewDecimalLogicalType(10, 5)
	testutil.AssertNil(t, err)
	primD, err := NewPrimitiveNodeFromLogicalType(
		"d", RepetitionType_REQUIRED, decD,
		Type_INT64, -1, -1,
	)
	testutil.AssertNil(t, err)
	fields = append(fields, primD)

	schema, err := NewGroupNodeFromConvertedType(
		"schema",
		RepetitionType_REPEATED,
		fields,
		ConvertedType_NONE, -1)
	testutil.AssertNil(t, err)

	result := printNode(t, schema)
	expected := `message schema {
  required int32 a;
  optional group bag {
    repeated group b (List) {
      optional int64 item1;
      required boolean item2;
    }
  }
  required int32 c (Decimal(precision=3, scale=2));
  required int64 d (Decimal(precision=10, scale=5));
}
`
	testutil.AssertEqString(t, result, expected)
}

func confirmFactoryequivalence(
	t *testing.T,
	convertedType ConvertedType,
	fromMake LogicalType,
	checkIsType func(LogicalType) bool,
) {
	t.Helper()

	fromConvertedType, err := NewLogicalTypeFromConvertedType(convertedType, DecimalMetadata{})
	testutil.AssertNil(t, err)
	if fromMake.Kind() != fromConvertedType.Kind() {
		str, err := fromMake.ToString()
		testutil.AssertNil(t, err)
		t.Errorf(
			"got=%v; want=%v %s logical types unexpectedly do not match on type\n",
			fromMake.Kind(), fromConvertedType.Kind(), str)
	}
	testutil.AssertTrueM(t, fromConvertedType.Equals(fromMake),
		fmt.Sprintf(
			"%s logical types unexpectedly not equivalent",
			logicalTypeToString(t, fromMake)),
	)
	testutil.AssertTrueM(t, checkIsType(fromConvertedType),
		fmt.Sprintf(
			"%s logical type (from converted type) does not have expected type property",
			logicalTypeToString(t, fromConvertedType)),
	)
	testutil.AssertTrueM(t, checkIsType(fromMake),
		fmt.Sprintf(
			"%s logical type (from Make() or New()) does not have expected type property",
			logicalTypeToString(t, fromMake)),
	)
}

func TestLogicalTypeConstructionFactoryEquivalence(t *testing.T) {
	// For each legacy converted type, ensure that the equivalent logical type
	// object can be obtained from either the base class's FromConvertedType()
	// factory method or the logical type type class's New() or Make() method
	// (accessed via convenience methods on the base class) and that these
	// logical type objects are equivalent

	checkIsString := func(logicalType LogicalType) bool { return logicalType.isString() }
	checkIsMap := func(logicalType LogicalType) bool { return logicalType.isMap() }
	checkIsList := func(logicalType LogicalType) bool { return logicalType.isList() }
	checkIsEnum := func(logicalType LogicalType) bool { return logicalType.isEnum() }
	checkIsDate := func(logicalType LogicalType) bool { return logicalType.isDate() }
	checkIsTime := func(logicalType LogicalType) bool { return logicalType.isTime() }
	checkIsTimestamp := func(logicalType LogicalType) bool { return logicalType.isTimestamp() }
	checkIsInt := func(logicalType LogicalType) bool { return logicalType.isInt() }
	checkIsJSON := func(logicalType LogicalType) bool { return logicalType.isJSON() }
	checkIsBSON := func(logicalType LogicalType) bool { return logicalType.isBSON() }
	checkIsInterval := func(logicalType LogicalType) bool { return logicalType.isInterval() }
	checkIsNone := func(logicalType LogicalType) bool { return logicalType.isNone() }

	cases := []struct {
		convertedType ConvertedType
		logicalType   LogicalType
		checkIsType   func(LogicalType) bool
	}{
		{ConvertedType_UTF8, NewStringLogicalType(), checkIsString},
		{ConvertedType_MAP, NewMapLogicalType(), checkIsMap},
		{ConvertedType_MAP_KEY_VALUE, NewMapLogicalType(), checkIsMap},
		{ConvertedType_LIST, NewListLogicalType(), checkIsList},
		{ConvertedType_ENUM, NewEnumLogicalType(), checkIsEnum},
		{ConvertedType_DATE, NewDateLogicalType(), checkIsDate},
		{ConvertedType_TIME_MILLIS, newTimeLogicalType(t, true,
			LogicalTypeTimeUnit_MILLIS), checkIsTime},
		{ConvertedType_TIME_MICROS, newTimeLogicalType(t, true,
			LogicalTypeTimeUnit_MICROS), checkIsTime},
		{ConvertedType_TIMESTAMP_MILLIS, newTimestampLogicalType(t, true,
			LogicalTypeTimeUnit_MILLIS, false, false), checkIsTimestamp},
		{ConvertedType_TIMESTAMP_MICROS, newTimestampLogicalType(t, true,
			LogicalTypeTimeUnit_MICROS, false, false), checkIsTimestamp},
		{ConvertedType_UINT_8, newIntLogicalType(t, 8, false), checkIsInt},
		{ConvertedType_UINT_16, newIntLogicalType(t, 16, false), checkIsInt},
		{ConvertedType_UINT_32, newIntLogicalType(t, 32, false), checkIsInt},
		{ConvertedType_UINT_64, newIntLogicalType(t, 64, false), checkIsInt},
		{ConvertedType_INT_8, newIntLogicalType(t, 8, true), checkIsInt},
		{ConvertedType_INT_16, newIntLogicalType(t, 16, true), checkIsInt},
		{ConvertedType_INT_32, newIntLogicalType(t, 32, true), checkIsInt},
		{ConvertedType_INT_64, newIntLogicalType(t, 64, true), checkIsInt},
		{ConvertedType_JSON, NewJSONLogicalType(), checkIsJSON},
		{ConvertedType_BSON, NewBSONLogicalType(), checkIsBSON},
		{ConvertedType_INTERVAL, NewIntervalLogicalType(), checkIsInterval},
		{ConvertedType_NONE, NewNoLogicalType(), checkIsNone},
	}

	for _, c := range cases {
		confirmFactoryequivalence(
			t, c.convertedType, c.logicalType, c.checkIsType)
	}

	// ConvertedType::DECIMAL, LogicalType::Decimal, is_decimal
	convertedDecimalMetadata := DecimalMetadata{
		isSet:     true,
		precision: 10,
		scale:     4,
	}
	fromConvertedType, err := NewLogicalTypeFromConvertedType(
		ConvertedType_DECIMAL, convertedDecimalMetadata)
	testutil.AssertNil(t, err)

	fromMake, err := NewDecimalLogicalType(10, 4)
	testutil.AssertNil(t, err)
	testutil.AssertDeepEq(t, fromMake.Kind(), fromConvertedType.Kind())
	testutil.AssertTrue(t, fromConvertedType.Equals(fromMake))
	testutil.AssertTrue(t, fromConvertedType.isDecimal())
	testutil.AssertTrue(t, fromMake.isDecimal())

	d1, err := NewDecimalLogicalType(16, 0)
	testutil.AssertNil(t, err)
	d2, err := NewDecimalLogicalType(16, 0)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, d1.Equals(d2))
}

func confirmConvertedTypeCompatibility(
	t *testing.T,
	original LogicalType,
	expectedConvertedType ConvertedType,
) {
	t.Helper()

	testutil.AssertTrueM(t, original.isValid(),
		fmt.Sprintf(
			"%s logical type unexpectedly is not valid",
			logicalTypeToString(t, original)),
	)

	convertedDecimalMetadata := DecimalMetadata{}
	convertedType := original.ToConvertedType(&convertedDecimalMetadata)
	testutil.AssertDeepEqM(t, convertedType, expectedConvertedType,
		fmt.Sprintf(
			"%s logical type unexpectedly returns incorrect converted type",
			logicalTypeToString(t, original)),
	)
	testutil.AssertFalseM(t, convertedDecimalMetadata.isSet,
		fmt.Sprintf(
			"%s logical type unexpectedly returns converted decimal metatdata that is set",
			logicalTypeToString(t, original)),
	)
	testutil.AssertTrueM(t, original.isCompatible(convertedType, convertedDecimalMetadata),
		fmt.Sprintf(
			"%s logical type unexpectedly is incompatible with converted type and decimal metadata it returned",
			logicalTypeToString(t, original)),
	)
	testutil.AssertFalseM(t, original.isCompatible(convertedType, DecimalMetadata{true, 1, 1}),
		fmt.Sprintf(
			"%s logical type unexpectedly is compatible with converted decimal metadata that is set",
			logicalTypeToString(t, original)),
	)
	testutil.AssertTrueM(t, original.isCompatible(convertedType, DecimalMetadata{}),
		fmt.Sprintf(
			"%s logical type unexpectedly is incompatible with converted type it returned",
			logicalTypeToString(t, original)),
	)
	reconstructed, err := NewLogicalTypeFromConvertedType(convertedType, convertedDecimalMetadata)
	testutil.AssertNil(t, err)
	testutil.AssertTrueM(t, reconstructed.isValid(),
		fmt.Sprintf(
			"Reconstructed %s logical type unexpectedly is not valid",
			logicalTypeToString(t, reconstructed)),
	)
	testutil.AssertTrueM(t, reconstructed.Equals(original),
		fmt.Sprintf(
			"Reconstructed logical type (%s) unexpectedly not equivalent to original logical type (%s)",
			logicalTypeToString(t, reconstructed), logicalTypeToString(t, original)),
	)
}

func TestLogicalTypeConstructionConvertedTypeCompatibility(t *testing.T) {
	// For each legacy converted type, ensure that the equivalent logical type
	// emits correct, compatible converted type information and that the emitted
	// information can be used to reconstruct another equivalent logical type.

	cases := []struct {
		logicalType   LogicalType
		convertedType ConvertedType
	}{
		{NewStringLogicalType(), ConvertedType_UTF8},
		{NewMapLogicalType(), ConvertedType_MAP},
		{NewListLogicalType(), ConvertedType_LIST},
		{NewEnumLogicalType(), ConvertedType_ENUM},
		{NewDateLogicalType(), ConvertedType_DATE},
		{newTimeLogicalType(t, true,
			LogicalTypeTimeUnit_MILLIS), ConvertedType_TIME_MILLIS},
		{newTimeLogicalType(t, true,
			LogicalTypeTimeUnit_MICROS), ConvertedType_TIME_MICROS},
		{newTimestampLogicalType(t, true,
			LogicalTypeTimeUnit_MILLIS, false, false), ConvertedType_TIMESTAMP_MILLIS},
		{newTimestampLogicalType(t, true,
			LogicalTypeTimeUnit_MICROS, false, false), ConvertedType_TIMESTAMP_MICROS},
		{newIntLogicalType(t, 8, false), ConvertedType_UINT_8},
		{newIntLogicalType(t, 16, false), ConvertedType_UINT_16},
		{newIntLogicalType(t, 32, false), ConvertedType_UINT_32},
		{newIntLogicalType(t, 64, false), ConvertedType_UINT_64},
		{newIntLogicalType(t, 8, true), ConvertedType_INT_8},
		{newIntLogicalType(t, 16, true), ConvertedType_INT_16},
		{newIntLogicalType(t, 32, true), ConvertedType_INT_32},
		{newIntLogicalType(t, 64, true), ConvertedType_INT_64},
		{NewJSONLogicalType(), ConvertedType_JSON},
		{NewBSONLogicalType(), ConvertedType_BSON},
		{NewIntervalLogicalType(), ConvertedType_INTERVAL},
		{NewNoLogicalType(), ConvertedType_NONE},
	}

	for _, c := range cases {
		confirmConvertedTypeCompatibility(t, c.logicalType, c.convertedType)
	}

	// Special cases ...
	var original LogicalType
	var convertedType ConvertedType
	var convertedDecimalMetadata DecimalMetadata

	// DECIMAL
	original, err := NewDecimalLogicalType(6, 2)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, original.isValid())
	convertedType = original.ToConvertedType(&convertedDecimalMetadata)
	testutil.AssertDeepEq(t, convertedType, ConvertedType_DECIMAL)
	testutil.AssertTrue(t, convertedDecimalMetadata.isSet)
	testutil.AssertDeepEq(t, convertedDecimalMetadata.precision, 6)
	testutil.AssertDeepEq(t, convertedDecimalMetadata.scale, 2)
	testutil.AssertTrue(t, original.isCompatible(convertedType, convertedDecimalMetadata))
	reconstructed, err := NewLogicalTypeFromConvertedType(convertedType, convertedDecimalMetadata)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, reconstructed.isValid())
	testutil.AssertTrue(t, reconstructed.Equals(original))

	// Unknown
	original = NewUnknownLogicalType()
	testutil.AssertTrue(t, original.isInvalid())
	testutil.AssertFalse(t, original.isValid())
	convertedType = original.ToConvertedType(&convertedDecimalMetadata)
	testutil.AssertDeepEq(t, convertedType, ConvertedType_NA)
	testutil.AssertFalse(t, convertedDecimalMetadata.isSet)
	testutil.AssertTrue(t, original.isCompatible(convertedType, convertedDecimalMetadata))
	testutil.AssertTrue(t, original.isCompatible(convertedType, DecimalMetadata{}))
	reconstructed, err = NewLogicalTypeFromConvertedType(convertedType, convertedDecimalMetadata)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, reconstructed.isInvalid())
	testutil.AssertTrue(t, reconstructed.Equals(original))
}

func confirmNewTypeIncompatibility(
	t *testing.T,
	logicalType LogicalType,
	checkIsType func(LogicalType) bool,
) {
	t.Helper()

	testutil.AssertTrueM(t, logicalType.isValid(),
		fmt.Sprintf(
			"%s logical type unexpectedly is not valid",
			logicalTypeToString(t, logicalType)),
	)
	testutil.AssertTrueM(t, checkIsType(logicalType),
		fmt.Sprintf(
			"%s logical type is not expected logical type",
			logicalTypeToString(t, logicalType)),
	)

	var convertedDecimalMetadata DecimalMetadata
	convertedType := logicalType.ToConvertedType(&convertedDecimalMetadata)
	if convertedType != ConvertedType_NONE {
		str, err := logicalType.ToString()
		testutil.AssertNil(t, err)
		t.Errorf(
			"not equal: %s logical type converted type unexpectedly is not NONE\n",
			str)
	}
	testutil.AssertFalseM(t, convertedDecimalMetadata.isSet,
		fmt.Sprintf(
			"%s logical type converted decimal metadata unexpectedly is set",
			logicalTypeToString(t, logicalType)),
	)
}

func TestLogicalTypeConstructionNewTypeIncompatibility(t *testing.T) {
	// For each new logical type, ensure that the type
	// correctly reports that it has no legacy equivalent

	checkIsUUID := func(lt LogicalType) bool { return lt.isUUID() }
	checkIsNull := func(lt LogicalType) bool { return lt.isNull() }
	checkIsTime := func(lt LogicalType) bool { return lt.isTime() }
	checkIsTimestamp := func(lt LogicalType) bool { return lt.isTimestamp() }

	cases := []struct {
		logicalType LogicalType
		checkIsType func(LogicalType) bool
	}{
		{NewUUIDLogicalType(), checkIsUUID},
		{NewNullLogicalType(), checkIsNull},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MILLIS), checkIsTime},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MICROS), checkIsTime},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_NANOS), checkIsTime},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS), checkIsTime},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_NANOS, false, false), checkIsTimestamp},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_NANOS, false, false), checkIsTimestamp},
	}

	for _, c := range cases {
		confirmNewTypeIncompatibility(t, c.logicalType, c.checkIsType)
	}
}

func TestLogicalTypeConstructionFactoryExceptions(t *testing.T) {
	// Ensure that logical type construction catches invalid arguments
	cases := []struct {
		f func() error
	}{
		{func() error {
			// Invalid TimeUnit
			_, err := NewTimeLogicalType(true, LogicalTypeTimeUnit_UNKNOWN)
			return err
		}},
		{func() error {
			// Invalid TimeUnit
			_, err := NewTimestampLogicalType(
				true, LogicalTypeTimeUnit_UNKNOWN, false, false)
			return err
		}},
		{func() error {
			// Invalid bit width
			_, err := NewIntLogicalType(-1, false)
			return err
		}},
		{func() error {
			// Invalid bit width
			_, err := NewIntLogicalType(0, false)
			return err
		}},
		{func() error {
			// Invalid bit width
			_, err := NewIntLogicalType(1, false)
			return err
		}},
		{func() error {
			// Invalid bit width
			_, err := NewIntLogicalType(65, false)
			return err
		}},
		{func() error {
			// Invalid precision
			_, err := NewDecimalLogicalType(-1, 0)
			return err
		}},
		{func() error {
			// Invalid precision
			_, err := NewDecimalLogicalType(0, 0)
			return err
		}},
		{func() error {
			// Invalid scale
			_, err := NewDecimalLogicalType(10, -1)
			return err
		}},
		{func() error {
			// Invalid scale
			_, err := NewDecimalLogicalType(10, 11)
			return err
		}},
	}

	for _, c := range cases {
		testutil.AssertErrorIs(t, c.f(), ParquetException)
	}
}

func confirmLogicalTypeProperties(
	t *testing.T,
	logicalType LogicalType,
	nested bool,
	serialized bool,
	valid bool,
) {
	t.Helper()

	testutil.AssertTrueM(t, logicalType.isNested() == nested,
		fmt.Sprintf(
			"%s logical type has incorrect nested() property",
			logicalTypeToString(t, logicalType)),
	)
	testutil.AssertTrueM(t, logicalType.isSerialized() == serialized,
		fmt.Sprintf(
			"%s logical type has incorrect serialized() property",
			logicalTypeToString(t, logicalType)),
	)
	testutil.AssertTrueM(t, logicalType.isValid() == valid,
		fmt.Sprintf(
			"%s logical type has incorrect valid() property",
			logicalTypeToString(t, logicalType)),
	)
	testutil.AssertTrueM(t, logicalType.isNonnested() != nested,
		fmt.Sprintf(
			"%s logical type has incorrect nonnested() property",
			logicalTypeToString(t, logicalType)),
	)
	testutil.AssertTrueM(t, logicalType.isInvalid() != valid,
		fmt.Sprintf(
			"%s logical type has incorrect invalid() property",
			logicalTypeToString(t, logicalType)),
	)
}

func TestLogicalTypeOperationLogicalTypeProperties(t *testing.T) {
	// For each logical type, ensure that the correct general properties are reported
	cases := []struct {
		logicalType LogicalType
		nested      bool
		serialized  bool
		valid       bool
	}{
		{NewStringLogicalType(), false, true, true},
		{NewMapLogicalType(), true, true, true},
		{NewListLogicalType(), true, true, true},
		{NewEnumLogicalType(), false, true, true},
		{newDecimalLogicalType(t, 16, 6), false, true, true},
		{NewDateLogicalType(), false, true, true},
		{newTimeLogicalType(
			t, true, LogicalTypeTimeUnit_MICROS),
			false, true, true},
		{newTimestampLogicalType(
			t, true, LogicalTypeTimeUnit_MICROS, false, false),
			false, true, true},
		{NewIntervalLogicalType(), false, true, true},
		{newIntLogicalType(t, 8, false), false, true, true},
		{newIntLogicalType(t, 64, true), false, true, true},
		{NewNullLogicalType(), false, true, true},
		{NewJSONLogicalType(), false, true, true},
		{NewBSONLogicalType(), false, true, true},
		{NewUUIDLogicalType(), false, true, true},
		{NewNoLogicalType(), false, false, true},
		{NewUnknownLogicalType(), false, false, false},
	}

	for _, c := range cases {
		confirmLogicalTypeProperties(
			t, c.logicalType, c.nested, c.serialized, c.valid)
	}
}

const physicalTypeCount = 8

var physicalType = [physicalTypeCount]Type{
	Type_BOOLEAN, Type_INT32, Type_INT64, Type_INT96,
	Type_FLOAT, Type_DOUBLE, Type_BYTE_ARRAY, Type_FIXED_LEN_BYTE_ARRAY,
}

func confirmSinglePrimitiveTypeApplicability(
	t *testing.T,
	logicalType LogicalType,
	applicableType Type,
) {
	t.Helper()

	for i := 0; i < physicalTypeCount; i++ {
		if physicalType[i] == applicableType {
			testutil.AssertTrueM(t, logicalType.isApplicable(physicalType[i], -1),
				fmt.Sprintf(
					"%s logical type unexpectedly inapplicable to physical type %s",
					logicalTypeToString(t, logicalType),
					TypeToString(physicalType[i])),
			)
		} else {
			testutil.AssertFalseM(t, logicalType.isApplicable(physicalType[i], -1),
				fmt.Sprintf(
					"%s logical type unexpectedly applicable to physical type %s",
					logicalTypeToString(t, logicalType),
					TypeToString(physicalType[i])),
			)
		}
	}
}

func confirmAnyPrimitiveTypeApplicability(
	t *testing.T,
	logicalType LogicalType,
) {
	t.Helper()
	for i := 0; i < physicalTypeCount; i++ {
		testutil.AssertTrueM(t, logicalType.isApplicable(physicalType[i], -1),
			fmt.Sprintf(
				"%s logical type unexpectedly inapplicable to physical type %s",
				logicalTypeToString(t, logicalType),
				TypeToString(physicalType[i])),
		)
	}
}

func confirmNoPrimitiveTypeApplicability(
	t *testing.T,
	logicalType LogicalType,
) {
	t.Helper()
	for i := 0; i < physicalTypeCount; i++ {
		testutil.AssertFalseM(t, logicalType.isApplicable(physicalType[i], -1),
			fmt.Sprintf(
				"%s logical type unexpectedly applicable to physical type %s",
				logicalTypeToString(t, logicalType),
				TypeToString(physicalType[i])),
		)
	}
}

func TestLogicalTypeOperationLogicalTypeApplicability(t *testing.T) {
	// Check that each logical type correctly reports which
	// underlying primitive type(s) it can be applied to
	singleTypeCases := []struct {
		logicalType    LogicalType
		applicableType Type
	}{
		{NewStringLogicalType(), Type_BYTE_ARRAY},
		{NewEnumLogicalType(), Type_BYTE_ARRAY},
		{NewDateLogicalType(), Type_INT32},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MILLIS), Type_INT32},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MICROS), Type_INT64},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS), Type_INT64},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MILLIS, false, false), Type_INT64},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MICROS, false, false), Type_INT64},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_NANOS, false, false), Type_INT64},
		{newIntLogicalType(t, 8, false), Type_INT32},
		{newIntLogicalType(t, 16, false), Type_INT32},
		{newIntLogicalType(t, 32, false), Type_INT32},
		{newIntLogicalType(t, 64, false), Type_INT64},
		{newIntLogicalType(t, 8, true), Type_INT32},
		{newIntLogicalType(t, 16, true), Type_INT32},
		{newIntLogicalType(t, 32, true), Type_INT32},
		{newIntLogicalType(t, 64, true), Type_INT64},
		{NewJSONLogicalType(), Type_BYTE_ARRAY},
		{NewBSONLogicalType(), Type_BYTE_ARRAY},
	}
	for _, c := range singleTypeCases {
		confirmSinglePrimitiveTypeApplicability(t, c.logicalType, c.applicableType)
	}

	noTypeCases := []LogicalType{NewMapLogicalType(), NewListLogicalType()}
	for _, c := range noTypeCases {
		confirmNoPrimitiveTypeApplicability(t, c)
	}

	anyTypeCases := []LogicalType{
		NewNullLogicalType(), NewNoLogicalType(), NewUnknownLogicalType(),
	}
	for _, c := range anyTypeCases {
		confirmAnyPrimitiveTypeApplicability(t, c)
	}

	// Fixed binary, exact length cases ...
	inapplicableTypes := []struct {
		physicalType   Type
		physicalLength int32
	}{
		{Type_FIXED_LEN_BYTE_ARRAY, 8},
		{Type_FIXED_LEN_BYTE_ARRAY, 20},
		{Type_BOOLEAN, -1},
		{Type_INT32, -1},
		{Type_INT64, -1},
		{Type_INT96, -1},
		{Type_FLOAT, -1},
		{Type_DOUBLE, -1},
		{Type_BYTE_ARRAY, -1},
	}

	var logicalType LogicalType
	logicalType = NewIntervalLogicalType()
	testutil.AssertTrue(t, logicalType.isApplicable(Type_FIXED_LEN_BYTE_ARRAY, 12))
	for _, c := range inapplicableTypes {
		testutil.AssertFalse(t, logicalType.isApplicable(c.physicalType, c.physicalLength))
	}

	logicalType = NewUUIDLogicalType()
	testutil.AssertTrue(t, logicalType.isApplicable(Type_FIXED_LEN_BYTE_ARRAY, 16))
	for _, c := range inapplicableTypes {
		testutil.AssertFalse(t, logicalType.isApplicable(c.physicalType, c.physicalLength))
	}
}

func TestLogicalTypeOperationDecimalLogicalTypeApplicability(t *testing.T) {
	// Check that the decimal logical type correctly reports which
	// underlying primitive type(s) it can be applied to
	var logicalType LogicalType

	for precision := 1; precision <= 9; precision++ {
		logicalType = newDecimalLogicalType(t, precision, 0)
		testutil.AssertTrueM(t, logicalType.isApplicable(Type_INT32, -1),
			fmt.Sprintf(
				"%s unexpectedly inapplicable to physical type INT32",
				logicalTypeToString(t, logicalType)),
		)
	}
	logicalType = newDecimalLogicalType(t, 10, 0)
	testutil.AssertFalseM(t, logicalType.isApplicable(Type_INT32, -1),
		fmt.Sprintf(
			"%s unexpectedly applicable to physical type INT32",
			logicalTypeToString(t, logicalType)),
	)

	for precision := 1; precision <= 18; precision++ {
		logicalType = newDecimalLogicalType(t, precision, 0)
		testutil.AssertTrueM(t, logicalType.isApplicable(Type_INT64, -1),
			fmt.Sprintf(
				"%s unexpectedly inapplicable to physical type INT64",
				logicalTypeToString(t, logicalType)),
		)
	}
	logicalType = newDecimalLogicalType(t, 19, 0)
	testutil.AssertFalseM(t, logicalType.isApplicable(Type_INT64, -1),
		fmt.Sprintf(
			"%s unexpectedly applicable to physical type INT64",
			logicalTypeToString(t, logicalType)),
	)

	for precision := 1; precision <= 36; precision++ {
		logicalType = newDecimalLogicalType(t, precision, 0)
		testutil.AssertTrueM(t, logicalType.isApplicable(Type_BYTE_ARRAY, -1),
			fmt.Sprintf(
				"%s unexpectedly inapplicable to physical type BYTE_ARRAY",
				logicalTypeToString(t, logicalType)),
		)
	}

	cases := []struct {
		physicalLength int32
		precisionLimit int
	}{
		{1, 2}, {2, 4}, {3, 6}, {4, 9}, {8, 18},
		{10, 23}, {16, 38}, {20, 47}, {32, 76},
	}

	for _, c := range cases {
		var precision int
		for precision = 1; precision <= c.precisionLimit; precision++ {
			logicalType = newDecimalLogicalType(t, precision, 0)
			testutil.AssertTrueM(t,
				logicalType.isApplicable(
					Type_FIXED_LEN_BYTE_ARRAY, c.physicalLength,
				),
				fmt.Sprintf(
					"%s unexpectedly inapplicable to physical type FIXED_LEN_BYTE_ARRAY with length %d",
					logicalTypeToString(t, logicalType),
					c.physicalLength,
				),
			)
		}
		logicalType = newDecimalLogicalType(t, precision, 0)
		testutil.AssertFalseM(t,
			logicalType.isApplicable(
				Type_FIXED_LEN_BYTE_ARRAY, c.physicalLength,
			),
			fmt.Sprintf(
				"%s unexpectedly applicable to physical type FIXED_LEN_BYTE_ARRAY with length %d",
				logicalTypeToString(t, logicalType),
				c.physicalLength,
			),
		)
	}

	testutil.AssertFalse(t, newDecimalLogicalType(t, 16, 6).isApplicable(Type_BOOLEAN, -1))
	testutil.AssertFalse(t, newDecimalLogicalType(t, 16, 6).isApplicable(Type_FLOAT, -1))
	testutil.AssertFalse(t, newDecimalLogicalType(t, 16, 6).isApplicable(Type_DOUBLE, -1))
}

func TestLogicalTypeOperationLogicalTypeRepresentation(t *testing.T) {
	// Ensure that each logical type prints a correct string and
	// JSON representation

	cases := []struct {
		logicalType          LogicalType
		stringRepresentation string
		JSONRepresentation   string
	}{
		{NewUnknownLogicalType(), "Unknown", `{"Type": "Unknown"}`},
		{NewStringLogicalType(), "String", `{"Type": "String"}`},
		{NewMapLogicalType(), "Map", `{"Type": "Map"}`},
		{NewListLogicalType(), "List", `{"Type": "List"}`},
		{NewEnumLogicalType(), "Enum", `{"Type": "Enum"}`},
		{newDecimalLogicalType(t, 10, 4), "Decimal(precision=10, scale=4)",
			`{"Type": "Decimal", "precision": 10, "scale": 4}`},
		{newDecimalLogicalType(t, 10, 0), "Decimal(precision=10, scale=0)",
			`{"Type": "Decimal", "precision": 10, "scale": 0}`},
		{NewDateLogicalType(), "Date", `{"Type": "Date"}`},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MILLIS),
			"Time(isAdjustedToUTC=true, timeUnit=milliseconds)",
			`{"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "milliseconds"}`},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MICROS),
			"Time(isAdjustedToUTC=true, timeUnit=microseconds)",
			`{"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "microseconds"}`},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS),
			"Time(isAdjustedToUTC=true, timeUnit=nanoseconds)",
			`{"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "nanoseconds"}`},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MILLIS),
			"Time(isAdjustedToUTC=false, timeUnit=milliseconds)",
			`{"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "milliseconds"}`},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MICROS),
			"Time(isAdjustedToUTC=false, timeUnit=microseconds)",
			`{"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "microseconds"}`},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_NANOS),
			"Time(isAdjustedToUTC=false, timeUnit=nanoseconds)",
			`{"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "nanoseconds"}`},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MILLIS, false, false),
			"Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, " +
				"is_from_converted_type=false, force_set_converted_type=false)",
			`{"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "milliseconds", ` +
				`"is_from_converted_type": false, "force_set_converted_type": false}`},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MICROS, false, false),
			"Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, " +
				"is_from_converted_type=false, force_set_converted_type=false)",
			`{"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "microseconds", ` +
				`"is_from_converted_type": false, "force_set_converted_type": false}`},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_NANOS, false, false),
			"Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, " +
				"is_from_converted_type=false, force_set_converted_type=false)",
			`{"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "nanoseconds", ` +
				`"is_from_converted_type": false, "force_set_converted_type": false}`},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_MILLIS, true, true),
			"Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, " +
				"is_from_converted_type=true, force_set_converted_type=true)",
			`{"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "milliseconds", ` +
				`"is_from_converted_type": true, "force_set_converted_type": true}`},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_MICROS, false, false),
			"Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, " +
				"is_from_converted_type=false, force_set_converted_type=false)",
			`{"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "microseconds", ` +
				`"is_from_converted_type": false, "force_set_converted_type": false}`},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_NANOS, false, false),
			"Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, " +
				"is_from_converted_type=false, force_set_converted_type=false)",
			`{"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "nanoseconds", ` +
				`"is_from_converted_type": false, "force_set_converted_type": false}`},
		{NewIntervalLogicalType(), "Interval", `{"Type": "Interval"}`},
		{newIntLogicalType(t, 8, false), "Int(bitWidth=8, isSigned=false)",
			`{"Type": "Int", "bitWidth": 8, "isSigned": false}`},
		{newIntLogicalType(t, 16, false), "Int(bitWidth=16, isSigned=false)",
			`{"Type": "Int", "bitWidth": 16, "isSigned": false}`},
		{newIntLogicalType(t, 32, false), "Int(bitWidth=32, isSigned=false)",
			`{"Type": "Int", "bitWidth": 32, "isSigned": false}`},
		{newIntLogicalType(t, 64, false), "Int(bitWidth=64, isSigned=false)",
			`{"Type": "Int", "bitWidth": 64, "isSigned": false}`},
		{newIntLogicalType(t, 8, true), "Int(bitWidth=8, isSigned=true)",
			`{"Type": "Int", "bitWidth": 8, "isSigned": true}`},
		{newIntLogicalType(t, 16, true), "Int(bitWidth=16, isSigned=true)",
			`{"Type": "Int", "bitWidth": 16, "isSigned": true}`},
		{newIntLogicalType(t, 32, true), "Int(bitWidth=32, isSigned=true)",
			`{"Type": "Int", "bitWidth": 32, "isSigned": true}`},
		{newIntLogicalType(t, 64, true), "Int(bitWidth=64, isSigned=true)",
			`{"Type": "Int", "bitWidth": 64, "isSigned": true}`},
		{NewNullLogicalType(), "Null", `{"Type": "Null"}`},
		{NewJSONLogicalType(), "JSON", `{"Type": "JSON"}`},
		{NewBSONLogicalType(), "BSON", `{"Type": "BSON"}`},
		{NewUUIDLogicalType(), "UUID", `{"Type": "UUID"}`},
		{NewNoLogicalType(), "None", `{"Type": "None"}`},
	}

	for _, c := range cases {
		testutil.AssertEqString(t, logicalTypeToString(t, c.logicalType), c.stringRepresentation)
		testutil.AssertEqString(t, logicalTypeToJSON(t, c.logicalType), c.JSONRepresentation)
	}
}

func TestLogicalTypeOperationLogicalTypeSortOrder(t *testing.T) {
	// Ensure that each logical type reports the correct sort order

	cases := []struct {
		logicalType LogicalType
		sortOrder   SortOrder
	}{
		{NewUnknownLogicalType(), SortOrder_UNKNOWN},
		{NewStringLogicalType(), SortOrder_UNSIGNED},
		{NewMapLogicalType(), SortOrder_UNKNOWN},
		{NewListLogicalType(), SortOrder_UNKNOWN},
		{NewEnumLogicalType(), SortOrder_UNSIGNED},
		{newDecimalLogicalType(t, 8, 2), SortOrder_SIGNED},
		{NewDateLogicalType(), SortOrder_SIGNED},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MILLIS), SortOrder_SIGNED},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MICROS), SortOrder_SIGNED},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS), SortOrder_SIGNED},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MILLIS), SortOrder_SIGNED},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MICROS), SortOrder_SIGNED},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_NANOS), SortOrder_SIGNED},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MILLIS, false, false), SortOrder_SIGNED},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MICROS, false, false), SortOrder_SIGNED},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_NANOS, false, false), SortOrder_SIGNED},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_MILLIS, false, false), SortOrder_SIGNED},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_MICROS, false, false), SortOrder_SIGNED},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_NANOS, false, false), SortOrder_SIGNED},
		{NewIntervalLogicalType(), SortOrder_UNKNOWN},
		{newIntLogicalType(t, 8, false), SortOrder_UNSIGNED},
		{newIntLogicalType(t, 16, false), SortOrder_UNSIGNED},
		{newIntLogicalType(t, 32, false), SortOrder_UNSIGNED},
		{newIntLogicalType(t, 64, false), SortOrder_UNSIGNED},
		{newIntLogicalType(t, 8, true), SortOrder_SIGNED},
		{newIntLogicalType(t, 16, true), SortOrder_SIGNED},
		{newIntLogicalType(t, 32, true), SortOrder_SIGNED},
		{newIntLogicalType(t, 64, true), SortOrder_SIGNED},
		{NewNullLogicalType(), SortOrder_UNKNOWN},
		{NewJSONLogicalType(), SortOrder_UNSIGNED},
		{NewBSONLogicalType(), SortOrder_UNSIGNED},
		{NewUUIDLogicalType(), SortOrder_UNSIGNED},
		{NewNoLogicalType(), SortOrder_UNKNOWN},
	}

	for _, c := range cases {
		testutil.AssertDeepEqM(t, c.logicalType.SortOrder(), c.sortOrder,
			fmt.Sprintf("%s logical type has incorrect sort order",
				logicalTypeToString(t, c.logicalType)))
	}
}

func confirmPrimitiveNodeFactoryEquivalence(
	t *testing.T,
	logicalType LogicalType,
	convertedType ConvertedType,
	physicalType Type,
	physicalLength int,
	precision int,
	scale int,
) {
	t.Helper()

	name := "something"
	repetition := RepetitionType_REQUIRED
	fromConvertedType := newCheckedPrimitiveTestNode(
		t,
		name,
		repetition,
		physicalType,
		convertedType,
		physicalLength,
		precision,
		scale,
	)
	fromLogicalType, err := NewPrimitiveNodeFromLogicalType(
		name,
		repetition,
		logicalType,
		physicalType,
		physicalLength,
		-1,
	)
	testutil.AssertNil(t, err)
	testutil.AssertTrueM(t, fromConvertedType.Equals(fromLogicalType),
		fmt.Sprintf(
			"Primitive node constructed with converted type "+
				"%s unexpectedly not equivalent to primitive node "+
				"constructed with logical type %s",
			ConvertedTypeToString(convertedType),
			logicalTypeToString(t, logicalType),
		))
}

func confirmGroupNodeFactoryEquivalence(
	t *testing.T,
	name string,
	logicalType LogicalType,
	convertedType ConvertedType,
) {
	repetition := RepetitionType_OPTIONAL
	fromConvertedType, err := NewGroupNodeFromConvertedType(
		name, repetition, []Node{}, convertedType, -1)
	testutil.AssertNil(t, err)
	fromLogicalType, err := NewGroupNodeFromLogicalType(
		name, repetition, []Node{}, logicalType, -1)
	testutil.AssertNil(t, err)
	testutil.AssertTrueM(t, fromConvertedType.Equals(fromLogicalType),
		fmt.Sprintf(
			"Group node constructed with converted type "+
				"%s unexpectedly not equivalent to group node "+
				"constructed with logical type %s",
			ConvertedTypeToString(convertedType),
			logicalTypeToString(t, logicalType),
		),
	)
}

func TestSchemaNodeCreationFactoryEquivalence(t *testing.T) {
	// Ensure that the Node factory methods produce equivalent results
	// regardless of whether they are given a converted type or a logical type.

	// Primitive nodes ...
	cases := []struct {
		logicalType    LogicalType
		convertedType  ConvertedType
		physicalType   Type
		physicalLength int
		precision      int
		scale          int
	}{
		{NewStringLogicalType(), ConvertedType_UTF8, Type_BYTE_ARRAY, -1, -1, -1},
		{NewEnumLogicalType(), ConvertedType_ENUM, Type_BYTE_ARRAY, -1, -1, -1},
		{newDecimalLogicalType(t, 16, 6), ConvertedType_DECIMAL, Type_INT64, -1, 16, 6},
		{NewDateLogicalType(), ConvertedType_DATE, Type_INT32, -1, -1, -1},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MILLIS), ConvertedType_TIME_MILLIS,
			Type_INT32, -1, -1, -1},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MICROS), ConvertedType_TIME_MICROS,
			Type_INT64, -1, -1, -1},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MILLIS, false, false),
			ConvertedType_TIMESTAMP_MILLIS, Type_INT64, -1, -1, -1},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MICROS, false, false),
			ConvertedType_TIMESTAMP_MICROS, Type_INT64, -1, -1, -1},
		{NewIntervalLogicalType(), ConvertedType_INTERVAL, Type_FIXED_LEN_BYTE_ARRAY, 12,
			-1, -1},
		{newIntLogicalType(t, 8, false), ConvertedType_UINT_8, Type_INT32, -1, -1, -1},
		{newIntLogicalType(t, 8, true), ConvertedType_INT_8, Type_INT32, -1, -1, -1},
		{newIntLogicalType(t, 16, false), ConvertedType_UINT_16, Type_INT32, -1, -1, -1},
		{newIntLogicalType(t, 16, true), ConvertedType_INT_16, Type_INT32, -1, -1, -1},
		{newIntLogicalType(t, 32, false), ConvertedType_UINT_32, Type_INT32, -1, -1, -1},
		{newIntLogicalType(t, 32, true), ConvertedType_INT_32, Type_INT32, -1, -1, -1},
		{newIntLogicalType(t, 64, false), ConvertedType_UINT_64, Type_INT64, -1, -1, -1},
		{newIntLogicalType(t, 64, true), ConvertedType_INT_64, Type_INT64, -1, -1, -1},
		{NewJSONLogicalType(), ConvertedType_JSON, Type_BYTE_ARRAY, -1, -1, -1},
		{NewBSONLogicalType(), ConvertedType_BSON, Type_BYTE_ARRAY, -1, -1, -1},
		{NewNoLogicalType(), ConvertedType_NONE, Type_INT64, -1, -1, -1},
	}

	for _, c := range cases {
		confirmPrimitiveNodeFactoryEquivalence(t, c.logicalType,
			c.convertedType, c.physicalType, c.physicalLength,
			c.precision, c.scale,
		)
	}

	// Group nodes ...
	confirmGroupNodeFactoryEquivalence(t, "map", NewMapLogicalType(), ConvertedType_MAP)
	confirmGroupNodeFactoryEquivalence(t, "list", NewListLogicalType(), ConvertedType_LIST)
}

func TestSchemaNodeCreationFactoryExceptions(t *testing.T) {
	// Ensure that the Node factory method that accepts a logical type refuses to create
	// an object if compatibility conditions are not met

	// Nested logical type on non-group node ...
	_, err := NewPrimitiveNodeFromLogicalType("map", RepetitionType_REQUIRED,
		NewMapLogicalType(), Type_INT64, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Incompatible primitive type ...
	_, err = NewPrimitiveNodeFromLogicalType("string", RepetitionType_REQUIRED,
		NewStringLogicalType(), Type_BOOLEAN, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Incompatible primitive length ...
	_, err = NewPrimitiveNodeFromLogicalType(
		"interval", RepetitionType_REQUIRED,
		NewIntervalLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, 11, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Primitive too small for given precision ...
	_, err = NewPrimitiveNodeFromLogicalType(
		"decimal", RepetitionType_REQUIRED,
		newDecimalLogicalType(t, 16, 6), Type_INT32, -1, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Incompatible primitive length ...
	_, err = NewPrimitiveNodeFromLogicalType(
		"uuid", RepetitionType_REQUIRED,
		NewUUIDLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, 64, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Non-positive length argument for fixed length binary ...
	_, err = NewPrimitiveNodeFromLogicalType(
		"negative_length", RepetitionType_REQUIRED,
		NewNoLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, -16, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Non-positive length argument for fixed length binary ...
	_, err = NewPrimitiveNodeFromLogicalType(
		"zero_length", RepetitionType_REQUIRED,
		NewNoLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, 0, -1,
	)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Non-nested logical type on group node ...
	_, err = NewGroupNodeFromLogicalType(
		"list", RepetitionType_REPEATED,
		[]Node{}, NewJSONLogicalType(), -1)
	testutil.AssertErrorIs(t, err, ParquetException)

	// nullptr logical type arguments convert to NoLogicalType/ConvertedType::NONE
	var empty LogicalType
	var node Node
	node, err = NewPrimitiveNodeFromLogicalType(
		"value", RepetitionType_REQUIRED,
		empty, Type_DOUBLE, -1, -1,
	)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, node.LogicalType().isNone())
	testutil.AssertDeepEq(t, node.ConvertedType(), ConvertedType_NONE)
	node, err = NewGroupNodeFromLogicalType(
		"items", RepetitionType_REPEATED,
		[]Node{}, empty, -1)
	testutil.AssertNil(t, err)
	testutil.AssertTrue(t, node.LogicalType().isNone())
	testutil.AssertDeepEq(t, node.ConvertedType(), ConvertedType_NONE)

	// Invalid ConvertedType in deserialized element ...
	node, err = NewPrimitiveNodeFromLogicalType(
		"string", RepetitionType_REQUIRED,
		NewStringLogicalType(), Type_BYTE_ARRAY, -1, -1,
	)
	testutil.AssertNil(t, err)
	testutil.AssertDeepEq(t, node.LogicalType().Kind(), LogicalTypeKind_STRING)
	testutil.AssertTrue(t, node.LogicalType().isValid())
	testutil.AssertTrue(t, node.LogicalType().isSerialized())
	var stringIntermediary parquet.SchemaElement
	testutil.AssertNil(t, node.ToParquet(&stringIntermediary))
	// ... corrupt the Thrift intermediary ....
	stringIntermediary.LogicalType.STRING = nil
	_, err = NewPrimitiveNodeFromParquet(&stringIntermediary, 1)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Invalid TimeUnit in deserialized TimeLogicalType ...
	node, err = NewPrimitiveNodeFromLogicalType(
		"time", RepetitionType_REQUIRED,
		newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS),
		Type_INT64, -1, -1,
	)
	testutil.AssertNil(t, err)
	var timeIntermediary parquet.SchemaElement
	testutil.AssertNil(t, node.ToParquet(&timeIntermediary))
	// ... corrupt the Thrift intermediary ....
	timeIntermediary.LogicalType.TIME.Unit.NANOS = nil
	_, err = NewPrimitiveNodeFromParquet(&timeIntermediary, 1)
	testutil.AssertErrorIs(t, err, ParquetException)

	// Invalid TimeUnit in deserialized TimestampLogicalType ...
	node, err = NewPrimitiveNodeFromLogicalType(
		"timestamp", RepetitionType_REQUIRED,
		newTimestampLogicalType(t, true, LogicalTypeTimeUnit_NANOS, false, false),
		Type_INT64, -1, -1,
	)
	testutil.AssertNil(t, err)
	var timestampIntermediary parquet.SchemaElement
	testutil.AssertNil(t, node.ToParquet(&timestampIntermediary))
	// ... corrupt the Thrift intermediary ....
	timestampIntermediary.LogicalType.TIMESTAMP.Unit.NANOS = nil
	_, err = NewPrimitiveNodeFromParquet(&timestampIntermediary, 1)
	testutil.AssertErrorIs(t, err, ParquetException)
}

type schemaElementConstructionArguments struct {
	name                string
	logicalType         LogicalType
	physicalType        Type
	physicalLength      int
	expectConvertedType bool
	convertedType       ConvertedType
	expectLogicalType   bool
	checkLogicalType    func(parquet.SchemaElement) bool
}

type legacySchemaElementConstructionArguments struct {
	name                string
	physicalType        Type
	physicalLength      int
	expectConvertedType bool
	convertedType       ConvertedType
	expectLogicalType   bool
	checkLogicalType    func(parquet.SchemaElement) bool
}

type testSchemaElementConstruction struct {
	node                Node
	element             parquet.SchemaElement
	name                string
	expectConvertedType bool
	convertedType       ConvertedType // expected converted type in Thrift object
	expectLogicalType   bool
	checkLogicalType    func(parquet.SchemaElement) bool // specialized (by logical type) logicalType check for Thrift object
}

func newTestSchemaElementConstruction() *testSchemaElementConstruction {
	return &testSchemaElementConstruction{}
}

func (tsec *testSchemaElementConstruction) Reconstruct(
	c schemaElementConstructionArguments,
) (*testSchemaElementConstruction, error) {
	// Make node, create serializable Thrift object from it ...
	node, err := NewPrimitiveNodeFromLogicalType(
		c.name, RepetitionType_REQUIRED,
		c.logicalType, c.physicalType, c.physicalLength, -1,
	)
	if err != nil {
		return nil, err
	}
	tsec.node = node
	tsec.element = parquet.SchemaElement{}
	if err := tsec.node.ToParquet(&tsec.element); err != nil {
		return nil, err
	}

	// ... then set aside some values for later inspection.
	tsec.name = c.name
	tsec.expectConvertedType = c.expectConvertedType
	tsec.convertedType = c.convertedType
	tsec.expectLogicalType = c.expectLogicalType
	tsec.checkLogicalType = c.checkLogicalType
	return tsec, nil
}

func (tsec *testSchemaElementConstruction) LegacyReconstruct(
	c legacySchemaElementConstructionArguments,
) (*testSchemaElementConstruction, error) {
	// Make node, create serializable Thrift object from it ...
	node, err := NewPrimitiveNodeFromConvertedType(
		c.name, RepetitionType_REQUIRED, c.physicalType,
		c.convertedType, c.physicalLength, -1, -1, -1,
	)
	if err != nil {
		return nil, err
	}
	tsec.node = node
	tsec.element = parquet.SchemaElement{}
	if err := tsec.node.ToParquet(&tsec.element); err != nil {
		return nil, err
	}

	// ... then set aside some values for later inspection.
	tsec.name = c.name
	tsec.expectConvertedType = c.expectConvertedType
	tsec.convertedType = c.convertedType
	tsec.expectLogicalType = c.expectLogicalType
	tsec.checkLogicalType = c.checkLogicalType
	return tsec, nil
}

func (tsec *testSchemaElementConstruction) Inspect(t *testing.T) {
	t.Helper()
	testutil.AssertDeepEq(t, tsec.element.Name, tsec.name)
	if tsec.expectConvertedType {
		testutil.AssertTrueM(t, tsec.element.IsSetConvertedType(),
			fmt.Sprintf(
				"%s logical type unexpectedly failed to generate a converted "+
					"type in the Thrift intermediate object",
				logicalTypeToString(t, tsec.node.LogicalType()),
			))

		testutil.AssertDeepEqM(t,
			tsec.element.GetConvertedType(),
			ConvertedTypeToThrift(tsec.convertedType),
			fmt.Sprintf(
				"%s logical type unexpectedly failed to generate "+
					"correct converted type in the Thrift intermediate object",
				logicalTypeToString(t, tsec.node.LogicalType()),
			),
		)
	} else {
		testutil.AssertFalseM(t, tsec.element.IsSetConvertedType(),
			fmt.Sprintf(
				"%s logical type unexpectedly generated a converted type in "+
					"the Thrift intermediate object",
				logicalTypeToString(t, tsec.node.LogicalType()),
			))
	}
	if tsec.expectLogicalType {
		testutil.AssertTrueM(t, tsec.element.IsSetLogicalType(),
			fmt.Sprintf(
				"%s logical type generated incorrect logicalType "+
					"settings in the Thrift intermediate object",
				logicalTypeToString(t, tsec.node.LogicalType()),
			))
	} else {
		testutil.AssertFalseM(t, tsec.element.IsSetLogicalType(),
			fmt.Sprintf(
				"%s logical type unexpectedly generated a logicalType in "+
					"the Thrift intermediate object",
				logicalTypeToString(t, tsec.node.LogicalType()),
			))
	}
}

/*
 * The Test*SchemaElementConstruction suites confirm that the logical type
 * and converted type members of the Thrift intermediate message object
 * (format::SchemaElement) that is created upon serialization of an annotated
 * schema node are correctly populated.
 */

func TestSchemaElementConstructionSimpleCases(t *testing.T) {
	tsec := newTestSchemaElementConstruction()

	// used for logical types that don't expect a logicalType to be set
	checkNothing := func(parquet.SchemaElement) bool { return true }

	cases := []schemaElementConstructionArguments{
		{
			"string", NewStringLogicalType(), Type_BYTE_ARRAY,
			-1, true, ConvertedType_UTF8, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetSTRING()
			},
		},
		{
			"enum", NewEnumLogicalType(), Type_BYTE_ARRAY,
			-1, true, ConvertedType_ENUM, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetENUM()
			},
		},
		{
			"date", NewDateLogicalType(), Type_INT32,
			-1, true, ConvertedType_DATE, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetDATE()
			},
		},
		{
			"interval", NewIntervalLogicalType(), Type_FIXED_LEN_BYTE_ARRAY,
			12, true, ConvertedType_INTERVAL, false, checkNothing,
		},
		{
			"null", NewNullLogicalType(), Type_DOUBLE, -1, false,
			ConvertedType_NA, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetUNKNOWN()
			},
		},
		{
			"json", NewJSONLogicalType(), Type_BYTE_ARRAY,
			-1, true, ConvertedType_JSON, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetJSON()
			},
		},
		{
			"bson", NewBSONLogicalType(), Type_BYTE_ARRAY,
			-1, true, ConvertedType_BSON, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetBSON()
			},
		},
		{
			"uuid", NewUUIDLogicalType(), Type_FIXED_LEN_BYTE_ARRAY,
			16, false, ConvertedType_NA, true,
			func(element parquet.SchemaElement) bool {
				return element.LogicalType.IsSetUUID()
			},
		},
		{
			"none", NewNoLogicalType(), Type_INT64,
			-1, false, ConvertedType_NA, false, checkNothing,
		},

		{
			"unknown", NewUnknownLogicalType(), Type_INT64,
			-1, true, ConvertedType_NA, false, checkNothing,
		},
	}

	for _, c := range cases {
		ts, err := tsec.Reconstruct(c)
		testutil.AssertNil(t, err)
		ts.Inspect(t)
	}

	legacyCases := []legacySchemaElementConstructionArguments{
		{
			"timestamp_ms", Type_INT64, -1, true, ConvertedType_TIMESTAMP_MILLIS,
			false, checkNothing,
		},
		{
			"timestamp_us", Type_INT64, -1, true, ConvertedType_TIMESTAMP_MICROS,
			false, checkNothing,
		},
	}

	for _, c := range legacyCases {
		ts, err := tsec.LegacyReconstruct(c)
		testutil.AssertNil(t, err)
		ts.Inspect(t)
	}
}

type testDecimalSchemaElementConstruction struct {
	testSchemaElementConstruction
	precision int
	scale     int
}

func newTestDecimalSchemaElementConstruction() *testDecimalSchemaElementConstruction {
	return &testDecimalSchemaElementConstruction{
		testSchemaElementConstruction: testSchemaElementConstruction{},
	}
}

func (tdsec *testDecimalSchemaElementConstruction) Reconstruct(
	c schemaElementConstructionArguments,
) (*testDecimalSchemaElementConstruction, error) {
	_, err := tdsec.testSchemaElementConstruction.Reconstruct(c)
	if err != nil {
		return nil, err
	}
	decimalLogicalType := c.logicalType.(*DecimalLogicalType)
	tdsec.precision = decimalLogicalType.Precision
	tdsec.scale = decimalLogicalType.Scale
	return tdsec, nil
}

func (tdsec *testDecimalSchemaElementConstruction) Inspect(t *testing.T) {
	t.Helper()
	tdsec.testSchemaElementConstruction.Inspect(t)
	testutil.AssertDeepEq(t, tdsec.element.Precision, tdsec.precision)
	testutil.AssertDeepEq(t, tdsec.element.Scale, tdsec.scale)
	testutil.AssertDeepEq(t, tdsec.element.LogicalType.DECIMAL.Precision, tdsec.precision)
	testutil.AssertDeepEq(t, tdsec.element.LogicalType.DECIMAL.Scale, tdsec.scale)
}

func TestDecimalSchemaElementConstructionDecimalCases(t *testing.T) {
	tsec := newTestSchemaElementConstruction()

	checkDecimal := func(element parquet.SchemaElement) bool {
		return element.LogicalType.IsSetDECIMAL()
	}

	cases := []schemaElementConstructionArguments{
		{
			"decimal", newDecimalLogicalType(t, 16, 6), Type_INT64, -1, true,
			ConvertedType_DECIMAL, true, checkDecimal,
		},
		{
			"decimal", newDecimalLogicalType(t, 1, 0), Type_INT32, -1, true,
			ConvertedType_DECIMAL, true, checkDecimal,
		},
		{
			"decimal", newDecimalLogicalType(t, 10, 0), Type_INT64, -1, true,
			ConvertedType_DECIMAL, true, checkDecimal,
		},
		{
			"decimal", newDecimalLogicalType(t, 11, 11), Type_INT64, -1, true,
			ConvertedType_DECIMAL, true, checkDecimal,
		},
	}

	for _, c := range cases {
		ts, err := tsec.Reconstruct(c)
		testutil.AssertNil(t, err)
		ts.Inspect(t)
	}
}

type testTemporalSchemaElementConstruction struct {
	testSchemaElementConstruction
	adjusted bool
	unit     LogicalTypeTimeUnit
}

func newTestTemporalSchemaElementConstruction() *testTemporalSchemaElementConstruction {
	return &testTemporalSchemaElementConstruction{
		testSchemaElementConstruction: testSchemaElementConstruction{},
	}
}

func (ttsec *testTemporalSchemaElementConstruction) ReconstructTimeLogicalType(
	c schemaElementConstructionArguments,
) (*testTemporalSchemaElementConstruction, error) {
	_, err := ttsec.testSchemaElementConstruction.Reconstruct(c)
	if err != nil {
		return nil, err
	}
	t := c.logicalType.(TimeLogicalType)
	ttsec.adjusted = t.IsAdjustedToUTC
	ttsec.unit = t.TimeUnit
	return ttsec, nil
}

func (ttsec *testTemporalSchemaElementConstruction) ReconstructTimestampLogicalType(
	c schemaElementConstructionArguments,
) (*testTemporalSchemaElementConstruction, error) {
	_, err := ttsec.testSchemaElementConstruction.Reconstruct(c)
	if err != nil {
		return nil, err
	}
	t := c.logicalType.(TimestampLogicalType)
	ttsec.adjusted = t.IsAdjustedToUTC
	ttsec.unit = t.TimeUnit
	return ttsec, nil
}

func (ttsec *testTemporalSchemaElementConstruction) InspectTimeLogicalType(t *testing.T) {
	t.Helper()
	ttsec.testSchemaElementConstruction.Inspect(t)
	testutil.AssertDeepEq(t, ttsec.element.LogicalType.TIME.IsAdjustedToUTC, ttsec.adjusted)
	switch ttsec.unit {
	case LogicalTypeTimeUnit_MILLIS:
		testutil.AssertTrue(t, ttsec.element.LogicalType.TIME.GetUnit().IsSetMILLIS())
	case LogicalTypeTimeUnit_MICROS:
		testutil.AssertTrue(t, ttsec.element.LogicalType.TIME.GetUnit().IsSetMICROS())
	case LogicalTypeTimeUnit_NANOS:
		testutil.AssertTrue(t, ttsec.element.LogicalType.TIME.GetUnit().IsSetNANOS())
	case LogicalTypeTimeUnit_UNKNOWN:
	default:
		t.Error("Invalid time unit in test case")
	}
}

func (ttsec *testTemporalSchemaElementConstruction) InspectTimeTimestampLogicalType(t *testing.T) {
	t.Helper()
	ttsec.testSchemaElementConstruction.Inspect(t)
	testutil.AssertDeepEq(t, ttsec.element.LogicalType.TIMESTAMP.IsAdjustedToUTC, ttsec.adjusted)
	switch ttsec.unit {
	case LogicalTypeTimeUnit_MILLIS:
		testutil.AssertTrue(t, ttsec.element.LogicalType.TIMESTAMP.GetUnit().IsSetMILLIS())
	case LogicalTypeTimeUnit_MICROS:
		testutil.AssertTrue(t, ttsec.element.LogicalType.TIMESTAMP.GetUnit().IsSetMICROS())
	case LogicalTypeTimeUnit_NANOS:
		testutil.AssertTrue(t, ttsec.element.LogicalType.TIMESTAMP.GetUnit().IsSetNANOS())
	case LogicalTypeTimeUnit_UNKNOWN:
	default:
		t.Error("Invalid time unit in test case")
	}
}

func TestTemporalSchemaElementConstructionTemporalCases(t *testing.T) {
	ttsec := newTestTemporalSchemaElementConstruction()

	checkTime := func(element parquet.SchemaElement) bool {
		return element.LogicalType.IsSetTIME()
	}

	timeCases := []schemaElementConstructionArguments{
		{
			"time_T_ms",
			newTimeLogicalType(t, true, LogicalTypeTimeUnit_MILLIS),
			Type_INT32,
			-1, true, ConvertedType_TIME_MILLIS, true, checkTime,
		},
		{
			"time_F_ms",
			newTimeLogicalType(t, false, LogicalTypeTimeUnit_MILLIS),
			Type_INT32,
			-1, false, ConvertedType_NA, true, checkTime,
		},
		{
			"time_T_us",
			newTimeLogicalType(t, true, LogicalTypeTimeUnit_MICROS),
			Type_INT64,
			-1, true, ConvertedType_TIME_MICROS, true, checkTime,
		},
		{
			"time_F_us",
			newTimeLogicalType(t, false, LogicalTypeTimeUnit_MICROS),
			Type_INT64,
			-1, false, ConvertedType_NA, true, checkTime,
		},
		{
			"time_T_ns",
			newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS),
			Type_INT64,
			-1, false, ConvertedType_NA, true, checkTime,
		},
		{
			"time_F_ns",
			newTimeLogicalType(t, false, LogicalTypeTimeUnit_NANOS),
			Type_INT64,
			-1, false, ConvertedType_NA, true, checkTime,
		},
	}

	for _, c := range timeCases {
		ts, err := ttsec.ReconstructTimeLogicalType(c)
		testutil.AssertNil(t, err)
		ts.InspectTimeLogicalType(t)
	}

	checkTimestamp := func(element parquet.SchemaElement) bool {
		return element.LogicalType.IsSetTIMESTAMP()
	}

	timestampCases := []schemaElementConstructionArguments{
		{
			"timestamp_T_ms",
			newTimestampLogicalType(
				t, true, LogicalTypeTimeUnit_MILLIS, false, false),
			Type_INT64, -1, true, ConvertedType_TIMESTAMP_MILLIS,
			true, checkTimestamp,
		},
		{
			"timestamp_F_ms",
			newTimestampLogicalType(
				t, false, LogicalTypeTimeUnit_MILLIS, false, false),
			Type_INT64, -1, false, ConvertedType_NA,
			true, checkTimestamp,
		},
		{
			"timestamp_F_ms_force",
			newTimestampLogicalType(
				t, false, LogicalTypeTimeUnit_MILLIS, false, true),
			Type_INT64, -1, true, ConvertedType_TIMESTAMP_MILLIS,
			true, checkTimestamp,
		},
		{
			"timestamp_T_us",
			newTimestampLogicalType(
				t, true, LogicalTypeTimeUnit_MICROS, false, false),
			Type_INT64, -1, true, ConvertedType_TIMESTAMP_MICROS,
			true, checkTimestamp,
		},
		{
			"timestamp_F_us",
			newTimestampLogicalType(
				t, false, LogicalTypeTimeUnit_MICROS, false, false),
			Type_INT64, -1, false, ConvertedType_NA,
			true, checkTimestamp,
		},
		{
			"timestamp_F_us_force",
			newTimestampLogicalType(
				t, false, LogicalTypeTimeUnit_MILLIS, false, true),
			Type_INT64, -1, true, ConvertedType_TIMESTAMP_MILLIS,
			true, checkTimestamp,
		},
		{
			"timestamp_T_ns",
			newTimestampLogicalType(
				t, true, LogicalTypeTimeUnit_NANOS, false, false),
			Type_INT64, -1, false, ConvertedType_NA,
			true, checkTimestamp,
		},
		{
			"timestamp_F_ns",
			newTimestampLogicalType(
				t, false, LogicalTypeTimeUnit_NANOS, false, false),
			Type_INT64, -1, false, ConvertedType_NA,
			true, checkTimestamp,
		},
	}

	for _, c := range timestampCases {
		ts, err := ttsec.ReconstructTimestampLogicalType(c)
		testutil.AssertNil(t, err)
		ts.InspectTimeTimestampLogicalType(t)
	}
}

type testIntegerSchemaElementConstruction struct {
	testSchemaElementConstruction
	width  int8
	signed bool
}

func newTestIntegerSchemaElementConstruction() *testIntegerSchemaElementConstruction {
	return &testIntegerSchemaElementConstruction{
		testSchemaElementConstruction: testSchemaElementConstruction{},
	}
}

func (tisec *testIntegerSchemaElementConstruction) Reconstruct(
	c schemaElementConstructionArguments,
) (*testIntegerSchemaElementConstruction, error) {
	_, err := tisec.testSchemaElementConstruction.Reconstruct(c)
	if err != nil {
		return nil, err
	}
	intLogicalType := c.logicalType.(IntLogicalType)
	tisec.width = int8(intLogicalType.Width)
	tisec.signed = intLogicalType.Signed
	return tisec, nil
}

func (tisec *testIntegerSchemaElementConstruction) Inspect(t *testing.T) {
	t.Helper()
	tisec.testSchemaElementConstruction.Inspect(t)
	testutil.AssertDeepEq(t, tisec.element.LogicalType.INTEGER.GetBitWidth(), tisec.width)
	testutil.AssertDeepEq(t, tisec.element.LogicalType.INTEGER.GetIsSigned(), tisec.signed)
}

func TestIntegerSchemaElementConstructionIntegerCases(t *testing.T) {
	tisec := newTestIntegerSchemaElementConstruction()

	checkInteger := func(element parquet.SchemaElement) bool {
		return element.LogicalType.IsSetINTEGER()
	}

	cases := []schemaElementConstructionArguments{
		{
			"uint8", newIntLogicalType(t, 8, false), Type_INT32, -1,
			true, ConvertedType_UINT_8, true, checkInteger,
		},
		{
			"uint16", newIntLogicalType(t, 16, false), Type_INT32, -1,
			true, ConvertedType_UINT_16, true, checkInteger,
		},
		{
			"uint32", newIntLogicalType(t, 32, false), Type_INT32, -1,
			true, ConvertedType_UINT_32, true, checkInteger,
		},
		{
			"uint64", newIntLogicalType(t, 64, false), Type_INT64, -1,
			true, ConvertedType_UINT_64, true, checkInteger,
		},
		{
			"int8", newIntLogicalType(t, 8, true), Type_INT32, -1,
			true, ConvertedType_INT_8, true, checkInteger,
		},
		{
			"int16", newIntLogicalType(t, 16, true), Type_INT32, -1,
			true, ConvertedType_INT_16, true, checkInteger,
		},
		{
			"int32", newIntLogicalType(t, 32, true), Type_INT32, -1,
			true, ConvertedType_INT_32, true, checkInteger,
		},
		{
			"int64", newIntLogicalType(t, 64, true), Type_INT64, -1,
			true, ConvertedType_INT_64, true, checkInteger,
		},
	}

	for _, c := range cases {
		ti, err := tisec.Reconstruct(c)
		testutil.AssertNil(t, err)
		ti.Inspect(t)
	}
}

func TestLogicalTypeSerializationSchemaElementNestedCases(t *testing.T) {
	// Confirm that the intermediate Thrift objects created during node
	// serialization contain correct ConvertedType and ConvertedType information

	stringNode, err := NewPrimitiveNodeFromLogicalType(
		"string", RepetitionType_REQUIRED,
		NewStringLogicalType(), Type_BYTE_ARRAY, -1, -1,
	)
	testutil.AssertNil(t, err)

	dateNode, err := NewPrimitiveNodeFromLogicalType(
		"date", RepetitionType_REQUIRED,
		NewDateLogicalType(), Type_INT32, -1, -1,
	)
	testutil.AssertNil(t, err)

	jsonNode, err := NewPrimitiveNodeFromLogicalType(
		"date", RepetitionType_REQUIRED,
		NewJSONLogicalType(), Type_BYTE_ARRAY, -1, -1,
	)
	testutil.AssertNil(t, err)

	uuidNode, err := NewPrimitiveNodeFromLogicalType(
		"uuid", RepetitionType_REQUIRED,
		NewUUIDLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, 16, -1,
	)
	testutil.AssertNil(t, err)

	timestampNode, err := NewPrimitiveNodeFromLogicalType(
		"timestamp", RepetitionType_REQUIRED,
		newTimestampLogicalType(t, false,
			LogicalTypeTimeUnit_NANOS, false, false),
		Type_INT64, 16, -1,
	)
	testutil.AssertNil(t, err)

	intNode, err := NewPrimitiveNodeFromLogicalType(
		"int", RepetitionType_REQUIRED,
		newIntLogicalType(t, 64, false), Type_INT64, -1, -1,
	)
	testutil.AssertNil(t, err)

	decimalNode, err := NewPrimitiveNodeFromLogicalType(
		"decimal", RepetitionType_REQUIRED,
		newDecimalLogicalType(t, 16, 6), Type_INT64, -1, -1,
	)
	testutil.AssertNil(t, err)

	listNode, err := NewGroupNodeFromLogicalType(
		"list", RepetitionType_REPEATED,
		[]Node{stringNode, dateNode, jsonNode, uuidNode,
			timestampNode, intNode, decimalNode},
		NewListLogicalType(), -1,
	)
	testutil.AssertNil(t, err)

	var listElements []parquet.SchemaElement
	testutil.AssertNil(t, ToParquet(listNode, &listElements))
	testutil.AssertDeepEq(t, listElements[0].GetName(), "list")
	testutil.AssertTrue(t, listElements[0].IsSetConvertedType())
	testutil.AssertTrue(t, listElements[0].IsSetLogicalType())
	testutil.AssertDeepEq(t, listElements[0].GetConvertedType(), ConvertedTypeToThrift(ConvertedType_LIST))
	testutil.AssertTrue(t, listElements[0].LogicalType.IsSetLIST())
	testutil.AssertTrue(t, listElements[1].LogicalType.IsSetSTRING())
	testutil.AssertTrue(t, listElements[2].LogicalType.IsSetDATE())
	testutil.AssertTrue(t, listElements[3].LogicalType.IsSetJSON())
	testutil.AssertTrue(t, listElements[4].LogicalType.IsSetUUID())
	testutil.AssertTrue(t, listElements[5].LogicalType.IsSetTIMESTAMP())
	testutil.AssertTrue(t, listElements[6].LogicalType.IsSetINTEGER())
	testutil.AssertTrue(t, listElements[7].LogicalType.IsSetDECIMAL())

	mapNode, err := NewGroupNodeFromLogicalType(
		"map", RepetitionType_REQUIRED, []Node{}, NewMapLogicalType(), -1,
	)
	testutil.AssertNil(t, err)
	var mapElements []parquet.SchemaElement
	testutil.AssertNil(t, ToParquet(mapNode, &mapElements))
	testutil.AssertDeepEq(t, mapElements[0].GetName(), "map")
	testutil.AssertTrue(t, mapElements[0].IsSetConvertedType())
	testutil.AssertTrue(t, mapElements[0].IsSetLogicalType())
	testutil.AssertDeepEq(t, mapElements[0].GetConvertedType(), ConvertedTypeToThrift(ConvertedType_MAP))
	testutil.AssertTrue(t, mapElements[0].LogicalType.IsSetMAP())
}

func confirmPrimitiveNodeRoundtrip(
	t *testing.T,
	logicalType LogicalType,
	physicalType Type,
	physicalLength int,
) {
	t.Helper()
	original, err := NewPrimitiveNodeFromLogicalType(
		"something", RepetitionType_REQUIRED,
		logicalType, physicalType, physicalLength, -1,
	)
	testutil.AssertNil(t, err)

	var intermediary parquet.SchemaElement
	testutil.AssertNil(t, original.ToParquet(&intermediary))
	recovered, err := NewPrimitiveNodeFromParquet(&intermediary, 1)
	testutil.AssertNil(t, err)
	testutil.AssertTrueM(t, original.Equals(recovered),
		fmt.Sprintf("Recovered primitive node unexpectedly not equivalent to "+
			"original primitive node constructed with logical type %s",
			logicalTypeToString(t, logicalType),
		),
	)
}

func confirmGroupNodeRoundtrip(
	t *testing.T,
	name string,
	logicalType LogicalType,
) {
	var nodeVector []Node
	original, err := NewGroupNodeFromLogicalType(
		"group", RepetitionType_REPEATED,
		nodeVector, NewListLogicalType(), -1)
	testutil.AssertNil(t, err)
	var elements []parquet.SchemaElement
	testutil.AssertNil(t, ToParquet(original, &elements))
	recovered, err := NewGroupNodeFromParquet(&(elements[0]), 1, nodeVector)
	testutil.AssertNil(t, err)
	testutil.AssertTrueM(t, original.Equals(recovered),
		fmt.Sprintf("Recovered group node unexpectedly not equivalent to "+
			"original group node constructed with logical type %s",
			logicalTypeToString(t, logicalType)))
}

func TestLogicalTypeSerializationRoundtrips(t *testing.T) {
	// Confirm that Thrift serialization-deserialization of nodes with logical
	// types produces equivalent reconstituted nodes

	// Primitive nodes ...
	cases := []struct {
		logicalType    LogicalType
		physicalType   Type
		physicalLength int
	}{
		{NewStringLogicalType(), Type_BYTE_ARRAY, -1},
		{NewEnumLogicalType(), Type_BYTE_ARRAY, -1},
		{newDecimalLogicalType(t, 16, 6), Type_INT64, -1},
		{NewDateLogicalType(), Type_INT32, -1},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MILLIS), Type_INT32, -1},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_MICROS), Type_INT64, -1},
		{newTimeLogicalType(t, true, LogicalTypeTimeUnit_NANOS), Type_INT64, -1},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MILLIS), Type_INT32, -1},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_MICROS), Type_INT64, -1},
		{newTimeLogicalType(t, false, LogicalTypeTimeUnit_NANOS), Type_INT64, -1},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MILLIS, false, false), Type_INT64, -1},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_MICROS, false, false), Type_INT64, -1},
		{newTimestampLogicalType(t, true, LogicalTypeTimeUnit_NANOS, false, false), Type_INT64, -1},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_MILLIS, false, false), Type_INT64, -1},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_MICROS, false, false), Type_INT64, -1},
		{newTimestampLogicalType(t, false, LogicalTypeTimeUnit_NANOS, false, false), Type_INT64, -1},
		{NewIntervalLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, 12},
		{newIntLogicalType(t, 8, false), Type_INT32, -1},
		{newIntLogicalType(t, 16, false), Type_INT32, -1},
		{newIntLogicalType(t, 32, false), Type_INT32, -1},
		{newIntLogicalType(t, 64, false), Type_INT64, -1},
		{newIntLogicalType(t, 8, true), Type_INT32, -1},
		{newIntLogicalType(t, 16, true), Type_INT32, -1},
		{newIntLogicalType(t, 32, true), Type_INT32, -1},
		{newIntLogicalType(t, 64, true), Type_INT64, -1},
		{NewNullLogicalType(), Type_BOOLEAN, -1},
		{NewJSONLogicalType(), Type_BYTE_ARRAY, -1},
		{NewBSONLogicalType(), Type_BYTE_ARRAY, -1},
		{NewUUIDLogicalType(), Type_FIXED_LEN_BYTE_ARRAY, 16},
		{NewNoLogicalType(), Type_BOOLEAN, -1},
	}

	for _, c := range cases {
		confirmPrimitiveNodeRoundtrip(
			t, c.logicalType, c.physicalType, c.physicalLength)
	}

	// Group nodes ...
	confirmGroupNodeRoundtrip(t, "map", NewMapLogicalType())
	confirmGroupNodeRoundtrip(t, "list", NewListLogicalType())
}
