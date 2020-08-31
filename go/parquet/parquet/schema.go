package parquet

import (
	"fmt"
	"io"
	"strings"

	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/util"
	"github.com/xitongsys/parquet-go/parquet"
)

// https://github.com/apache/arrow/blob/cb2647af3884d7271345c579962d7f1b64b89093/cpp/src/parquet/schema.h
// List encodings: using the terminology from Impala to define different styles
// of representing logical lists (a.k.a. ARRAY types) in Parquet schemas. Since
// the converted type named in the Parquet metadata is ConvertedType::LIST we
// use that terminology here. It also helps distinguish from the *_ARRAY
// primitive types.
//
// One-level encoding: Only allows required lists with required cells
//   repeated value_type name
//
// Two-level encoding: Enables optional lists with only required cells
//   <required/optional> group list
//     repeated value_type item
//
// Three-level encoding: Enables optional lists with optional cells
//   <required/optional> group bag
//     repeated group list
//       <required/optional> value_type item
//
// 2- and 1-level encoding are respectively equivalent to 3-level encoding with
// the non-repeated nodes set to required.
//
// The "official" encoding recommended in the Parquet spec is the 3-level, and
// we use that as the default when creating list types. For semantic completeness
// we allow the other two. Since all types of encodings will occur "in the
// wild" we need to be able to interpret the associated definition levels in
// the context of the actual encoding used in the file.
//
// NB: Some Parquet writers may not set ConvertedType::LIST on the repeated
// SchemaElement, which could make things challenging if we are trying to infer
// that a sequence of nodes semantically represents an array according to one
// of these encodings (versus a struct containing an array). We should refuse
// the temptation to guess, as they say.

type ListEncodingType int

const (
	_ ListEncodingType = iota
	ListEncodingType_ONE_LEVEL
	ListEncodingType_TWO_LEVEL
	ListEncodingType_THREE_LEVEL
)

// ----------------------------------------------------------------------
// ColumnPath

// Can we type alias this?
type ColumnPath []string

func NewColumnPathFromDotString(dotstring string) ColumnPath {
	return ColumnPath(strings.Split(dotstring, "."))
}

func NewColumnPathFromNode(node Node) ColumnPath {
	// Build the path in reverse order as we traverse the nodes to the top
	var rpath []string
	cursor := node
	// The schema node is not part of the ColumnPath
	for cursor.Parent() != nil {
		rpath = append(rpath, cursor.Name())
		cursor = cursor.Parent()
	}

	// Build ColumnPath in correct order
	// Reverse the slice without allocating a new one.
	for i := len(rpath)/2 - 1; i >= 0; i-- {
		opp := len(rpath) - 1 - i
		rpath[i], rpath[opp] = rpath[opp], rpath[i]
	}

	return ColumnPath(rpath)
}

func (cp ColumnPath) extend(nodeName string) ColumnPath {
	return ColumnPath(append(cp, nodeName))
}

func (cp ColumnPath) ToDotString() string {
	return strings.Join(cp, ".")
}

// Fixme(nickpoorman): This maybe doesn't make sense in Go.
// Leaving here to make porting easier for now.
func (cp ColumnPath) ToDotVector() []string {
	return cp
}

// ----------------------------------------------------------------------
// Base node

type NodeType int

const (
	_ NodeType = iota
	NodeType_PRIMITIVE
	NodeType_GROUP
)

// Node must be an interface because we pass around PrimitiveNode as if it's a node.
type Node interface {
	isPrimitive() bool
	isGroup() bool
	isOptional() bool
	isRepeated() bool
	isRequired() bool
	Equals(other Node) bool
	path() ColumnPath
	ToParquet(element interface{}) error
	Visit(visitor NodeVisitor) error
	VisitConst(visitor ConstVisitor) error
	// EqualsInternal(other Node) bool // TODO: REMOVE

	NodeType() NodeType
	Name() string
	Parent() Node
	RepetitionType() RepetitionType
	ConvertedType() ConvertedType
	LogicalType() LogicalType
	SetParent(pParent Node)
}

// Base class for logical schema types. A type has a name, repetition level,
// and optionally a logical type (ConvertedType in Parquet metadata parlance)
type node struct {
	nodeType       NodeType
	name           string
	repetitionType RepetitionType
	convertedType  ConvertedType
	logicalType    LogicalType
	id             int

	// Nodes should not be shared, they have a single parent.
	parent Node
}

// https://github.com/apache/arrow/blob/cb2647af3884d7271345c579962d7f1b64b89093/cpp/src/parquet/schema.h#L102
// NewNodeFromConvertedType creates a new Node
func newNodeFromConvertedType(nodeType NodeType, name string, repetitionType RepetitionType, convertedType ConvertedType, id int) *node {
	return &node{
		nodeType:       nodeType,
		name:           name,
		repetitionType: repetitionType,
		convertedType:  convertedType,
		id:             id,
		parent:         nil,

		// Because logicalType is a struct type, we meed to make sure there is
		// one on the node otherwise methods like Equals() will panic with nil
		// pointer dereference when called.
		logicalType: NewNoLogicalType(),
	}
}

// NewNodeFromLogicalType creates a new Node
func newNodeFromLogicalType(nodeType NodeType, name string, repetitionType RepetitionType, logicalType LogicalType, id int) *node {
	return &node{
		nodeType:       nodeType,
		name:           name,
		repetitionType: repetitionType,
		logicalType:    logicalType,
		id:             id,
		parent:         nil,
	}
}

func (n *node) isPrimitive() bool {
	return n.nodeType == NodeType_PRIMITIVE
}

func (n *node) isGroup() bool {
	return n.nodeType == NodeType_GROUP
}

func (n *node) isOptional() bool {
	return n.repetitionType == RepetitionType_OPTIONAL
}

func (n *node) isRepeated() bool {
	return n.repetitionType == RepetitionType_REPEATED
}

func (n *node) isRequired() bool {
	return n.repetitionType == RepetitionType_REQUIRED
}

func (n *node) path() ColumnPath {
	// TODO(nickpoorman): Cache the result, or more precisely, cache .ToDotString()
	//    since it is being used to access the leaf nodes
	return NewColumnPathFromNode(n)
}

// We don't want to implement these becasue 'node' is a base type. The actual Node type
// implementations (PrimitiveNode/GroupNode) should contain the implementations.

func (n *node) ToParquet(element interface{}) error {
	// This should never be called directly
	panic("base type 'node' does not implement 'ToParquet'")
}

func (n *node) Visit(visitor NodeVisitor) error {
	// This should never be called directly
	panic("base type 'node' does not implement 'Visit'")
}

func (n *node) VisitConst(visitor ConstVisitor) error {
	// This should never be called directly
	panic("base type 'node' does not implement 'VisitConst'")
}

func (n *node) Equals(other Node) bool {
	return n.nodeType == other.NodeType() && n.name == other.Name() &&
		n.repetitionType == other.RepetitionType() && n.convertedType == other.ConvertedType() &&
		(n.logicalType.Equals(other.LogicalType()))
}

func (n *node) NodeType() NodeType {
	return n.nodeType
}

func (n *node) Name() string {
	return n.name
}

func (n *node) Parent() Node {
	return n.parent
}

func (n *node) RepetitionType() RepetitionType {
	return n.repetitionType
}

func (n *node) ConvertedType() ConvertedType {
	return n.convertedType
}

func (n *node) LogicalType() LogicalType {
	return n.logicalType
}

func (n *node) SetParent(pParent Node) {
	n.parent = pParent
}

// NodeVisitor abstract class for walking schemas with the visitor pattern
type NodeVisitor interface {
	Visit(node Node) error
}

type ConstVisitor interface {
	Visit(node Node) error
}

// TODO: Remove
// Save our breath all over the place with these type aliases
// type NodePtr *Node
// type NodeSlice []NodePtr

// ----------------------------------------------------------------------
// Primitive node

// A type that is one of the primitive Parquet storage types. In addition to
// the other type metadata (name, repetition level, logical type), also has the
// physical storage type and their type-specific metadata (byte width, decimal
// parameters)
type PrimitiveNode struct {
	node
	PhysicalType    Type
	TypeLength      int // For FIXED_LEN_BYTE_ARRAY
	DecimalMetadata DecimalMetadata
	ColumnOrder     ColumnOrder
}

// If the parquet file is corrupted it is possible the type value decoded
// will not be in the range of format::Type::type, which is undefined behavior.
// This method prevents this by loading the value as the underlying type and checking
// to make sure it is in range.
// TODO(nickpoorman): Implement SafeLoader in parquet/schema.cc

func NewPrimitiveNodeFromParquet(opaqueElement interface{}, nodeId int) (Node, error) {
	element := opaqueElement.(*parquet.SchemaElement)

	var primitiveNode *PrimitiveNode
	if element.IsSetLogicalType() {
		// updated writer with logical type present
		logicalType, err := LogicalTypeFromThrift(*element.GetLogicalType())
		if err != nil {
			return nil, err
		}
		pn, err := NewPrimitiveNodeFromLogicalType(
			element.GetName(),
			RepetitionTypeFromThrift(element.GetRepetitionType()),
			logicalType,
			TypeFromThrift(element.GetType()),
			int(element.GetTypeLength()),
			nodeId,
		)
		if err != nil {
			return nil, err
		}
		primitiveNode = pn
	} else if element.IsSetConvertedType() {
		// legacy writer with logical type present
		pn, err := NewPrimitiveNodeFromConvertedType(
			element.GetName(),
			RepetitionTypeFromThrift(element.GetRepetitionType()),
			TypeFromThrift(element.GetType()),
			ConvertedTypeFromThrift(element.GetConvertedType()),
			int(element.GetTypeLength()),
			int(element.GetPrecision()),
			int(element.GetScale()),
			nodeId,
		)
		if err != nil {
			return nil, err
		}
		primitiveNode = pn
	} else {
		// logical type not present
		pn, err := NewPrimitiveNodeFromLogicalType(
			element.GetName(),
			RepetitionTypeFromThrift(element.GetRepetitionType()),
			NewNoLogicalType(),
			TypeFromThrift(element.GetType()),
			int(element.GetTypeLength()),
			nodeId,
		)
		if err != nil {
			return nil, err
		}
		primitiveNode = pn
	}

	return primitiveNode, nil
}

func NewPrimitiveNodeFromConvertedType(name string, repetitionType RepetitionType,
	physicalType Type, convertedType ConvertedType,
	length int, precision int, scale int, id int) (*PrimitiveNode, error) {

	pn := &PrimitiveNode{
		node:         *newNodeFromConvertedType(NodeType_PRIMITIVE, name, repetitionType, convertedType, id),
		PhysicalType: physicalType,
		TypeLength:   length,
	}

	// Check if the physical and logical types match
	// Mapping referred from Apache parquet-mr as on 2016-02-22
	switch convertedType {
	case ConvertedType_NONE:
		// Logical type not set
	case ConvertedType_UTF8, ConvertedType_JSON, ConvertedType_BSON:
		if physicalType != Type_BYTE_ARRAY {
			return pn, fmt.Errorf(
				"%s can only annotate BYTE_ARRAY fields: %w",
				ConvertedTypeToString(convertedType),
				ParquetException,
			)
		}
	case ConvertedType_DECIMAL:
		if physicalType != Type_INT32 && physicalType != Type_INT64 &&
			physicalType != Type_BYTE_ARRAY && physicalType != Type_FIXED_LEN_BYTE_ARRAY {
			return pn, fmt.Errorf("DECIMAL can only annotate INT32, INT64, BYTE_ARRAY, and FIXED: %w", ParquetException)
		}
		if precision <= 0 {
			return pn, fmt.Errorf(
				"Invalid DECIMAL precision: %d. Precision must be a number between 1 and 38 inclusive: %w",
				precision,
				ParquetException,
			)
		}
		if scale < 0 {
			return pn, fmt.Errorf(
				"Invalid DECIMAL scale: %d. Scale must be a number between 0 and precision inclusive: %w",
				scale,
				ParquetException,
			)
		}
		if scale > precision {
			return pn, fmt.Errorf(
				"Invalid DECIMAL scale %d cannot be greater than precision %d: %w",
				scale,
				precision,
				ParquetException,
			)
		}
		pn.DecimalMetadata.isSet = true
		pn.DecimalMetadata.precision = precision
		pn.DecimalMetadata.scale = scale
	case ConvertedType_DATE, ConvertedType_TIME_MILLIS, ConvertedType_UINT_8,
		ConvertedType_UINT_16, ConvertedType_UINT_32, ConvertedType_INT_8,
		ConvertedType_INT_16, ConvertedType_INT_32:
		if physicalType != Type_INT32 {
			return pn, fmt.Errorf(
				"%s can only annotate INT32: %w",
				ConvertedTypeToString(convertedType),
				ParquetException,
			)
		}
	case ConvertedType_TIME_MICROS, ConvertedType_TIMESTAMP_MILLIS,
		ConvertedType_TIMESTAMP_MICROS, ConvertedType_UINT_64, ConvertedType_INT_64:
		if physicalType != Type_INT64 {
			return pn, fmt.Errorf(
				"%s can only annotate INT64: %w",
				ConvertedTypeToString(convertedType),
				ParquetException,
			)
		}
	case ConvertedType_INTERVAL:
		if physicalType != Type_FIXED_LEN_BYTE_ARRAY || length != 12 {
			return pn, fmt.Errorf("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12): %w", ParquetException)
		}
	case ConvertedType_ENUM:
		if physicalType != Type_BYTE_ARRAY {
			return pn, fmt.Errorf("ENUM can only annotate BYTE_ARRAY fields: %w", ParquetException)
		}
	case ConvertedType_NA:
		// NA can annotate any type
	default:
		return pn, fmt.Errorf(
			"%s can not be applied to a primitive type: %w",
			ConvertedTypeToString(convertedType),
			ParquetException,
		)
	}
	// For forward compatibility, create an equivalent logical type
	lt, err := NewLogicalTypeFromConvertedType(pn.ConvertedType(), pn.DecimalMetadata)
	if err != nil {
		return pn, err
	}
	pn.logicalType = lt
	debug.Assert(pn.LogicalType() != nil && !pn.logicalType.isNested() &&
		pn.logicalType.isCompatible(pn.convertedType, pn.DecimalMetadata),
		fmt.Sprintf(
			"NewPrimitiveNodeFromConvertedType: logicalType is nil (%t) or logicalType is nested (%t) or logicalType is not compatable (%t)",
			!(pn.LogicalType() != nil),
			!(!pn.logicalType.isNested()),
			!(pn.logicalType.isCompatible(pn.convertedType, pn.DecimalMetadata)),
		),
	)
	if physicalType == Type_FIXED_LEN_BYTE_ARRAY {
		if length <= 0 {
			return pn, fmt.Errorf("Invalid FIXED_LEN_BYTE_ARRAY length: %d: %w", length, ParquetException)
		}
		pn.TypeLength = length
	}

	return pn, nil
}

func NewPrimitiveNodeFromLogicalType(name string, repetitionType RepetitionType,
	logicalType LogicalType, physicalType Type, physicalLength int, id int) (*PrimitiveNode, error) {

	pn := &PrimitiveNode{
		node:         *newNodeFromLogicalType(NodeType_PRIMITIVE, name, repetitionType, logicalType, id),
		PhysicalType: physicalType,
		TypeLength:   physicalLength,
	}

	if pn.logicalType != nil {
		// Check for logical type <=> node type consistency
		if !pn.logicalType.isNested() {
			// Check for logical type <=> physical type consistency
			if pn.logicalType.isApplicable(physicalType, int32(physicalLength)) {
				// For backward compatibility, assign equivalent legacy
				// converted type (if possible)
				pn.convertedType = pn.logicalType.ToConvertedType(&pn.DecimalMetadata)
			} else {
				ltStr, err := pn.logicalType.ToString()
				if err != nil {
					return nil, err
				}
				return pn, fmt.Errorf(
					"%s can not be applied to primitive type %s: %w",
					ltStr,
					TypeToString(physicalType),
					ParquetException,
				)
			}
		} else {
			ltStr, err := pn.logicalType.ToString()
			if err != nil {
				return nil, err
			}
			return pn, fmt.Errorf(
				"Nested logical type %s can not be applied to non-group node: %w",
				ltStr,
				ParquetException,
			)
		}
	} else {
		pn.logicalType = NewNoLogicalType()
		pn.convertedType = pn.logicalType.ToConvertedType(&pn.DecimalMetadata)
	}
	debug.Assert(pn.logicalType != nil && !pn.logicalType.isNested() &&
		pn.logicalType.isCompatible(pn.convertedType, pn.DecimalMetadata),
		"NewPrimitiveNodeFromLogicalType: logicalType is nil or logicalType is nested or logicalType is not compatable",
	)

	if physicalType == Type_FIXED_LEN_BYTE_ARRAY {
		if physicalLength <= 0 {
			return pn, fmt.Errorf("Invalid FIXED_LEN_BYTE_ARRAY length: %d: %w", physicalLength, ParquetException)
		}
	}

	return pn, nil
}

func (pn *PrimitiveNode) Equals(other Node) bool {
	if !pn.node.Equals(other) {
		return false
	}
	otherPn, ok := other.(*PrimitiveNode)
	return ok && pn.EqualsInternal(otherPn)
}

func (pn *PrimitiveNode) ToParquet(opaqueElement interface{}) error {
	element := opaqueElement.(*parquet.SchemaElement)
	element.Name = pn.name
	repetitionType := RepetitionTypeToThrift(pn.repetitionType)
	element.RepetitionType = &repetitionType
	if pn.convertedType != ConvertedType_NONE {
		convertedType := ConvertedTypeToThrift(pn.convertedType)
		element.ConvertedType = &convertedType
	}
	if pn.logicalType != nil && pn.logicalType.isSerialized() &&
		// TODO(nickpoorman): remove the following conjunct to enable serialization
		// of IntervalTypes after parquet.thrift recognizes them
		!pn.logicalType.isInterval() {
		logicalType, err := pn.logicalType.ToThrift()
		if err != nil {
			return err
		}
		element.LogicalType = logicalType
	}
	physicalType := TypeToThrift(pn.PhysicalType)
	element.Type = &physicalType
	if pn.PhysicalType == Type_FIXED_LEN_BYTE_ARRAY {
		typeLength := int32(pn.TypeLength)
		element.TypeLength = &typeLength
	}
	if pn.DecimalMetadata.isSet {
		p := int32(pn.DecimalMetadata.precision)
		element.Precision = &p
		s := int32(pn.DecimalMetadata.scale)
		element.Scale = &s
	}
	return nil
}

func (pn *PrimitiveNode) Visit(visitor NodeVisitor) error {
	return visitor.Visit(pn)
}

func (pn *PrimitiveNode) VisitConst(visitor ConstVisitor) error {
	return visitor.Visit(pn)
}

func (pn *PrimitiveNode) EqualsInternal(other *PrimitiveNode) bool {
	isEqual := true
	if pn.PhysicalType != other.PhysicalType {
		return false
	}
	if pn.convertedType == ConvertedType_DECIMAL {
		isEqual = isEqual && pn.DecimalMetadata.precision == other.DecimalMetadata.precision &&
			pn.DecimalMetadata.scale == other.DecimalMetadata.scale
	}
	if pn.PhysicalType == Type_FIXED_LEN_BYTE_ARRAY {
		isEqual = isEqual && pn.TypeLength == other.TypeLength
	}
	return isEqual
}

// ----------------------------------------------------------------------
// Group node

type GroupNode struct {
	node
	Fields []Node
	// Mapping between field name to the field index
	FieldNameToIdx *util.UnorderedMultimapStringInt
}

// ----------------------------------------------------------------------
// Node construction from Parquet metadata

func NewGroupNodeFromParquet(opaqueElement interface{}, nodeId int, fields []Node) (Node, error) {
	element := opaqueElement.(*parquet.SchemaElement)

	var groupNode *GroupNode
	if element.IsSetLogicalType() {
		logicalType, err := LogicalTypeFromThrift(*element.GetLogicalType())
		if err != nil {
			return nil, err
		}
		// updated writer with logical type present
		gn, err := NewGroupNodeFromLogicalType(
			element.GetName(),
			RepetitionTypeFromThrift(element.GetRepetitionType()),
			fields,
			logicalType,
			nodeId)
		if err != nil {
			return nil, err
		}
		groupNode = gn
	} else {
		var convertedType ConvertedType
		if element.IsSetConvertedType() {
			convertedType = ConvertedTypeFromThrift(element.GetConvertedType())
		} else {
			convertedType = ConvertedType_NONE
		}
		gn, err := NewGroupNodeFromConvertedType(
			element.GetName(),
			RepetitionTypeFromThrift(element.GetRepetitionType()),
			fields,
			convertedType,
			nodeId,
		)
		if err != nil {
			return nil, err
		}
		groupNode = gn
	}

	return groupNode, nil
}

func NewGroupNodeFromConvertedType(name string, repetitionType RepetitionType,
	fields []Node, convertedType ConvertedType, id int) (*GroupNode, error) {

	gn := &GroupNode{
		node:           *newNodeFromConvertedType(NodeType_GROUP, name, repetitionType, convertedType, id),
		Fields:         fields,
		FieldNameToIdx: util.NewUnorderedMultimapStringInt(),
	}

	// For forward compatibility, create an equivalent logical type
	lt, err := NewLogicalTypeFromConvertedType(
		gn.convertedType,
		DecimalMetadata{isSet: false, scale: -1, precision: -1},
	)
	if err != nil {
		return gn, err
	}
	gn.logicalType = lt
	debug.Assert(gn.logicalType != nil && (gn.logicalType.isNested() || gn.logicalType.isNone()) &&
		gn.logicalType.isCompatible(
			gn.convertedType,
			DecimalMetadata{isSet: false, scale: -1, precision: -1}),
		"NewGroupNodeFromConvertedType: Invalid LogicalType",
	)
	for fieldIdx, field := range gn.Fields {
		field.SetParent(gn)
		gn.FieldNameToIdx.Emplace(field.Name(), fieldIdx)
	}

	return gn, nil
}

func NewGroupNodeFromLogicalType(name string, repetitionType RepetitionType,
	fields []Node, logicalType LogicalType, id int) (*GroupNode, error) {

	gn := &GroupNode{
		node:           *newNodeFromLogicalType(NodeType_GROUP, name, repetitionType, logicalType, id),
		Fields:         fields,
		FieldNameToIdx: util.NewUnorderedMultimapStringInt(),
	}

	if gn.logicalType != nil {
		// Check for logical type <=> node type consistency
		if gn.logicalType.isNested() {
			// For backward compatibility, assign equivalent legacy converted type (if possible)
			gn.convertedType = gn.logicalType.ToConvertedType(nil)
		} else {
			ltStr, err := gn.logicalType.ToString()
			if err != nil {
				return nil, err
			}
			return gn, fmt.Errorf(
				"Logical type %s can not be applied to group node: %w",
				ltStr,
				ParquetException,
			)
		}
	} else {
		gn.logicalType = NewNoLogicalType()
		gn.convertedType = gn.logicalType.ToConvertedType(nil)
	}
	debug.Assert(gn.logicalType != nil && (gn.logicalType.isNested() || gn.logicalType.isNone()) &&
		gn.logicalType.isCompatible(
			gn.convertedType,
			DecimalMetadata{isSet: false, scale: -1, precision: -1}),
		"NewGroupNodeFromLogicalType: Invalid LogicalType",
	)
	for fieldIdx, field := range gn.Fields {
		field.SetParent(gn)
		gn.FieldNameToIdx.Emplace(field.Name(), fieldIdx)
	}

	return gn, nil
}

func (gn *GroupNode) Equals(other Node) bool {
	if !gn.node.Equals(other) {
		return false
	}
	otherGn, ok := other.(*GroupNode)
	return ok && gn.EqualsInternal(otherGn)
}

func (gn *GroupNode) Field(i int) Node {
	return gn.Fields[i]
}

// FieldIndexByName gets the index of a field by its name, or negative value if not found. If
// several fields share the same name, it is unspecified which one is returned.
func (gn *GroupNode) FieldIndexByName(name string) int {
	value, found := gn.FieldNameToIdx.Find(name)
	if !found {
		return -1
	}
	return value
}

// FieldIndexByNode gets the index of a field by its node, or negative value if not found.
func (gn *GroupNode) FieldIndexByNode(node Node) int {
	values, found := gn.FieldNameToIdx.Get(node.Name())
	if !found {
		return -1
	}
	for _, idx := range values {
		if node == gn.Field(idx) {
			return idx
		}
	}
	return -1
}

func (gn *GroupNode) FieldCount() int {
	return len(gn.Fields)
}

func (gn *GroupNode) ToParquet(opaqueElement interface{}) error {
	element := opaqueElement.(*parquet.SchemaElement)
	element.Name = gn.name
	fieldCount := int32(gn.FieldCount())
	element.NumChildren = &fieldCount
	repetitionType := RepetitionTypeToThrift(gn.repetitionType)
	element.RepetitionType = &repetitionType
	if gn.convertedType != ConvertedType_NONE {
		convertedType := ConvertedTypeToThrift(gn.convertedType)
		element.ConvertedType = &convertedType
	}
	if gn.logicalType != nil && gn.logicalType.isSerialized() {
		logicalType, err := gn.logicalType.ToThrift()
		if err != nil {
			return err
		}
		element.LogicalType = logicalType
	}
	return nil
}

func (gn *GroupNode) Visit(visitor NodeVisitor) error {
	return visitor.Visit(gn)
}

func (gn *GroupNode) VisitConst(visitor ConstVisitor) error {
	return visitor.Visit(gn)
}

func (gn *GroupNode) EqualsInternal(other *GroupNode) bool {
	// If the pointers are equal.
	if gn == other {
		return true
	}
	if gn.FieldCount() != other.FieldCount() {
		return false
	}
	for i := 0; i < gn.FieldCount(); i++ {
		if !gn.Field(i).Equals(other.Field(i)) {
			return false
		}
	}
	return true
}

const printSchemaDefaultIdentWidth = 2

func PrintSchema(schema Node, stream io.Writer, indentWidth int) error {
	printer := NewSchemaPrinter(stream, indentWidth)
	return printer.Visit(schema)
}

// ColumnDescriptor encapsulates information necessary to interpret
// primitive column data in the context of a particular schema. We have to
// examine the node structure of a column's path to the root in the schema tree
// to be able to reassemble the nested structure from the repetition and
// definition levels.
type ColumnDescriptor struct {
	Node               Node
	PrimitiveNode      *PrimitiveNode
	MaxDefinitionLevel int16
	MaxRepetitionLevel int16
}

func NewColumnDescriptor(node Node,
	maxDefinitionLevel int16, maxRepetitionLevel int16,
	SchemaDescriptor *SchemaDescriptor) (*ColumnDescriptor, error) {

	if !node.isPrimitive() {
		return nil, fmt.Errorf("Must be a primitive type: %w", ParquetException)
	}
	pn, ok := node.(*PrimitiveNode)
	if !ok {
		return nil, fmt.Errorf("Must be a primitive type. Is: %T: %w", node, ParquetException)
	}

	return &ColumnDescriptor{
		Node:               node,
		PrimitiveNode:      pn,
		MaxDefinitionLevel: maxDefinitionLevel,
		MaxRepetitionLevel: maxRepetitionLevel,
	}, nil
}

func (cd *ColumnDescriptor) Equals(other *ColumnDescriptor) bool {
	return cd.PrimitiveNode.Equals(other.PrimitiveNode) &&
		cd.MaxRepetitionLevel == other.MaxRepetitionLevel &&
		cd.MaxDefinitionLevel == other.MaxDefinitionLevel
}

func (cd *ColumnDescriptor) PhysicalType() Type {
	return cd.PrimitiveNode.PhysicalType
}

func (cd *ColumnDescriptor) ConvertedType() ConvertedType {
	return cd.PrimitiveNode.ConvertedType()
}

func (cd *ColumnDescriptor) LogicalType() LogicalType {
	return cd.PrimitiveNode.LogicalType()
}

func (cd *ColumnDescriptor) ColumnOrder() ColumnOrder {
	return cd.PrimitiveNode.ColumnOrder
}

func (cd *ColumnDescriptor) SortOrder() SortOrder {
	la := cd.LogicalType()
	pt := cd.PhysicalType()
	if la != nil {
		return GetSortOrderLogical(la, pt)
	} else {
		return GetSortOrderConverted(cd.ConvertedType(), pt)
	}
}

func (cd *ColumnDescriptor) Name() string {
	return cd.PrimitiveNode.Name()
}

func (cd *ColumnDescriptor) path() ColumnPath {
	return cd.PrimitiveNode.path()
}

func (cd *ColumnDescriptor) schemaNode() Node {
	return cd.Node
}

func (cd *ColumnDescriptor) ToString() (string, error) {
	var b strings.Builder
	fmt.Fprint(&b, "column descriptor = {\n")
	fmt.Fprintf(&b, "  name: %s,\n", cd.Name())
	fmt.Fprintf(&b, "  path: %s,\n", cd.path().ToDotString())
	fmt.Fprintf(&b, "  physical_type: %s,\n", TypeToString(cd.PhysicalType()))
	fmt.Fprintf(&b, "  converted_type: %s,\n", ConvertedTypeToString(cd.ConvertedType()))
	str, err := cd.LogicalType().ToString()
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "  logical_type: %s,\n", str)
	fmt.Fprintf(&b, "  max_definition_level: %d,\n", cd.MaxDefinitionLevel)
	fmt.Fprintf(&b, "  max_repetition_level: %d,\n", cd.MaxRepetitionLevel)

	if cd.PhysicalType() == Type_FIXED_LEN_BYTE_ARRAY {
		fmt.Fprintf(&b, "  length: %d,\n", cd.typeLength())
	}

	if cd.ConvertedType() == ConvertedType_DECIMAL {
		fmt.Fprintf(&b, "  precision: %d,\n", cd.typePrecision())
		fmt.Fprintf(&b, "  scale: %d,\n", cd.typeScale())
	}

	fmt.Fprint(&b, "}")
	return b.String(), nil
}

func (cd *ColumnDescriptor) typeLength() int {
	return cd.PrimitiveNode.TypeLength
}

func (cd *ColumnDescriptor) typePrecision() int {
	return cd.PrimitiveNode.DecimalMetadata.precision
}

func (cd *ColumnDescriptor) typeScale() int {
	return cd.PrimitiveNode.DecimalMetadata.scale
}

type SchemaUpdater struct {
	columnOrders []ColumnOrder
	leafCount    int
}

func NewSchemaUpdater() *SchemaUpdater {
	return &SchemaUpdater{}
}

func (s *SchemaUpdater) Visit(node Node) error {
	if node.isGroup() {
		gn := node.(*GroupNode)
		for i := 0; i < gn.FieldCount(); i++ {
			if err := gn.Field(i).Visit(s); err != nil {
				return err
			}
		}
	} else {
		// leaf node
		leafNode := node.(*PrimitiveNode)
		leafNode.ColumnOrder = s.columnOrders[s.leafCount]
		s.leafCount++
	}
	return nil
}

// Container for the converted Parquet schema with a computed information from
// the schema analysis needed for file reading
//
// * Column index to Node
// * Max repetition / definition levels for each primitive node
//
// The ColumnDescriptor objects produced by this class can be used to assist in
// the reconstruction of fully materialized data structures from the
// repetition-definition level encoding of nested data

type SchemaDescriptor struct {
	// Root Node
	Schema Node

	// Root Node
	GroupNode *GroupNode

	// Result of leaf node / tree analysis
	Leaves []ColumnDescriptor

	NodeToLeafIndex map[*PrimitiveNode]int

	// Mapping between leaf nodes and root group of leaf (first node
	// below the schema's root group)
	//
	// For example, the leaf `a.b.c.d` would have a link back to `a`
	//
	// -- a  <------
	// -- -- b     |
	// -- -- -- c  |
	// -- -- -- -- d
	LeafToBase map[int]Node

	// Mapping between ColumnPath DotString to the leaf index
	LeafToIdx *util.UnorderedMultimapStringInt
}

func NewSchemaDescriptor(schema Node) (*SchemaDescriptor, error) {
	sd := &SchemaDescriptor{
		NodeToLeafIndex: make(map[*PrimitiveNode]int),
		LeafToBase:      make(map[int]Node),
		LeafToIdx:       util.NewUnorderedMultimapStringInt(),
	}
	err := sd.Init(schema)
	return sd, err
}

func (sd *SchemaDescriptor) Column(i int) *ColumnDescriptor {
	debug.Assert(i >= 0 && i < len(sd.Leaves), "SchemaDescriptor: Column: index out of bounds")
	l := sd.Leaves[i]
	return &l
}

// Analyze the schema
func (sd *SchemaDescriptor) Init(schema Node) error {
	sd.Schema = schema
	if !sd.Schema.isGroup() {
		return fmt.Errorf("Must initialize with a schema group: %w", ParquetException)
	}
	gn, ok := sd.Schema.(*GroupNode)
	if !ok {
		return fmt.Errorf("Must initialize with a schema group. Is: %T: %w", sd.Schema, ParquetException)
	}
	sd.GroupNode = gn
	sd.Leaves = make([]ColumnDescriptor, 0, 1)
	for i := 0; i < sd.GroupNode.FieldCount(); i++ {
		if err := sd.BuildTree(sd.GroupNode.Field(i), 0, 0, sd.GroupNode.Field(i)); err != nil {
			return err
		}
	}

	return nil
}

func (sd *SchemaDescriptor) Equals(other *SchemaDescriptor) bool {
	if sd.numColumns() != other.numColumns() {
		return false
	}

	for i := 0; i < sd.numColumns(); i++ {
		if !sd.Column(i).Equals(other.Column(i)) {
			return false
		}
	}

	return true
}

// The number of physical columns appearing in the file
func (sd *SchemaDescriptor) numColumns() int {
	return len(sd.Leaves)
}

func (sd *SchemaDescriptor) schemaRoot() Node {
	return sd.Schema
}

// Returns the root (child of the schema root) node of the leaf(column) node
func (sd *SchemaDescriptor) GetColumnRoot(i int) Node {
	debug.Assert(i >= 0 && i < len(sd.Leaves), "SchemaDescriptor: GetColumnRoot: index out of bounds")
	return sd.LeafToBase[i]
}

func (sd *SchemaDescriptor) name() string {
	return sd.GroupNode.Name()
}

func (sd *SchemaDescriptor) ToString() (string, error) {
	var b strings.Builder
	err := PrintSchema(sd.Schema, &b, printSchemaDefaultIdentWidth)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func (sd *SchemaDescriptor) updateColumnOrders(columnOrders []ColumnOrder) error {
	if len(columnOrders) != sd.numColumns() {
		return fmt.Errorf("Malformed schema: not enough ColumnOrder values: %w", ParquetException)
	}
	visitor := NewSchemaUpdater()
	return sd.GroupNode.Visit(visitor)
}

// GetColumnIndexByNode returns the column index corresponding to a particular
// PrimitiveNode. Returns -1 if not found.
func (sd *SchemaDescriptor) GetColumnIndexByNode(node *PrimitiveNode) int {
	it, found := sd.NodeToLeafIndex[node]
	if !found {
		// Not found
		return -1
	}
	return it
}

// GetColumnIndexByNodePath returns the column index corresponding to a particular
// node path. Returns -1 if not found.
func (sd *SchemaDescriptor) GetColumnIndexByNodePath(nodePath string) int {
	it, found := sd.LeafToIdx.Find(nodePath)
	if !found {
		// Not found
		return -1
	}
	return it
}

func (sd *SchemaDescriptor) BuildTree(node Node, maxDefLevel int16,
	maxRepLevel int16, base Node) error {

	if node.isOptional() {
		maxDefLevel++
	} else if node.isRepeated() {
		// Repeated fields add a definition level. This is used to distinguish
		// between an empty list and a list with an item in it.
		maxRepLevel++
		maxDefLevel++
	}

	// Now, walk the schema and create a ColumnDescriptor for each leaf node
	if node.isGroup() {
		group := node.(*GroupNode)
		for i := 0; i < group.FieldCount(); i++ {
			sd.BuildTree(group.Field(i), maxDefLevel, maxRepLevel, base)
		}
	} else {
		pn, ok := node.(*PrimitiveNode)
		if !ok {
			return fmt.Errorf("node should be *PrimitiveNode but it is: %T: %w", node, ParquetException)
		}
		sd.NodeToLeafIndex[pn] = len(sd.Leaves)

		// Primitive node, append to leaves
		cd, err := NewColumnDescriptor(node, maxDefLevel, maxRepLevel, sd)
		if err != nil {
			return err
		}

		sd.Leaves = append(sd.Leaves, *cd)
		sd.LeafToBase[len(sd.Leaves)-1] = base
		sd.LeafToIdx.Emplace(node.path().ToDotString(), len(sd.Leaves)-1)
	}

	return nil
}

// ----------------------------------------------------------------------
// Schema printing

type SchemaPrinter struct {
	ConstVisitor

	stream io.Writer

	indent      int
	indentWidth int
}

func NewSchemaPrinter(stream io.Writer, indentWidth int) *SchemaPrinter {
	return &SchemaPrinter{
		stream:      stream,
		indentWidth: 2, // C++ version has this hard coded to 2
	}
}

func (s *SchemaPrinter) Visit(node Node) error {
	if err := s.Indent(); err != nil {
		return err
	}
	if node.isGroup() {
		groupNode, ok := node.(*GroupNode)
		if !ok {
			debug.Warn(fmt.Sprintf("node is supposed to be a group node but it is: %T", node))
		}
		return s.visitGroupNode(groupNode)
	} else {
		// Primitive
		primitiveNode, ok := node.(*PrimitiveNode)
		if !ok {
			debug.Warn(fmt.Sprintf("node is supposed to be a primitive node but it is: %T", node))
		}
		return s.visitPrimitiveNode(primitiveNode)
	}
}

func (s *SchemaPrinter) Indent() error {
	if s.indent > 0 {
		_, err := fmt.Fprint(s.stream, strings.Repeat(" ", s.indent))
		return err
	}
	return nil
}

func (s *SchemaPrinter) visitGroupNode(node *GroupNode) error {
	if node.Parent() == nil {
		if _, err := fmt.Fprintf(s.stream, "message %s {\n", node.Name()); err != nil {
			return err
		}
	} else {
		if err := PrintRepLevel(node.RepetitionType(), s.stream); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(s.stream, " group %s", node.Name()); err != nil {
			return err
		}
		lt := node.ConvertedType()
		la := node.LogicalType()
		if la != nil && la.isValid() && !la.isNone() {
			laStr, err := la.ToString()
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(s.stream, " (%s)", laStr); err != nil {
				return err
			}
		} else if lt != ConvertedType_NONE {
			if _, err := fmt.Fprintf(s.stream, " (%s)", ConvertedTypeToString(lt)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(s.stream, " {\n"); err != nil {
			return err
		}
	}

	s.indent = s.indent + s.indentWidth
	for i := 0; i < node.FieldCount(); i++ {
		if err := node.Field(i).VisitConst(s); err != nil {
			return err
		}
	}
	s.indent = s.indent - s.indentWidth
	if err := s.Indent(); err != nil {
		return err
	}
	_, err := fmt.Fprint(s.stream, "}\n")
	return err
}

func (s *SchemaPrinter) visitPrimitiveNode(node *PrimitiveNode) error {
	if err := PrintRepLevel(node.RepetitionType(), s.stream); err != nil {
		return err
	}
	if _, err := fmt.Fprint(s.stream, " "); err != nil {
		return err
	}
	if err := PrintType(node, s.stream); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.stream, " %s", node.Name()); err != nil {
		return err
	}
	if err := PrintConvertedType(node, s.stream); err != nil {
		return err
	}
	_, err := fmt.Fprintln(s.stream, ";")
	return err
}

func PrintRepLevel(repetition RepetitionType, stream io.Writer) error {
	var str string
	switch repetition {
	case RepetitionType_REQUIRED:
		str = "required"
	case RepetitionType_OPTIONAL:
		str = "optional"
	case RepetitionType_REPEATED:
		str = "repeated"
	default:
		return nil
	}

	_, err := stream.Write([]byte(str))
	return err
}

func PrintType(node *PrimitiveNode, stream io.Writer) error {
	var str string
	switch node.PhysicalType {
	case Type_BOOLEAN:
		str = "boolean"
	case Type_INT32:
		str = "int32"
	case Type_INT64:
		str = "int64"
	case Type_INT96:
		str = "int96"
	case Type_FLOAT:
		str = "float"
	case Type_DOUBLE:
		str = "double"
	case Type_BYTE_ARRAY:
		str = "binary"
	case Type_FIXED_LEN_BYTE_ARRAY:
		str = fmt.Sprintf("fixed_len_byte_array(%d)", node.TypeLength)
	default:
		return nil
	}

	_, err := stream.Write([]byte(str))
	return err
}

func PrintConvertedType(node *PrimitiveNode, stream io.Writer) error {
	lt := node.ConvertedType()
	la := node.LogicalType()
	if la != nil && la.isValid() && !la.isNone() {
		laStr, strErr := la.ToString()
		if strErr != nil {
			return strErr
		}
		_, err := stream.Write([]byte(fmt.Sprintf(" (%s)", laStr)))
		return err
	} else if lt == ConvertedType_DECIMAL {
		_, err := stream.Write(
			[]byte(fmt.Sprintf(" (%s(%d,%d))",
				ConvertedTypeToString(lt),
				node.DecimalMetadata.precision,
				node.DecimalMetadata.scale)),
		)
		return err
	} else if lt != ConvertedType_NONE {
		_, err := stream.Write(
			[]byte(fmt.Sprintf(" (%s)", ConvertedTypeToString(lt))),
		)
		return err
	}
	return nil
}
