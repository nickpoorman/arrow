package parquet

import (
	"fmt"

	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/xitongsys/parquet-go/parquet"
)

// TODO(nickpoorman): Remove
// type SchemaElement struct {
// 	parquet.SchemaElement
// }

// ----------------------------------------------------------------------
// Conversion from Parquet Thrift metadata

func FromParquet(schema []parquet.SchemaElement) (*SchemaDescriptor, error) {
	converter := NewFlatSchemaConverter(schema, len(schema))
	root, err := converter.Convert()
	if err != nil {
		return nil, err
	}
	// sd.Init() is called inside NewSchemaDescriptor().
	return NewSchemaDescriptor(root)
}

func ToParquet(schema *GroupNode, out *[]parquet.SchemaElement) error {
	flattner := NewSchemaFlattener(schema, out)
	return flattner.Flatten()
}

// ----------------------------------------------------------------------
// Schema converters

type FlatSchemaConverter struct {
	elements  []parquet.SchemaElement
	length    int
	pos       int
	currentId int
}

func NewFlatSchemaConverter(elements []parquet.SchemaElement, length int) *FlatSchemaConverter {
	return &FlatSchemaConverter{
		elements:  elements,
		length:    length,
		pos:       0,
		currentId: 0,
	}
}

func (f *FlatSchemaConverter) Convert() (Node, error) {
	if len(f.elements) < 1 {
		return nil, fmt.Errorf("FlatSchemaConverter has no elements: %w", ParquetException)
	}
	root := f.elements[0]

	if root.GetNumChildren() == 0 {
		if f.length == 1 {
			// Degenerate case of Parquet file with no columns
			return NewGroupNodeFromParquet(&root, f.nextId(), []Node{})
		} else {
			return nil, fmt.Errorf("Parquet schema had multiple nodes but root had no children: %w", ParquetException)
		}
	}

	// Relaxing this restriction as some implementations don't set this
	// if root.RepetitionType != parquet.FieldRepetitionType_REPEATED {
	//   return nil, fmt.Errorf("Root node was not FieldRepetitionType_REPEATED: %w", ParquetException);
	// }

	return f.NextNode()
}

func (f *FlatSchemaConverter) Next() (*parquet.SchemaElement, error) {
	if f.pos == f.length {
		return nil, fmt.Errorf("Malformed schema: not enough SchemaElement values: %w", ParquetException)
	}
	e := f.elements[f.pos]
	f.pos++
	return &e, nil
}

func (f *FlatSchemaConverter) NextNode() (Node, error) {
	element, err := f.Next()
	if err != nil {
		return nil, err
	}

	nodeId := f.nextId()

	numChildren := int(element.GetNumChildren())

	if numChildren == 0 {
		// Leaf (primitive) node
		return NewPrimitiveNodeFromParquet(element, nodeId)
	} else {
		// Group
		fields := make([]Node, 0, numChildren)
		for i := 0; i < numChildren; i++ {
			field, err := f.NextNode()
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}
		return NewGroupNodeFromParquet(element, nodeId, fields)
	}
}

func (f *FlatSchemaConverter) nextId() int {
	f.currentId++
	return f.currentId
}

// ----------------------------------------------------------------------
// Conversion to Parquet Thrift metadata

func SchemaToParquet(schema *GroupNode, out []parquet.SchemaElement) error {
	panic("SchemaToParquet: implement this")
}

// Converts nested parquet schema back to a flat vector of Thrift structs
type SchemaFlattener struct {
	root     *GroupNode
	elements *[]parquet.SchemaElement
}

func NewSchemaFlattener(root *GroupNode, elements *[]parquet.SchemaElement) *SchemaFlattener {
	return &SchemaFlattener{
		root:     root,
		elements: elements,
	}
}

func (s *SchemaFlattener) Flatten() error {
	visitor := NewSchemaVisitor(s.elements)
	return s.root.VisitConst(visitor)
}

type SchemaVisitor struct {
	elements *[]parquet.SchemaElement
}

func NewSchemaVisitor(elements *[]parquet.SchemaElement) *SchemaVisitor {
	return &SchemaVisitor{
		elements: elements,
	}
}

func (s *SchemaVisitor) Visit(node Node) error {
	var element parquet.SchemaElement
	if err := node.ToParquet(&element); err != nil {
		return err
	}
	elms := append(*s.elements, element)
	*s.elements = elms

	if node.isGroup() {
		groupNode, ok := node.(*GroupNode)
		if !ok {
			debug.Warn(fmt.Sprintf("node is supposed to be a group node but it is: %T", node))
		}
		for i := 0; i < groupNode.FieldCount(); i++ {
			if err := groupNode.Field(i).VisitConst(s); err != nil {
				return err
			}
		}
	}
	return nil
}

// ----------------------------------------------------------------------
// Convenience primitive type factory functions

func PrimitiveFactoryBoolean(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_BOOLEAN, ConvertedType_NONE, -1, -1, -1, -1)
}

func PrimitiveFactoryInt32(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_INT32, ConvertedType_NONE, -1, -1, -1, -1)
}

func PrimitiveFactoryInt64(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_INT64, ConvertedType_NONE, -1, -1, -1, -1)
}

func PrimitiveFactoryInt96(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_INT96, ConvertedType_NONE, -1, -1, -1, -1)
}

func PrimitiveFactoryFloat(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_FLOAT, ConvertedType_NONE, -1, -1, -1, -1)
}

func PrimitiveFactoryDouble(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_DOUBLE, ConvertedType_NONE, -1, -1, -1, -1)
}

func PrimitiveFactoryByteArray(name string, repetition RepetitionType) (Node, error) {
	return NewPrimitiveNodeFromConvertedType(name, repetition, Type_BYTE_ARRAY, ConvertedType_NONE, -1, -1, -1, -1)
}
