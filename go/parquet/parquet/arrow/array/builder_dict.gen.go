package array

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// ----------------------------------------------------------------------
// Dictionary builder

//+ {{range .In}}
type DictionaryScalar struct {
}

//+ {{- end}}

type DictionaryMemoTable struct {
	impl dictionarymemoTableImpl
}

func NewDictionaryMemoTable(pool memory.Allocator, dataType arrow.DataType) *DictionaryMemoTable {
	return &DictionaryMemoTable{}
}

func (d *DictionaryMemoTable) GetOrInsertBool(value bool, out *int32)      {}
func (d *DictionaryMemoTable) GetOrInsertInt8(value int8, out *int32)      {}
func (d *DictionaryMemoTable) GetOrInsertInt16(value int16, out *int32)    {}
func (d *DictionaryMemoTable) GetOrInsertInt32(value int32, out *int32)    {}
func (d *DictionaryMemoTable) GetOrInsertInt64(value int64, out *int32)    {}
func (d *DictionaryMemoTable) GetOrInsertUint8(value uint8, out *int32)    {}
func (d *DictionaryMemoTable) GetOrInsertUint16(value uint16, out *int32)  {}
func (d *DictionaryMemoTable) GetOrInsertUint32(value uint32, out *int32)  {}
func (d *DictionaryMemoTable) GetOrInsertUint64(value uint64, out *int32)  {}
func (d *DictionaryMemoTable) GetOrInsertFloat(value float32, out *int32)  {}
func (d *DictionaryMemoTable) GetOrInsertDouble(value float64, out *int32) {}
func (d *DictionaryMemoTable) GetOrInsertString(value string, out *int32)  {}

func (d *DictionaryMemoTable) GetArrayData(startOffset, out *array.Data) {}

// Insert new memo values
func (d *DictionaryMemoTable) InsertValues(values array.Interface) {}

func (d *DictionaryMemoTable) Size() int32 {}

//+ {{range .In}}
// Array builder for created encoded DictionaryArray from
// dense array
//
// Unlike other builders, dictionary builder does not completely
// reset the state on Finish calls.
//+ type {{.BuilderType}}DictionaryBuilderBase struct {
type DictionaryBuilderBase struct {
	builder array.Builder
	builderType
}

func NewDictionaryBuilderBaseFixedSizeBinary(valueType arrow.DataType,
	pool memory.Allocator) *DictionaryBuilderBase {
	return &DictionaryBuilderBase{
		builder: array.NewRecordBuilder()
	}
}

//+ {{- end}}

// A DictionaryArray builder that always returns int32 dictionary
// indices so that data cast to dictionary form will have a consistent index
// type, e.g. for creating a ChunkedArray
