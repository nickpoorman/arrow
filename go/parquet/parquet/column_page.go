package parquet

import "github.com/apache/arrow/go/arrow/memory"

// TODO(nickpoorman): Verify this holds true for this implementation.
// TODO: Parallel processing is not yet safe because of memory-ownership
// semantics (the PageReader may or may not own the memory referenced by a
// page)

// TODO(nickpoorman): In the future Parquet implementations may store the crc code
// in format::PageHeader. parquet-mr currently does not, so we also skip it
// here, both on the read and write path
type Page struct {
	buffer *memory.Buffer
	type_  PageType
}

func NewPage(buffer *memory.Buffer, t PageType) *Page {
	return &Page{
		buffer: buffer,
		type_:  t,
	}
}

func (p *Page) Type() PageType { return p.type_ }

func (p *Page) Buffer() *memory.Buffer { return p.buffer }

// returns a pointer to the page's data
func (p *Page) Data() []byte { return p.buffer.Buf() }

// returns the total size in bytes of the page's data buffer
func (p *Page) Size() int { return p.buffer.Len() }

type DataPage struct {
	*Page

	numValues        int32
	encoding         EncodingType
	uncompressedSize int64
	statistics       *EncodedStatistics
}

func NewDataPage(t PageType, buffer *memory.Buffer, numValues int32,
	encoding EncodingType, uncompressedSize int64, statistics *EncodedStatistics) *DataPage {
	if statistics == nil {
		statistics = NewEncodedStatistics()
	}
	return &DataPage{
		Page: NewPage(buffer, t),

		numValues:        numValues,
		encoding:         encoding,
		uncompressedSize: uncompressedSize,
		statistics:       statistics,
	}
}

func (d *DataPage) NumValues() int32               { return d.numValues }
func (d *DataPage) Encoding() EncodingType         { return d.encoding }
func (d *DataPage) UncompressedSize() int64        { return d.uncompressedSize }
func (d *DataPage) Statistics() *EncodedStatistics { return d.statistics }

type DataPageV1 struct {
	*DataPage

	definitionLevelEncoding EncodingType
	repetitionLevelEncoding EncodingType
}

func NewDataPageV1(t PageType, buffer *memory.Buffer, numValues int32,
	encoding EncodingType, definitionLevelEncoding EncodingType,
	repetitionLevelEncoding EncodingType, uncompressedSize int64,
	statistics *EncodedStatistics) *DataPageV1 {
	if statistics == nil {
		statistics = NewEncodedStatistics()
	}
	return &DataPageV1{
		DataPage: NewDataPage(
			PageType_DATA_PAGE, buffer, numValues,
			encoding, uncompressedSize, statistics,
		),

		definitionLevelEncoding: definitionLevelEncoding,
		repetitionLevelEncoding: repetitionLevelEncoding,
	}
}

func (d *DataPageV1) RepetitionLevelEncoding() EncodingType {
	return d.repetitionLevelEncoding
}

func (d *DataPageV1) DefinitionLevelEncoding() EncodingType {
	return d.definitionLevelEncoding
}

type DataPageV2 struct {
	*DataPage

	numNulls                   int32
	numRows                    int32
	definitionLevelsByteLength int32
	repetitionLevelsByteLength int32
	isCompressed               bool
}

func NewDataPageV2(buffer *memory.Buffer, numValues int32, numNulls int32,
	numRows int32, encoding EncodingType,
	definitionLevelsByteLength int32, repetitionLevelsByteLength int32,
	uncompressedSize int64, isCompressed bool,
	statistics *EncodedStatistics) *DataPageV2 {
	if statistics == nil {
		statistics = NewEncodedStatistics()
	}
	return &DataPageV2{
		DataPage: NewDataPage(
			PageType_DATA_PAGE_V2, buffer, numValues,
			encoding, uncompressedSize, statistics,
		),

		numNulls:                   numNulls,
		numRows:                    numRows,
		definitionLevelsByteLength: definitionLevelsByteLength,
		repetitionLevelsByteLength: repetitionLevelsByteLength,
		isCompressed:               isCompressed,
	}
}

func (d *DataPageV2) NumNulls() int32 { return d.numNulls }
func (d *DataPageV2) NumRows() int32  { return d.numRows }

func (d *DataPageV2) DefinitionLevelsByteLength() int32 {
	return d.definitionLevelsByteLength
}

func (d *DataPageV2) RepetitionLevelsByteLength() int32 {
	return d.repetitionLevelsByteLength
}

func (d *DataPageV2) IsCompressed() bool { return d.isCompressed }

type DictionaryPage struct {
	*Page

	numValues int32
	encoding  EncodingType
	isSorted  bool
}

func NewDictionaryPage(buffer *memory.Buffer, numValues int32,
	encoding EncodingType, isSorted bool) *DictionaryPage {
	return &DictionaryPage{
		Page: NewPage(buffer, PageType_DICTIONARY_PAGE),

		numValues: numValues,
		encoding:  encoding,
		isSorted:  isSorted,
	}
}

func (d *DictionaryPage) NumValues() int32       { return d.numValues }
func (d *DictionaryPage) Encoding() EncodingType { return d.encoding }
func (d *DictionaryPage) IsSorted() bool         { return d.isSorted }
