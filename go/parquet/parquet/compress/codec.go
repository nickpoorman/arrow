package compress

// const kUseDefaultCompressionLevel = math.MinInt64

// // Compression codec
// type Codec interface {
// 	// Return special value to indicate that a codec implementation should use
// 	// its default compression level
// 	UseDefaultCompressionLevel() int

// 	// Return a string name for compression type
// 	GetCodecAsString(t compress.CompressionType) string

// 	// Create a codec for the given compression algorithm
// 	Create(codec compress.CompressionType, compressionLevel int)

// 	// Return true if support for indicated codec has been enabled
// 	IsAvailable(codec compress.CompressionType) bool

// 	// One-shot decompression function
// 	//
// 	// outputBufferLen must be correct and therefore be obtained in advance.
// 	// The actual decompressed length is returned.
// 	//
// 	// Note: One-shot decompression is not always compatible with streaming
// 	// compression. Depending on the codec (e.g. LZ4), different formats may
// 	// be used.
// 	Decompress(inputLen int64, input *uint8, outputBufferLen int64, outputBuffer uint8) (int64, error)

// 	// One-shot compression function
// 	//
// 	// outputBufferLen must first have been computed using MaxCompressedLen().
// 	// The actual compressed length is returned.
// 	//
// 	// Note: One-shot compression is not always compatible with streaming
// 	// decompression.  Depending on the codec (e.g. LZ4), different formats may
// 	// be used.
// 	Compress(inputLen int64, input *uint8, outputBufferLen int64, outputBuffer *uint8) (int64, error)

// 	MaxCompressedLen(inputLen int64, input *uint8) int64

// 	// Create a streaming compressor instance
// 	MakeCompressor() (*Compressor, error)

// 	MakeDecompressor() (*Decompressor, error)

// 	name() string

// 	// Initializes the codec's resources.
// 	Init() error
// }

// Return special value to indicate that a codec implementation should use
// its default compression level
func UseDefaultCompressionLevel() int {
	return kUseDefaultCompressionLevel
}

// // Return special value to indicate that a codec implementation should use
// // its default compression level
// func UseDefaultCompressionLevel() int

// // Return a string name for compression type
// func GetCodecAsString(t compress.Compression) string

// // Create a codec for the given compression algorithm
// func (c *Codec) Create(codec Compression, compressionLevel int)

// // Return true if support for indicated codec has been enabled
// func (c *Codec) IsAvailable(codec Compression) bool

// // One-shot decompression function
// //
// // outputBufferLen must be correct and therefore be obtained in advance.
// // The actual decompressed length is returned.
// //
// // Note: One-shot decompression is not always compatible with streaming
// // compression. Depending on the codec (e.g. LZ4), different formats may
// // be used.
// func (c *Codec) Decompress(inputLen int64, input *uint8, outputBufferLen int64, outputBuffer uint8) (int64, error)

// // One-shot compression function
// //
// // outputBufferLen must first have been computed using MaxCompressedLen().
// // The actual compressed length is returned.
// //
// // Note: One-shot compression is not always compatible with streaming
// // decompression.  Depending on the codec (e.g. LZ4), different formats may
// // be used.
// func (c *Codec) Compress(inputLen int64, input *uint8, outputBufferLen int64, outputBuffer *uint8) (int64, error)

// func (c *Codec) MaxCompressedLen(inputLen int64, input *uint8) int64

// // Create a streaming compressor instance
// func (c *Codec) MakeCompressor() (*Compressor, error)

// func (c *Codec) MakeDecompressor() (*Decompressor, error)

// func (c *Codec) name() string

// // Initializes the codec's resources.
// func (c *Codec) Init() error
