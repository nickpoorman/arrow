package util

import (
	"fmt"
	"math"
	"reflect"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/nickpoorman/arrow-parquet-go/internal/bytearray"
	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
	"github.com/nickpoorman/arrow-parquet-go/internal/util"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
// (literal encoding).
// For both types of runs, there is a byte-aligned indicator which encodes the length
// of the run and the type of the run.
// This encoding has the benefit that when there aren't any long enough runs, values
// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
// the run length are byte aligned. This allows for very efficient decoding
// implementations.
// The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
//    literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
// Each run is preceded by a varint. The varint's least significant bit is
// used to indicate whether the run is a literal run or a repeated run. The rest
// of the varint is used to determine the length of the run (eg how many times the
// value repeats).
//
// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
// in groups of 8), so that no matter the bit-width of the value, the sequence will end
// on a byte boundary without padding.
// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
// the actual number of encoded ints. (This means that the total number of encoded values
// can not be determined from the encoded data, since the number of values in the last
// group may not be a multiple of 8). For the last group of literal runs, we pad
// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
// without the need for additional checks.
//
// There is a break-even point when it is more storage efficient to do run length
// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
// for both the repeated encoding or the literal encoding.  This value can always
// be computed based on the bit-width.
// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
// Examples with bit-width 1 (eg encoding booleans):
// ----------------------------------------
// 100 1s followed by 100 0s:
// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
//  - (total 4 bytes)
//
// alternating 1s and 0s (200 total):
// 200 ints = 25 groups of 8
// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// (total 26 bytes, 1 byte overhead)
//

// Decoder class for RLE encoded data.
type RleDecoder struct {
	bitReader *bitutilext.BitReader
	// Number of bits needed to encode the value. Must be between 0 and 64.
	bitWidth int
	// currentValue uint64
	currentValue [8]byte
	repeatCount  int
	literalCount int
}

// Create a decoder object. buffer/buffer_len is the decoded data.
// bit_width is the width of each value (before encoding).
func NewRleDecoder(buffer []byte, bufferLen int, bitWidth int) *RleDecoder {
	return &RleDecoder{
		bitReader: bitutilext.NewBitReader(buffer, bufferLen),
		bitWidth:  bitWidth,
	}
}

func (d *RleDecoder) Reset(buffer []byte, bufferLen int, bitWidth int) {
	debug.Assert(bitWidth >= 0, "Assert: bitWidth >= 0")
	debug.Assert(bitWidth <= 64, "Assert: bitWidth <= 64")
	d.bitReader.Reset(buffer, bufferLen)
	d.bitWidth = bitWidth
	// d.currentValue = 0
	for i := 0; i < len(d.currentValue); i++ {
		d.currentValue[i] = 0
	}
	d.repeatCount = 0
	d.literalCount = 0
}

func (d *RleDecoder) NextCounts() bool {
	// Read the next run's indicator int, it could be a literal or repeated run.
	// The int is encoded as a vlq-encoded value.
	indicatorValue, result := d.bitReader.GetVlqInt()
	if !result {
		fmt.Println("NextCounts - no result")
		return false
	}
	fmt.Printf("NextCounts - indicatorValue: %d\n", indicatorValue)

	// lsb indicates if it is a literal run or repeated run
	isLiteral := (indicatorValue & 1) != 0
	count := indicatorValue >> 1
	fmt.Printf("NextCounts -- isLiteral: %t\n", isLiteral)
	if isLiteral {
		if count == 0 || count > uint32(math.MaxInt32/8) {
			return false
		}
		d.literalCount = int(count * 8)
		fmt.Printf("NextCounts - d.literalCount: %d\n", d.literalCount)
	} else {
		if count == 0 || count > uint32(math.MaxInt32) {
			return false
		}
		d.repeatCount = int(count)
		fmt.Printf("NextCounts - d.repeatCount: %d\n", d.repeatCount)
		// XXX (ARROW-4018) this is not big-endian compatible
		// var buffer [8]byte
		// binary.LittleEndian.PutUint64(buffer[:], d.currentValue)
		// fmt.Printf("buffer was: %#b\n", buffer)
		result := d.bitReader.GetAligned(
			int(bitutilext.CeilDiv(int64(d.bitWidth), 8)),
			// buffer[:],
			d.currentValue[:],
		)
		if !result {
			fmt.Println("no result")
			return false
		}
		// fmt.Printf("buffer is now: %#b\n", buffer)
		// d.currentValue = binary.LittleEndian.Uint64(buffer[:])
		fmt.Printf("Set d.currentValue = %d\n", d.currentValue)
	}
	return true
}

// // Gets the next value.  Returns false if there are no more.
// func (d *RleDecoder) Get(v interface{}) bool {
// 	// Ensure is a slice
// 	value := reflect.ValueOf(v)
// 	elem := value.Elem()
// 	typ := elem.Type()
// 	switch typ.Kind() {
// 	case reflect.Slice, reflect.Array:
// 		return d.GetBatch(v, 1) == 1
// 	default:
// 		sl := reflect.MakeSlice(reflect.SliceOf(typ), 1, 1)
// 		sl.Index(0).Set(elem)
// 		results := d.GetBatch(sl.Interface(), 1) == 1
// 		value.Elem().Set(sl.Index(0)) // set the result of the first element in the slice back to v
// 		return results
// 	}
// }

// Gets the next value.  Returns false if there are no more.
func (d *RleDecoder) Get(v []byte) bool {
	return d.GetBatch(v, 1) == 1
}

// // Gets a batch of values.  Returns the number of decoded elements.
// func (d *RleDecoder) GetBatch(values interface{}, batchSize int) int {
// 	debug.Assert(d.bitWidth >= 0, "Assert: d.bitWidth >= 0")
// 	valuesRead := 0

// 	out := reflect.ValueOf(values)
// 	outOffset := 0

// 	fmt.Printf("valuesRead: %d | batchSize: %d\n", valuesRead, batchSize)

// 	for valuesRead < batchSize {
// 		remaining := batchSize - valuesRead

// 		fmt.Printf("remaining: %d | valuesRead-loop: %d | batchSize: %d\n", remaining, valuesRead, batchSize)
// 		if d.repeatCount > 0 {
// 			fmt.Printf("d.repeatCount > 0 | %d\n", d.repeatCount)

// 			repeatBatch := util.MinInt(remaining, d.repeatCount)
// 			fill(out, outOffset, repeatBatch, d.currentValue)

// 			d.repeatCount -= repeatBatch
// 			valuesRead += repeatBatch
// 			outOffset += repeatBatch
// 		} else if d.literalCount > 0 {
// 			fmt.Printf("d.literalCount > 0 | %d\n", d.literalCount)
// 			literalBatch := util.MinInt(remaining, d.literalCount)
// 			actualRead := d.bitReader.GetBatch(
// 				d.bitWidth,
// 				sliceFromOffset(out, outOffset).Interface(),
// 				literalBatch,
// 			)
// 			if actualRead != literalBatch {
// 				return valuesRead
// 			}

// 			d.literalCount -= literalBatch
// 			valuesRead += literalBatch
// 			outOffset += literalBatch
// 		} else {
// 			fmt.Println("else NextCounts")
// 			if !d.NextCounts() {
// 				return valuesRead
// 			}
// 		}
// 	}

// 	return valuesRead
// }

// Gets a batch of values.  Returns the number of decoded elements.
func (d *RleDecoder) GetBatch(buffer []byte, batchSize int) int {
	debug.Assert(d.bitWidth >= 0, "Assert: d.bitWidth >= 0")
	valuesRead := 0

	out := bytearray.NewByteArrayBits(buffer, d.bitWidth)
	// out := reflect.ValueOf(values) // buffer
	outOffset := 0

	fmt.Printf("valuesRead: %d | batchSize: %d\n", valuesRead, batchSize)

	for valuesRead < batchSize {
		remaining := batchSize - valuesRead

		fmt.Printf("remaining: %d | valuesRead-loop: %d | batchSize: %d\n", remaining, valuesRead, batchSize)
		if d.repeatCount > 0 {
			fmt.Printf("d.repeatCount > 0 | %d\n", d.repeatCount)

			repeatBatch := util.MinInt(remaining, d.repeatCount)
			out.FillBytes(outOffset, repeatBatch, d.currentValue[:])

			d.repeatCount -= repeatBatch
			valuesRead += repeatBatch
			outOffset += repeatBatch
		} else if d.literalCount > 0 {
			fmt.Printf("d.literalCount > 0 | %d\n", d.literalCount)
			literalBatch := util.MinInt(remaining, d.literalCount)
			actualRead := d.bitReader.GetBatch(
				d.bitWidth,
				// sliceFromOffset(out, outOffset).Interface(),
				out.Slice(outOffset),
				literalBatch,
			)
			if actualRead != literalBatch {
				return valuesRead
			}

			d.literalCount -= literalBatch
			valuesRead += literalBatch
			outOffset += literalBatch
		} else {
			fmt.Println("else NextCounts")
			if !d.NextCounts() {
				return valuesRead
			}
		}
	}

	return valuesRead
}

func fill(values reflect.Value, first, last int, value interface{}) {
	v := reflect.ValueOf(value)
	for ; first < last; first++ {
		el := values.Index(first)
		el.Set(v.Convert(el.Type()))
	}
}

func sliceFromOffset(slice reflect.Value, offset int) reflect.Value {
	return slice.Slice(offset, slice.Len())
}

// TODO(nickpoorman): We can probably speed this up if we generate the types
// instead of using reflect.

// // Like GetBatch but add spacing for null entries
// func (d *RleDecoder) GetBatchSpaced(batchSize int, nullCount int, validBits []byte,
// 	validBitsOffset int64, out interface{}) int {

// 	fmt.Printf("Called GetBatchSpaced: %d | nullCount: %d | validBitsOffset: %d | validBits: %#b\n",
// 		batchSize, nullCount, validBitsOffset, validBits,
// 	)
// 	debug.Assert(d.bitWidth >= 0, "Assert: d.bitWidth >= 0")
// 	valuesRead := 0
// 	remainingNulls := nullCount

// 	bitReader := bitutilext.NewBitmapReader(validBits, int(validBitsOffset), batchSize)

// 	outSlice := reflect.ValueOf(out)
// 	outOffset := 0
// 	zero := reflect.Zero(outSlice.Type().Elem())

// 	for valuesRead < batchSize {
// 		debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
// 		isValid := bitReader.IsSet()
// 		bitReader.Next()

// 		if isValid {
// 			if (d.repeatCount == 0) && (d.literalCount == 0) {
// 				if !d.NextCounts() {
// 					return valuesRead
// 				}
// 			}
// 			if d.repeatCount > 0 {
// 				// The current index is already valid, we don't need to check that again
// 				repeatBatch := 1
// 				d.repeatCount--

// 				for d.repeatCount > 0 && (valuesRead+repeatBatch) < batchSize {
// 					debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
// 					if bitReader.IsSet() {
// 						d.repeatCount--
// 					} else {
// 						remainingNulls--
// 					}
// 					repeatBatch++

// 					bitReader.Next()
// 				}
// 				fill(outSlice, outOffset, repeatBatch, d.currentValue)
// 				// rOut = sliceFromOffset(rOut, repeatBatch)
// 				outOffset += repeatBatch
// 				valuesRead += repeatBatch
// 			} else if d.literalCount > 0 {
// 				literalBatch := util.MinInt(batchSize-valuesRead-remainingNulls, d.literalCount)

// 				// Decode the literals
// 				const kBufferSize int = 1024
// 				// var indicies [kBufferSize]interface{}
// 				// actualRead := d.bitReader.GetBatch(d.bitWidth, indicies[:], literalBatch)
// 				indicies := reflect.MakeSlice(reflect.TypeOf(out), kBufferSize, kBufferSize)
// 				literalBatch = util.MinInt(literalBatch, kBufferSize)
// 				actualRead := d.bitReader.GetBatch(d.bitWidth, indicies.Interface(), literalBatch)
// 				debug.Assert(actualRead == literalBatch, "Assert: actualRead == literalBatch")

// 				skipped := 0
// 				literalsRead := 1
// 				// rOut.Index(0).Set(reflect.ValueOf(indicies[0]))
// 				outSlice.Index(outOffset).Set(indicies.Index(0))
// 				// rOut = sliceFromOffset(rOut, 1)
// 				outOffset++

// 				// Read the first bitset to the end
// 				for literalsRead < literalBatch {
// 					debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
// 					if bitReader.IsSet() {
// 						// rOut.Index(0).Set(reflect.ValueOf(indicies[literalsRead]))
// 						outSlice.Index(outOffset).Set(indicies.Index(literalsRead))
// 						literalsRead++
// 					} else {
// 						outSlice.Index(outOffset).Set(zero)
// 						skipped++
// 					}
// 					// rOut = sliceFromOffset(rOut, 1) // TODO(nickpoorman): Keep an index counter instead of slicing every time
// 					outOffset++
// 					bitReader.Next()
// 				}
// 				d.literalCount -= literalBatch
// 				valuesRead += literalBatch + skipped
// 				remainingNulls -= skipped
// 			}
// 		} else {
// 			outSlice.Index(outOffset).Set(zero)
// 			// rOut = sliceFromOffset(rOut, 1)
// 			outOffset++
// 			valuesRead++
// 			remainingNulls--
// 		}
// 	}

// 	return valuesRead
// }

// Like GetBatch but add spacing for null entries
func (d *RleDecoder) GetBatchSpaced(batchSize int, nullCount int, validBits []byte,
	validBitsOffset int64, out []byte) int {

	fmt.Printf("Called GetBatchSpaced: %d | nullCount: %d | validBitsOffset: %d | validBits: %#b\n",
		batchSize, nullCount, validBitsOffset, validBits,
	)
	debug.Assert(d.bitWidth >= 0, "Assert: d.bitWidth >= 0")
	valuesRead := 0
	remainingNulls := nullCount

	bitReader := bitutilext.NewBitmapReader(validBits, int(validBitsOffset), batchSize)

	// outSlice := reflect.ValueOf(out)
	outSlice := bytearray.NewByteArrayBits(out, d.bitWidth)

	outOffset := 0
	// zero := reflect.Zero(outSlice.Type().Elem())

	for valuesRead < batchSize {
		debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
		isValid := bitReader.IsSet()
		bitReader.Next()

		if isValid {
			if (d.repeatCount == 0) && (d.literalCount == 0) {
				if !d.NextCounts() {
					return valuesRead
				}
			}
			if d.repeatCount > 0 {
				// The current index is already valid, we don't need to check that again
				repeatBatch := 1
				d.repeatCount--

				for d.repeatCount > 0 && (valuesRead+repeatBatch) < batchSize {
					debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
					if bitReader.IsSet() {
						d.repeatCount--
					} else {
						remainingNulls--
					}
					repeatBatch++

					bitReader.Next()
				}
				// fill(outSlice, outOffset, repeatBatch, d.currentValue)
				outSlice.FillBytes(outOffset, repeatBatch, d.currentValue[:])
				// rOut = sliceFromOffset(rOut, repeatBatch)
				outOffset += repeatBatch
				valuesRead += repeatBatch
			} else if d.literalCount > 0 {
				literalBatch := util.MinInt(batchSize-valuesRead-remainingNulls, d.literalCount)

				// Decode the literals
				const kBufferSize int = 1024
				// var indicies [kBufferSize]interface{}
				// actualRead := d.bitReader.GetBatch(d.bitWidth, indicies[:], literalBatch)
				// indicies := reflect.MakeSlice(reflect.TypeOf(out), kBufferSize, kBufferSize)
				indicies := bytearray.Make(outSlice.ElementSize(), kBufferSize, kBufferSize)

				literalBatch = util.MinInt(literalBatch, kBufferSize)
				// actualRead := d.bitReader.GetBatch(d.bitWidth, indicies.Interface(), literalBatch)
				actualRead := d.bitReader.GetBatch(d.bitWidth, indicies, literalBatch)

				debug.Assert(actualRead == literalBatch, "Assert: actualRead == literalBatch")

				skipped := 0
				literalsRead := 1
				// rOut.Index(0).Set(reflect.ValueOf(indicies[0]))
				// outSlice.Index(outOffset).Set(indicies.Index(0))
				outSlice.At(outOffset).Copy(indicies.At(0).Bytes())

				// rOut = sliceFromOffset(rOut, 1)
				outOffset++

				// Read the first bitset to the end
				for literalsRead < literalBatch {
					debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
					if bitReader.IsSet() {
						// rOut.Index(0).Set(reflect.ValueOf(indicies[literalsRead]))
						// outSlice.Index(outOffset).Set(indicies.Index(literalsRead))
						outSlice.At(outOffset).Copy(indicies.At(literalsRead).Bytes())
						literalsRead++
					} else {
						// outSlice.Index(outOffset).Set(zero)
						outSlice.At(outOffset).Zero()
						skipped++
					}
					// rOut = sliceFromOffset(rOut, 1) // TODO(nickpoorman): Keep an index counter instead of slicing every time
					outOffset++
					bitReader.Next()
				}
				d.literalCount -= literalBatch
				valuesRead += literalBatch + skipped
				remainingNulls -= skipped
			}
		} else {
			// outSlice.Index(outOffset).Set(zero)
			outSlice.At(outOffset).Zero()
			// rOut = sliceFromOffset(rOut, 1)
			outOffset++
			valuesRead++
			remainingNulls--
		}
	}

	return valuesRead
}

// func buildBuffer(data interface{}, size int, out *interface{}) {
// 	reflect.MakeSlice(reflect.TypeOf(data), size, size)

// 	switch v := data.(type) {
// 	case bool:
// 		*out = [size]bool{}
// 	case int8:

// 	case uint8:

// 	case int16:

// 	case uint16:

// 	case int32:

// 	case uint32:

// 	case int64:

// 	case int: // cast int to int64

// 	case uint64:

// 	case float32:

// 	case float64:

// 	default:
// 		panic(fmt.Sprintf("writeToBuffer: unknown type: %T", data))
// 	}
// }

func indexInRange(idx int32, dictionaryLength int32) bool {
	return idx >= 0 && idx < dictionaryLength
}

// Like GetBatch but the values are then decoded using the provided dictionary
func (d *RleDecoder) GetBatchWithDict(dictionary []byte, dictionaryLength int32,
	values []byte, batchSize int) int {
	// Per https://github.com/apache/parquet-format/blob/master/Encodings.md,
	// the maximum dictionary index width in Parquet is 32 bits.

	debug.Assert(d.bitWidth >= 0, "Assert: d.bitWidth >= 0")
	valuesRead := 0

	dictionarySlice := reflect.ValueOf(dictionary)
	out := reflect.ValueOf(values)
	outOffset := 0

	for valuesRead < batchSize {
		remaining := batchSize - valuesRead

		if d.repeatCount > 0 {
			idx := int32(d.currentValue)
			if !indexInRange(idx, dictionaryLength) {
				return valuesRead
			}
			repeatBatch := util.MinInt(remaining, d.repeatCount)
			fill(out, outOffset, repeatBatch, dictionarySlice.Index(int(idx)))

			/* Upkeep counters */
			d.repeatCount -= repeatBatch
			valuesRead += repeatBatch
			outOffset += repeatBatch
		} else if d.literalCount > 0 {
			const kBufferSize int = 1024
			var indicies [kBufferSize]int32

			literalBatch := util.MinInt(remaining, d.literalCount)
			literalBatch = util.MinInt(literalBatch, kBufferSize)

			actualRead := d.bitReader.GetBatch(d.bitWidth, indicies[:], literalBatch)
			if actualRead != literalBatch {
				return valuesRead
			}

			for i := 0; i < literalBatch; i++ {
				index := indicies[i]
				if !indexInRange(index, dictionaryLength) {
					return valuesRead
				}
				out.Index(outOffset + i).Set(
					dictionarySlice.Index(int(index)),
				)
			}

			/* Upkeep counters */
			d.literalCount -= literalBatch
			valuesRead += literalBatch
			outOffset += literalBatch
		} else {
			if !d.NextCounts() {
				return valuesRead
			}
		}
	}

	return valuesRead
}

// Like GetBatchWithDict but add spacing for null entries
//
// Null entries will be zero-initialized in `values` to avoid leaking
// private data.
func (d *RleDecoder) GetBatchWithDictSpaced(
	dictionary interface{}, dictionaryLength int32, out interface{},
	batchSize int, nullCount int, validBits []byte, validBitsOffset int64) int {

	debug.Assert(d.bitWidth >= 0, "Assert: d.bitWidth >= 0")
	valuesRead := 0
	remainingNulls := nullCount

	dictionarySlice := reflect.ValueOf(dictionary)
	outSlice := reflect.ValueOf(out)
	outOffset := 0
	zero := reflect.Zero(outSlice.Type().Elem())

	bitReader := bitutilext.NewBitmapReader(validBits, int(validBitsOffset), batchSize)

	for valuesRead < batchSize {
		debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
		isValid := bitReader.IsSet()
		bitReader.Next()

		if isValid {
			if (d.repeatCount == 0) && (d.literalCount == 0) {
				if !d.NextCounts() {
					return valuesRead
				}
			}
			if d.repeatCount > 0 {
				idx := int32(d.currentValue)
				if !indexInRange(idx, dictionaryLength) {
					return valuesRead
				}
				value := dictionarySlice.Index(int(idx)).Interface()
				// The current index is already valid, we don't need to check that again
				repeatBatch := 1
				d.repeatCount--

				for d.repeatCount > 0 && (valuesRead+repeatBatch) < batchSize {
					debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
					if bitReader.IsSet() {
						d.repeatCount--
					} else {
						remainingNulls--
					}
					repeatBatch++

					bitReader.Next()
				}
				fill(outSlice, outOffset, repeatBatch, value)
				outOffset += repeatBatch
				valuesRead += repeatBatch
			} else if d.literalCount > 0 {
				literalBatch := util.MinInt(batchSize-valuesRead-remainingNulls, d.literalCount)

				// Decode the literals
				const kBufferSize int = 1024
				var indicies [kBufferSize]int32
				literalBatch = util.MinInt(literalBatch, kBufferSize)
				actualRead := d.bitReader.GetBatch(d.bitWidth, indicies[:], literalBatch)
				if actualRead != literalBatch {
					return valuesRead
				}

				skipped := 0
				literalsRead := 1

				firstIdx := indicies[0]
				if !indexInRange(firstIdx, dictionaryLength) {
					return valuesRead
				}
				outSlice.Index(outOffset).Set(dictionarySlice.Index(int(firstIdx)))
				outOffset++

				// Read the first bitset to the end
				for literalsRead < literalBatch {
					debug.Assert(bitReader.Position() < batchSize, "Assert: bitReader.Position() < batchSize")
					if bitReader.IsSet() {
						idx := indicies[literalsRead]
						if !indexInRange(idx, dictionaryLength) {
							return valuesRead
						}
						outSlice.Index(outOffset).Set(dictionarySlice.Index(int(idx)))
						literalsRead++
					} else {
						outSlice.Index(outOffset).Set(zero)
						skipped++
					}
					outOffset++
					bitReader.Next()
				}
				d.literalCount -= literalBatch
				valuesRead += literalBatch + skipped
				remainingNulls -= skipped
			}
		} else {
			outSlice.Index(outOffset).Set(zero)
			outOffset++
			valuesRead++
			remainingNulls--
		}
	}

	return valuesRead
}

// The maximum number of values in a single literal run
// (number of groups encodable by a 1-byte indicator * 8)
const RleMaxValuesPerLiteralRun int = (1 << 6) * 8

// Class to incrementally build the rle data.   This class does not allocate any memory.
// The encoding has two modes: encoding repeated runs and literal runs.
// If the run is sufficiently short, it is more efficient to encode as a literal run.
// This class does so by buffering 8 values at a time.  If they are not all the same
// they are added to the literal run.  If they are the same, they are added to the
// repeated run.  When we switch modes, the previous run is flushed out.
type RleEncoder struct {
	// Number of bits needed to encode the value. Must be between 0 and 64.
	bitWidth int

	// Underlying buffer.
	bitWriter *bitutilext.BitWriter

	// If true, the buffer is full and subsequent Put()'s will fail.
	bufferFull bool

	// The maximum byte size a single run can take.
	maxRunByteSize int

	// We need to buffer at most 8 values for literals.  This happens when the
	// bit_width is 1 (so 8 values fit in one byte).
	// TODO: generalize this to other bit widths
	bufferedValues [8]uint64

	// Number of values in buffered_values_
	numBufferedValues int

	// The current (also last) value that was written and the count of how
	// many times in a row that value has been seen.  This is maintained even
	// if we are in a literal run.  If the repeat_count_ get high enough, we switch
	// to encoding repeated runs.
	currentValue uint64
	repeatCount  int

	// Number of literals in the current run.  This does not include the literals
	// that might be in buffered_values_.  Only after we've got a group big enough
	// can we decide if they should part of the literal_count_ or repeat_count_
	literalCount int

	// Pointer to a byte in the underlying buffer that stores the indicator byte.
	// This is reserved as soon as we need a literal run but the value is written
	// when the literal run is complete.
	literalIndicatorByte []byte
}

// buffer/buffer_len: preallocated output buffer.
// bit_width: max number of bits for value.
// TODO: consider adding a min_repeated_run_length so the caller can control
// when values should be encoded as repeated runs.  Currently this is derived
// based on the bit_width, which can determine a storage optimal choice.
// TODO: allow 0 bit_width (and have dict encoder use it)
func NewRleEncoder(buffer []byte, bufferLen int, bitWidth int) *RleEncoder {
	debug.Assert(bitWidth >= 0, "bitWidth must be greater than or equal to zero")
	debug.Assert(bitWidth <= 640, "bitWidth must be less than or equal to 64")
	encoder := &RleEncoder{
		bitWidth:       bitWidth,
		bitWriter:      bitutilext.NewBitWriter(buffer, bufferLen),
		maxRunByteSize: RleEncoderMinBufferSize(bitWidth),
	}
	debug.Assert(bufferLen >= encoder.maxRunByteSize, "Input buffer not big enough.")
	encoder.Clear()
	return encoder
}

// Returns the minimum buffer size needed to use the encoder for 'bit_width'
// This is the maximum length of a single run for 'bit_width'.
// It is not valid to pass a buffer less than this length.
func RleEncoderMinBufferSize(bitWidth int) int {
	// 1 indicator byte and MAX_VALUES_PER_LITERAL_RUN 'bit_width' values.
	maxLiteralRunSize := 1 +
		int(bitutil.BytesForBits(int64(RleMaxValuesPerLiteralRun*bitWidth)))
	// Up to MAX_VLQ_BYTE_LEN indicator and a single 'bit_width' value.
	maxRepeatedRunSize := int(bitutilext.KMaxVlqByteLength +
		bitutil.BytesForBits(int64(bitWidth)))

	if maxLiteralRunSize > maxRepeatedRunSize {
		return maxLiteralRunSize
	}
	return maxRepeatedRunSize
}

// Returns the maximum byte size it could take to encode 'num_values'.
func RleEncoderMaxBufferSize(bitWidth int, numValues int) int {
	// For a bit_width > 1, the worst case is the repetition of "literal run of length 8
	// and then a repeated run of length 8".
	// 8 values per smallest run, 8 bits per byte
	bytesPerRun := int64(bitWidth)
	numRuns := bitutilext.CeilDiv(int64(numValues), 8)
	literalMaxSize := numRuns + numRuns*bytesPerRun

	// In the very worst case scenario, the data is a concatenation of repeated
	// runs of 8 values. Repeated run has a 1 byte varint followed by the
	// bit-packed repeated value
	minRepeatedRunSize := 1 + bitutil.BytesForBits(int64(bitWidth))
	repeatedMaxSize := bitutilext.CeilDiv(int64(numValues), 8) * minRepeatedRunSize

	if literalMaxSize > repeatedMaxSize {
		return int(literalMaxSize)
	}
	return int(repeatedMaxSize)
}

// Encode value.  Returns true if the value fits in buffer, false otherwise.
// This value must be representable with bit_width_ bits.
// This function buffers input values 8 at a time.  After seeing all 8 values,
// it decides whether they should be encoded as a literal or repeated run.
func (e *RleEncoder) Put(value uint64) bool {
	debug.Assert(e.bitWidth == 64 || value < (uint64(1)<<e.bitWidth),
		fmt.Sprintf("Assert: e.bitWidth == 64 || value < (uint64(1) << e.bitWidth): e.bitWidth: %d, value: %d, vr: %d",
			e.bitWidth, value, (uint64(1)<<e.bitWidth)))

	if e.bufferFull {
		return false
	}

	if e.currentValue != value {
		if e.repeatCount >= 8 {
			// We had a run that was long enough but it has ended.  Flush the
			// current repeated run.
			debug.Assert(e.literalCount == 0, "literalCount must equal zero")
			e.flushRepeatedRun()
		}
		e.repeatCount = 1
		e.currentValue = value
	} else { // predict true
		e.repeatCount++
		if e.repeatCount > 8 {
			// This is just a continuation of the current run, no need to buffer the
			// values.
			// Note that this is the fast path for long repeated runs.
			return true
		}
	}

	e.bufferedValues[e.numBufferedValues] = value
	e.numBufferedValues++
	if e.numBufferedValues == 8 {
		debug.Assert(e.literalCount%8 == 0, "Assert: e.literalCount % 8 == 0")
		e.flushBufferedValues(false)
	}
	return true
}

// Flushes any pending values to the underlying buffer.
// Returns the total number of bytes written
func (e *RleEncoder) Flush() int {
	// fmt.Println("called Flush")
	if e.literalCount > 0 || e.repeatCount > 0 || e.numBufferedValues > 0 {
		allRepeat := e.literalCount == 0 && (e.repeatCount == e.numBufferedValues ||
			e.numBufferedValues == 0)
		// There is something pending, figure out if it's a repeated or literal run
		if e.repeatCount > 0 && allRepeat {
			e.flushRepeatedRun()
		} else {
			debug.Assert(e.literalCount%8 == 0, "Assert: e.literalCount % 8 == 0")
			// Buffer the last group of literals to 8 by padding with 0s.
			for ; e.numBufferedValues != 0 && e.numBufferedValues < 8; e.numBufferedValues++ {
				e.bufferedValues[e.numBufferedValues] = 0
			}
			e.literalCount += e.numBufferedValues
			e.flushLiteralRun(true)
			e.repeatCount = 0
		}
	}
	e.bitWriter.Flush(false)
	debug.Assert(e.numBufferedValues == 0, "Assert: e.numBufferedValues == 0")
	debug.Assert(e.literalCount == 0, "Assert: e.literalCount == 0")
	debug.Assert(e.repeatCount == 0, "Assert: e.repeatCount == 0")

	return e.bitWriter.BytesWritten()
}

// Resets all the state in the encoder.
func (e *RleEncoder) Clear() {
	e.bufferFull = false
	e.currentValue = 0
	e.repeatCount = 0
	e.numBufferedValues = 0
	e.literalCount = 0
	e.literalIndicatorByte = nil
	e.bitWriter.Clear()
}

// Returns pointer to underlying buffer
func (e *RleEncoder) Buffer() []byte {
	return e.bitWriter.Buffer()
}
func (e *RleEncoder) Len() int {
	return e.bitWriter.BytesWritten()
}

// Flushes any buffered values.  If this is part of a repeated run, this is largely
// a no-op.
// If it is part of a literal run, this will call FlushLiteralRun, which writes
// out the buffered literal values.
// If 'done' is true, the current run would be written even if it would normally
// have been buffered more.  This should only be called at the end, when the
// encoder has received all values even if it would normally continue to be
// buffered.
// Flush the values that have been buffered.  At this point we decide whether
// we need to switch between the run types or continue the current one.
func (e *RleEncoder) flushBufferedValues(done bool) {
	if e.repeatCount >= 8 {
		// Clear the buffered values.  They are part of the repeated run now and we
		// don't want to flush them out as literals.
		e.numBufferedValues = 0
		if e.literalCount != 0 {
			// There was a current literal run.  All the values in it have been flushed
			// but we still need to update the indicator byte.
			debug.Assert(e.literalCount%8 == 0, "Assert: e.literalCount % 8 == 0")
			debug.Assert(e.repeatCount == 8, "Assert: e.repeatCount == 8")
			e.flushLiteralRun(true)
		}
		debug.Assert(e.literalCount == 0, "Assert: e.literalCount == 0")
		return
	}

	e.literalCount += e.numBufferedValues
	debug.Assert(e.literalCount%8 == 0, "Assert: e.literalCount % 8 == 0")
	numGroups := e.literalCount / 8
	if numGroups+1 >= (1 << 6) {
		// We need to start a new literal run because the indicator byte we've reserved
		// cannot store more values.
		debug.Assert(e.literalIndicatorByte != nil, "Assert: e.literalIndicatorByte != nil")
		e.flushLiteralRun(true)
	} else {
		e.flushLiteralRun(done)
	}
	e.repeatCount = 0
}

// Flushes literal values to the underlying buffer.  If update_indicator_byte,
// then the current literal run is complete and the indicator byte is updated.
func (e *RleEncoder) flushLiteralRun(update_indicator_byte bool) {
	// fmt.Println("called flushLiteralRun")
	if e.literalIndicatorByte == nil {
		// The literal indicator byte has not been reserved yet, get one now.
		e.literalIndicatorByte = e.bitWriter.GetNextBytePtr(1)
		debug.Assert(e.literalIndicatorByte != nil, "literalIndicatorByte should not be nil")
	}

	// Write all the buffered values as bit packed literals
	for i := 0; i < e.numBufferedValues; i++ {
		success := e.bitWriter.PutValue(e.bufferedValues[i], e.bitWidth)
		debug.Assert(success, "There is a bug in using CheckBufferFull()")
	}
	e.numBufferedValues = 0

	if update_indicator_byte {
		// At this point we need to write the indicator byte for the literal run.
		// We only reserve one byte, to allow for streaming writes of literal values.
		// The logic makes sure we flush literal runs often enough to not overrun
		// the 1 byte.
		debug.Assert(e.literalCount%8 == 0, "Assert: e.literalCount % 8 == 0")
		numGroups := e.literalCount / 8
		indicatorValue := (numGroups << 1) | 1
		debug.Assert(indicatorValue&0xFFFFFF00 == 0, "Assert: indicatorValue & 0xFFFFFF00 == 0")
		e.literalIndicatorByte[0] = byte(indicatorValue)
		e.literalIndicatorByte = nil
		e.literalCount = 0
		e.checkBufferFull()
	}
}

// Flushes a repeated run to the underlying buffer.
func (e *RleEncoder) flushRepeatedRun() {
	// fmt.Println("called flushRepeatedRun")
	debug.Assert(e.repeatCount > 0, "Assert: e.repeatCount > 0")
	result := true
	// The lsb of 0 indicates this is a repeated run
	indicatorValue := uint32(e.repeatCount<<1 | 0)
	result = result && e.bitWriter.PutVlqInt(indicatorValue)
	result = result && e.bitWriter.PutAligned(e.currentValue,
		int(bitutilext.CeilDiv(int64(e.bitWidth), 8)))
	debug.Assert(result, "expected result to be true")
	e.numBufferedValues = 0
	e.repeatCount = 0
	e.checkBufferFull()
}

// Checks and sets buffer_full_. This must be called after flushing a run to
// make sure there are enough bytes remaining to encode the next run.
func (e *RleEncoder) checkBufferFull() {
	bytesWritten := e.bitWriter.BytesWritten()
	if bytesWritten+e.maxRunByteSize > e.bitWriter.BufferLen() {
		e.bufferFull = true
	}
}
