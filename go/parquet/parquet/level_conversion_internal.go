package parquet

import (
	"fmt"

	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

// TODO(nickpoorman): Speed this up with SIMD implementation.
func DefinitionLevelsToBitmap(
	defLevels []int16, numDefLevels int64,
	maxDefinitionLevel int16, maxRepetitionLevel int16,
	valuesRead *int64,
	nullCount *int64,
	validBits []byte,
	validBitsOffset int64,
) error {
	// We assume here that valid_bits is large enough to accommodate the
	// additional definition levels and the ones that have already been written
	validBitsWriter := bitutilext.NewBitmapWriter(
		validBits, int(validBitsOffset), int(numDefLevels))

	// TODO(nickpoorman): As an interim solution we are splitting the code path here
	// between repeated+flat column reads, and non-repeated+nested reads.
	// Those paths need to be merged in the future
	for i := 0; i < int(numDefLevels); i++ {
		if defLevels[i] == maxDefinitionLevel {
			validBitsWriter.Set()
		} else if maxRepetitionLevel > 0 {
			// repetition+flat case
			if defLevels[i] == (maxDefinitionLevel - 1) {
				validBitsWriter.Clear()
				*nullCount += 1
			} else {
				continue
			}
		} else {
			// non-repeated+nested case
			if defLevels[i] < maxDefinitionLevel {
				validBitsWriter.Clear()
				*nullCount += 1
			} else {
				fmt.Errorf(
					"definition level exceeds maximum: %w",
					ParquetException,
				)
			}
		}

		validBitsWriter.Next()
	}
	validBitsWriter.Finish()
	*valuesRead = int64(validBitsWriter.Position())
	return nil
}
