package util

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
	testingext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/testing"
	bitutilext "github.com/nickpoorman/arrow-parquet-go/parquet/arrow/util/bitutil"
)

const maxWidth int = 32

func TestBitArrayTestBool(t *testing.T) {
	const len int = 8
	buffer := make([]byte, 8)

	writer := bitutilext.NewBitWriter(buffer, len)

	// Write alternating 0's and 1's
	for i := 0; i < 8; i++ {
		result := writer.PutValue(uint64(i%2), 1)
		testutil.AssertTrue(t, result)
	}
	writer.Flush(false)
	testutil.AssertEqInt(t, int(buffer[0]), 0b10101010)

	// Write 00110011
	for i := 0; i < 8; i++ {
		result := false
		switch i {
		case 0, 1, 4, 5:
			result = writer.PutValue(0, 1)
		default:
			result = writer.PutValue(1, 1)
		}
		testutil.AssertTrue(t, result)
	}
	writer.Flush(false)

	// Validate the exact bit value
	testutil.AssertEqInt(t, int(buffer[0]), 0b10101010)
	testutil.AssertEqInt(t, int(buffer[1]), 0b11001100)

	fmt.Println("loop 1")

	// Use the reader and validate
	reader := bitutilext.NewBitReader(buffer, len)
	for i := 0; i < 8; i++ {
		fmt.Printf("\nreader index: %d - looking for: %d\n", i, uint64(i%2))
		var val uint64
		result := reader.GetValue(1, &val)
		testutil.AssertTrue(t, result)
		testutil.AssertDeepEq(t, val, uint64(i%2))
	}

	fmt.Println("loop 2")

	for i := 0; i < 8; i++ {
		fmt.Printf("\nreader index: %d\n", i)
		// val := false
		var val uint64
		result := reader.GetValue(1, &val)
		testutil.AssertTrue(t, result)
		switch i {
		case 0, 1, 4, 5:
			// testutil.AssertDeepEq(t, val, false)
			testutil.AssertDeepEq(t, val, uint64(0))
		default:
			// testutil.AssertDeepEq(t, val, true)
			testutil.AssertDeepEq(t, val, uint64(1))
		}
	}
}

// Writes 'num_vals' values with width 'bit_width' and reads them back.
func BitArrayValuesTest(t *testing.T, bitWidth int, numValues int) {
	len := bitutil.BytesForBits(int64(bitWidth * numValues))
	testutil.AssertGT(t, int(len), 0)
	var mod int64 = 1
	if bitWidth != 64 {
		mod = int64(1) << bitWidth
	}

	buffer := make([]byte, len)
	writer := bitutilext.NewBitWriter(buffer, int(len))
	for i := 0; i < numValues; i++ {
		result := writer.PutValue(uint64(int64(i)%mod), bitWidth)
		testutil.AssertTrue(t, result)
	}
	writer.Flush(false)
	testutil.AssertEqInt(t, writer.BytesWritten(), int(len))

	reader := bitutilext.NewBitReader(buffer, int(len))
	for i := 0; i < numValues; i++ {
		var val int64
		result := reader.GetValue(bitWidth, &val)
		testutil.AssertTrue(t, result)
		testutil.AssertEqInt(t, int(val), int(int64(i)%mod))
	}
	testutil.AssertEqInt(t, reader.BytesLeft(), 0)
}

// Test some mixed values
func TestBitArrayTestMixed(t *testing.T) {
	const length int = 1024
	buffer := make([]byte, length)
	parity := true

	writer := bitutilext.NewBitWriter(buffer, length)
	for i := 0; i < length; i++ {
		var result bool
		if i%2 == 0 {
			result = writer.PutValue(uint64(bToI(parity)), 1)
			parity = !parity
		} else {
			result = writer.PutValue(uint64(i), 10)
		}
		testutil.AssertTrue(t, result)
	}
	writer.Flush(false)

	parity = true
	reader := bitutilext.NewBitReader(buffer, int(length))
	for i := 0; i < length; i++ {
		var result bool
		if i%2 == 0 {
			var val bool
			result = reader.GetValue(1, &val)
			testutil.AssertDeepEq(t, val, parity)
			parity = !parity
		} else {
			var val int
			result = reader.GetValue(10, &val)
			testutil.AssertEqInt(t, val, i)
		}
		testutil.AssertTrue(t, result)
	}
}

func bToI(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
func ValidateRle(t *testing.T, values []int, bitWidth int, expectedEncoding []byte, expectedLen int) {
	fmt.Printf("\n--- ValidateRle ---\n")
	fmt.Printf("ValidateRle - valuesLen: %d | bitWidth: %d | expectedLen: %d\n", len(values), bitWidth, expectedLen)
	const length int = 64 * 1024
	buffer := make([]byte, length)
	testutil.AssertLT(t, expectedLen, length)

	encoder := NewRleEncoder(buffer, length, bitWidth)
	for i := 0; i < len(values); i++ {
		result := encoder.Put(uint64(values[i]))
		testutil.AssertTrue(t, result)
	}
	encodedLen := encoder.Flush()

	if expectedLen != -1 {
		testutil.AssertEqInt(t, encodedLen, expectedLen)
	}
	if expectedEncoding != nil {
		// copy(buffer, expectedEncoding[:encodedLen])
		testutil.AssertBitsEq(t, buffer[:encodedLen], expectedEncoding[:encodedLen])
	}

	// Verify read
	{
		decoder := NewRleDecoder(buffer, length, bitWidth)
		for i := 0; i < len(values); i++ {
			fmt.Printf("\nloop i: %d - looking for: %d\n", i, values[i])
			var val uint64
			result := decoder.Get(&val)
			testutil.AssertTrue(t, result)
			testutil.AssertEqInt(t, int(val), values[i])
		}
	}

	// Verify batch read
	{
		decoder := NewRleDecoder(buffer, length, bitWidth)
		valuesRead := make([]int, len(values))
		testutil.AssertEqInt(t, len(values), decoder.GetBatch(valuesRead, len(valuesRead)))
		testutil.AssertDeepEq(t, values, valuesRead)
	}
}

// A version of ValidateRle that round-trips the values and returns false if
// the returned values are not all the same
func CheckRoundTrip(t *testing.T, values []int, bitWidth int) bool {
	const length int = 64 * 1024
	buffer := make([]byte, length)
	encoder := NewRleEncoder(buffer, length, bitWidth)
	for i := 0; i < len(values); i++ {
		result := encoder.Put(uint64(values[i]))
		if !result {
			return false
		}
	}
	encodedLen := encoder.Flush()
	out := 0

	{
		decoder := NewRleDecoder(buffer, encodedLen, bitWidth)
		for i := 0; i < len(values); i++ {
			testutil.AssertTrue(t, decoder.Get(&out))
			if values[i] != out {
				return false
			}
		}
	}

	// Verify batch read
	{
		decoder := NewRleDecoder(buffer, encodedLen, bitWidth)
		valuesRead := make([]int, len(values))
		if len(values) != decoder.GetBatch(valuesRead, len(values)) {
			return false
		}

		if !reflect.DeepEqual(values, valuesRead) {
			return false
		}
	}

	return true
}

func TestRleSpecificSequences(t *testing.T) {
	const length int = 1024
	expectedBuffer := make([]byte, length)

	// Test 50 0' followed by 50 1's
	values := make([]int, 100)
	for i := 0; i < 50; i++ {
		values[i] = 0
	}
	for i := 50; i < 100; i++ {
		values[i] = 1
	}

	// expectedBuffer valid for bit width <= 1 byte
	expectedBuffer[0] = (50 << 1)
	expectedBuffer[1] = 0
	expectedBuffer[2] = (50 << 1)
	expectedBuffer[3] = 1

	for width := 1; width <= 8; width++ {
		fmt.Printf("\nTestRleSpecificSequences - ValidateRle-1 - width: %d\n", width)
		ValidateRle(t, values, width, expectedBuffer, 4)
	}

	for width := 9; width <= maxWidth; width++ {
		fmt.Printf("\nTestRleSpecificSequences - ValidateRle-2 - width: %d\n", width)
		ValidateRle(t, values, width, nil, int(2*(1+bitutilext.CeilDiv(int64(width), 8))))
	}

	// Test 100 0's and 1's alternating
	for i := 0; i < 100; i++ {
		values[i] = i % 2
	}
	numGroups := int(bitutilext.CeilDiv(100, 8))
	fmt.Println("numGroups: ", numGroups)
	expectedBuffer[0] = byte((numGroups << 1) | 1)
	for i := 1; i <= 100/8; i++ {
		expectedBuffer[i] = 0b10101010
	}
	// Values for the last 4 0 and 1's. The upper 4 bits should be padded to 0.
	expectedBuffer[100/8+1] = 0b00001010

	// num_groups and expected_buffer only valid for bit width = 1
	fmt.Print("\nTestRleSpecificSequences - ValidateRle-3\n")
	ValidateRle(t, values, 1, expectedBuffer, int(1+numGroups))
	for width := 2; width <= maxWidth; width++ {
		fmt.Printf("\nTestRleSpecificSequences - ValidateRle-4 - width: %d\n", width)
		numValues := int(bitutilext.CeilDiv(100, 8)) * 8
		ValidateRle(t, values, width, nil, 1+int(bitutilext.CeilDiv(int64(width*numValues), 8)))
	}

	// Test 16-bit values to confirm encoded values are stored in little endian
	values = make([]int, 28)
	for i := 0; i < 16; i++ {
		values[i] = 0x55aa
	}
	for i := 16; i < 28; i++ {
		values[i] = 0xaa55
	}
	expectedBuffer[0] = (16 << 1)
	expectedBuffer[1] = 0xaa
	expectedBuffer[2] = 0x55
	expectedBuffer[3] = (12 << 1)
	expectedBuffer[4] = 0x55
	expectedBuffer[5] = 0xaa

	ValidateRle(t, values, 16, expectedBuffer, 6)

	// Test 32-bit values to confirm encoded values are stored in little endian
	values = make([]int, 28)
	for i := 0; i < 16; i++ {
		values[i] = 0x555aaaa5
	}
	for i := 16; i < 28; i++ {
		values[i] = 0x5aaaa555
	}
	expectedBuffer[0] = (16 << 1)
	expectedBuffer[1] = 0xa5
	expectedBuffer[2] = 0xaa
	expectedBuffer[3] = 0x5a
	expectedBuffer[4] = 0x55
	expectedBuffer[5] = (12 << 1)
	expectedBuffer[6] = 0x55
	expectedBuffer[7] = 0xa5
	expectedBuffer[8] = 0xaa
	expectedBuffer[9] = 0x5a

	ValidateRle(t, values, 32, expectedBuffer, 10)
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
func RleValuesTest(t *testing.T, bitWidth int, numValues int, value int) {
	fmt.Printf("RleValuesTest - bitWidth: %d | numValues: %d | value: %d\n", bitWidth, numValues, value)
	var mod uint64 = 1
	if bitWidth != 64 {
		mod = uint64(int64(1) << bitWidth)
	}
	var values []int
	for v := 0; v < numValues; v++ {
		val := value
		if value == -1 {
			val = int(uint64(v) % mod)
		}
		values = append(values, val)
	}
	ValidateRle(t, values, bitWidth, nil, -1)
}

func TestRleTestValues(t *testing.T) {
	for width := 1; width <= maxWidth; width++ {
		RleValuesTest(t, width, 1, -1)
		RleValuesTest(t, width, 1024, -1)
		RleValuesTest(t, width, 1024, 0)
		RleValuesTest(t, width, 1024, 1)
	}
}

func TestRleBitWidthZeroRepeated(t *testing.T) {
	buffer := make([]byte, 1)
	numValues := 15
	buffer[0] = byte(numValues << 1) // repeated indicator byte
	decoder := NewRleDecoder(buffer, len(buffer), 0)
	var val byte
	for i := 0; i < numValues; i++ {
		result := decoder.Get(&val)
		testutil.AssertTrue(t, result)
		testutil.AssertEqInt(t, int(val), 0) // can only encode 0s with bit width 0
	}
	testutil.AssertFalse(t, decoder.Get(&val))
}

func TestRleBitWidthZeroLiteral(t *testing.T) {
	buffer := make([]byte, 1)
	numGroups := 4
	buffer[0] = byte(numGroups<<1 | 1) // literal indicator byte
	decoder := NewRleDecoder(buffer, len(buffer), 0)
	numValues := numGroups * 8
	var val byte
	for i := 0; i < numValues; i++ {
		result := decoder.Get(&val)
		testutil.AssertTrue(t, result)
		testutil.AssertEqInt(t, int(val), 0) // can only encode 0s with bit width 0
	}
	testutil.AssertFalse(t, decoder.Get(&val))
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
func TestBitRleFlush(t *testing.T) {
	var values []int
	for i := 0; i < 16; i++ {
		values = append(values, 1)
	}
	values = append(values, 0)
	ValidateRle(t, values, 1, nil, -1)
	values = append(values, 1)
	ValidateRle(t, values, 1, nil, -1)
	values = append(values, 1)
	ValidateRle(t, values, 1, nil, -1)
	values = append(values, 1)
	ValidateRle(t, values, 1, nil, -1)
}

// Test some random sequences.
func TestBitRleRandom(t *testing.T) {
	niters := 50
	ngroups := 1000
	maxGroupSize := 16
	values := make([]int, ngroups+maxGroupSize)

	// prng setup
	rdSeed := newRandom(1, math.MaxInt32-1, 1337)

	for iter := 0; iter < niters; iter++ {
		seed := int64(rdSeed.next())
		rd := newRandom(1, 20, seed)
		parity := false
		values = make([]int, 0)

		for i := 0; i < ngroups; i++ {
			groupSize := rd.next()
			if groupSize > maxGroupSize {
				groupSize = 1
			}
			for j := 0; j < groupSize; j++ {
				values = append(values, bToI(parity))
			}
			parity = !parity
		}
		if !CheckRoundTrip(t, values, bitutilext.NumRequiredBits(uint64(len(values)))) {
			t.Errorf("failing seed: %d", seed)
		}
	}
}

type random struct {
	rd  *rand.Rand
	max int
	min int
}

func newRandom(min, max int, seed int64) random {
	rd := rand.New(rand.NewSource(seed))
	return random{
		rd:  rd,
		min: min,
		max: max,
	}
}

func (r random) next() int {
	return r.rd.Intn(r.max-r.min) + r.min
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
func TestBitRleRepeatedPattern(t *testing.T) {
	var values []int
	minRun := 1
	maxRun := 32

	for i := minRun; i <= maxRun; i++ {
		v := i % 2
		for j := 0; j < i; j++ {
			values = append(values, v)
		}
	}

	// And go back down again
	for i := maxRun; i >= minRun; i-- {
		v := i % 2
		for j := 0; j < i; j++ {
			values = append(values, v)
		}
	}

	ValidateRle(t, values, 1, nil, -1)
}

func TestBitRleOverflow(t *testing.T) {
	for bitWidth := 1; bitWidth < 32; bitWidth += 3 {
		length := RleEncoderMinBufferSize(bitWidth)
		buffer := make([]byte, length)
		numAdded := 0
		parity := true

		encoder := NewRleEncoder(buffer, length, bitWidth)
		// Insert alternating true/false until there is no space left
		for {
			result := encoder.Put(uint64(bToI(parity)))
			parity = !parity
			if !result {
				break
			}
			numAdded++
		}

		bytesWritten := encoder.Flush()
		testutil.AssertLT(t, bytesWritten, length)
		testutil.AssertGT(t, numAdded, 0)

		decoder := NewRleDecoder(buffer, bytesWritten, bitWidth)
		parity = true
		var v uint32
		for i := 0; i < numAdded; i++ {
			result := decoder.Get(&v)
			testutil.AssertTrue(t, result)
			testutil.AssertDeepEq(t, v != 0, parity)
			parity = !parity
		}
		// Make sure we get false when reading past end a couple times.
		testutil.AssertFalse(t, decoder.Get(&v))
		testutil.AssertFalse(t, decoder.Get(&v))
	}
}

func CheckRoundTripSpacedInt32(t *testing.T, data *array.Int32, bitWidth int) {
	numValues := data.Len()
	bufferSize := RleEncoderMaxBufferSize(bitWidth, numValues)

	values := data.Int32Values()

	buffer := make([]byte, bufferSize)
	encoder := NewRleEncoder(buffer, bufferSize, bitWidth)
	for i := 0; i < numValues; i++ {
		if data.IsValid(i) {
			if !encoder.Put(uint64(values[i])) {
				t.Error("Encoding failed")
			}
		}
	}
	encodedSize := encoder.Flush()

	// Verify batch read
	decoder := NewRleDecoder(buffer, encodedSize, bitWidth)
	valuesRead := make([]int32, numValues)

	fmt.Println("numValues: ", numValues)
	testutil.AssertEqInt(t,
		decoder.GetBatchSpaced(
			numValues,
			data.NullN(),
			data.NullBitmapBytes(),
			int64(data.Offset()), valuesRead),
		numValues)

	for i := 0; i < numValues; i++ {
		// fmt.Printf("Index %d | isValid: %t\n", i, data.IsValid(i))
		if data.IsValid(i) {
			testutil.AssertDeepEqM(t, valuesRead[i], values[i], fmt.Sprintf("Index %d read %d but should be %d", i, valuesRead[i], values[i]))
		}
	}
}

func TestRleDecoderGetBatchSpaced(t *testing.T) {
	kSeed := int64(1337)
	rd := testingext.NewRandomArrayGenerator(kSeed)
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	int32Cases := []struct {
		maxValue        int32
		size            int64
		nullProbability float64
		bitWidth        int
	}{
		{1, 100000, 0.01, 1},
		{1, 100000, 0.1, 1},
		{1, 100000, 0.5, 1},
		{4, 100000, 0.05, 3}, // TODO: Figure out why this case fails.
		// {100, 100000, 0.05, 7},
	}
	for _, c := range int32Cases {
		arr := rd.Int32(int(c.size), 0 /*min=*/, c.maxValue, c.nullProbability, mem)
		CheckRoundTripSpacedInt32(t, arr, c.bitWidth)
		arr2 := array.NewSlice(arr, 1, int64(arr.Len())).(*array.Int32)
		CheckRoundTripSpacedInt32(t, arr2, c.bitWidth)
		arr.Release()
		arr2.Release()
	}
}

func TestRleDecoderGetBatchSpacedSpecific(t *testing.T) {
	kSeed := int64(1337)
	rd := testingext.NewRandomArrayGenerator(kSeed)
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	int32Cases := []struct {
		maxValue        int32
		size            int64
		nullProbability float64
		bitWidth        int
	}{
		{1, 100000, 0.01, 1},
		{1, 100000, 0.1, 1},
		{1, 100000, 0.5, 1},
		{4, 100000, 0.05, 3}, // TODO: Figure out why this case fails.
		// {100, 100000, 0.05, 7},
	}
	for _, c := range int32Cases {
		arr := rd.Int32(int(c.size) /*min=*/, 0, c.maxValue, c.nullProbability, mem)
		CheckRoundTripSpacedInt32(t, arr, c.bitWidth)
		arr2 := array.NewSlice(arr, 1, int64(arr.Len())).(*array.Int32)
		CheckRoundTripSpacedInt32(t, arr2, c.bitWidth)
		arr.Release()
		arr2.Release()
	}
}
