package bytearray

import (
	"encoding/binary"
	"testing"

	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
)

func TestExpectZeroAllocs(t *testing.T) {
	avg := testing.AllocsPerRun(1000, func() {
		ba := NewByteArray(make([]byte, 8), 8)
		ba.PutUint64(4)
	})
	if avg != 0.0 {
		t.Fatalf("ExpectZeroAllocs: got=%g; want=%g", avg, 0.0)
	}
}

// This does 4 allocs simply because of the io.Reader interface.
// We can do better....
// func BenchmarkReadBinary(b *testing.B) {
// 	num := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(num, 4)

// 	for n := 0; n < b.N; n++ {
// 		var prim uint64
// 		if err := binary.Read(bytes.NewBuffer(make([]byte, 8)), binary.LittleEndian, &prim); err != nil {
// 			panic(err)
// 		}
// 	}
// }

func TestUint32(t *testing.T) {
	{
		u32 := uint32(2)
		u32BA := NewByteArray(make([]byte, 4), 4)

		u32BA.PutUint32(u32)
		testutil.AssertDeepEq(t, u32BA.Uint32(), u32)
	}

	{
		u32 := uint32(2)
		u32BA := NewByteArray(make([]byte, 8), 8)

		u32BA.PutUint32(u32)
		testutil.AssertDeepEq(t, u32BA.Uint32(), u32)
	}

	{
		u64 := uint64(2)
		u32BA := NewByteArray(make([]byte, 4), 4)

		u32BA.PutUint64(u64)
		testutil.AssertDeepEq(t, u32BA.Uint64(), u64)
	}
}

// func TestElementsFillBytes(t *testing.T) {
// 	{
// 		// We need to be able to store 4 booleans in 4 bytes
// 		b := make([]byte, 4)
// 		boolBA := NewByteArray(b, 1)
// 		for i := 0; i < len(b); i++ {
// 			if i%2 == 0 {
// 				boolBA.ElementAt(i).PutBool(true)
// 			} else {
// 				boolBA.ElementAt(i).PutBool(false)
// 			}
// 		}
// 		want := []bool{true, false, true, false}
// 		got := make([]bool, 4)
// 		boolBA.ToValue(got)
// 		testutil.AssertDeepEq(t, got, want)
// 	}

// 	{
// 		var currentValue [8]byte
// 		binary.LittleEndian.PutUint64(currentValue[:], 1) // set current value to 1, i.e. true

// 		// We need to be able to store 4 booleans in 4 byte of type int
// 		b := make([]byte, 4)
// 		boolBA := NewByteArray(b, 1)
// 		boolBA.ElementsFillBytes(0, 4, currentValue[:])
// 		want := []int{1, 1, 1, 1}

// 		got := make([]int, 4)
// 		boolBA.ToValue(got)
// 		testutil.AssertDeepEq(t, got, want)
// 	}
// }

func BenchmarkElementsArrTest(b *testing.B) {
	currentValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(currentValue, 1) // set current value to 1, i.e. true

	// We need to be able to store 4 booleans in 4 byte of type int
	boolBA := NewByteArray(make([]byte, 4), 1)
	boolBA.ElementsFillBytes(0, 4, currentValue)

	// want := []uint8{1, 1, 1, 1}
	got := make([]uint8, 4)

	for n := 0; n < b.N; n++ {
		boolBA.ReadTo(got)
		// testutil.AssertDeepEq(t, got, want)
	}

}

func TestElementsArrTest(t *testing.T) {
	{
		currentValue := make([]byte, 8)
		binary.LittleEndian.PutUint64(currentValue, 1) // set current value to 1, i.e. true

		// We need to be able to store 4 booleans in 4 byte of type int
		boolBA := NewByteArray(make([]byte, 4), 1)
		boolBA.ElementsFillBytes(0, 4, currentValue)

		want := []uint8{1, 1, 1, 1}
		got := make([]uint8, 4)

		boolBA.ReadTo(got)
		testutil.AssertDeepEq(t, got, want)
	}

	{
		currentValue := make([]byte, 8)
		binary.LittleEndian.PutUint64(currentValue, 1) // set current value to 1, i.e. true

		// We need to be able to store 4 booleans in 4 byte of type int
		boolBA := NewByteArray(make([]byte, 4), 1)
		boolBA.ElementsFillBytes(0, 4, currentValue)

		want := []uint16{1, 1, 1, 1}
		got := make([]uint16, 4)

		boolBA.ReadTo(got)
		testutil.AssertDeepEq(t, got, want)
	}

	{
		currentValue := make([]byte, 1)
		currentValue[0] = 1 // set current value to 1, i.e. true

		// We need to be able to store 4 booleans in 4 byte of type int
		boolBA := NewByteArray(make([]byte, 4), 1)
		boolBA.ElementsFillBytes(0, 4, currentValue)

		want := []uint16{1, 1, 1, 1}
		got := make([]uint16, 4)

		boolBA.ReadTo(got)
		testutil.AssertDeepEq(t, got, want)
	}
}
