package bitutil

import (
	"math"
	"testing"

	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
)

func TestBitStreamUtil_ZigZag(t *testing.T) {
	cases := []int32{
		0, 1, 1234, -1, -1234, math.MaxInt32, math.MinInt32,
	}

	for _, v := range cases {
		{
			buffer := make([]byte, KMaxVlqByteLength)
			writer := NewBitWriter(buffer, len(buffer))
			reader := NewBitReader(buffer, len(buffer))
			writer.PutZigZagVlqInt(v)
			result, ok := reader.GetZigZagVlqInt()
			testutil.AssertTrue(t, ok)
			if result != v {
				t.Errorf("AssertEq: got=%d; want=%d\n", result, v)
			}
		}
	}
}
