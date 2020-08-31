package parquet

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/xitongsys/parquet-go/parquet"
)

func randomNumbersInt(n int, seed int64, maxValue int, signed bool, out interface{}) interface{} {
	r := rand.New(rand.NewSource(seed))
	rn := rand.New(rand.NewSource(seed))
	outT := reflect.ValueOf(out)
	for i := 0; i < n; i++ {
		// out[i] = int64(r.Intn(maxValue) * sign(signed, rn))
		el := outT.Index(i)
		random := r.Intn(maxValue) * sign(signed, rn)
		el.Set(reflect.ValueOf(random).Convert(el.Type()))
	}
	return out
}

func sign(signed bool, r *rand.Rand) int {
	if signed && r.Intn(2) == 0 {
		return -1
	} else {
		return 1
	}
}

// ----------------------------------------------------------------------
// Parquet Specific Assert Helpers

func AssertEqType(t *testing.T, got, want Type) {
	t.Helper()
	if got != want {
		t.Errorf("AssertEqType: got=%d; want=%d\n", got, want)
	}
}

func AssertEqSchemaElement(t *testing.T, got, want parquet.SchemaElement) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AssertEqSchemaElement: \ngot=%v\nwant=%v\n", got, want)
	}
}
