package testing

import (
	"math"
	"math/rand"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

const kSeedMax = math.MaxInt64

type RandomArrayGenerator struct {
	rd *rand.Rand
}

func NewRandomArrayGenerator(seed int64) *RandomArrayGenerator {
	return &RandomArrayGenerator{
		rd: rand.New(rand.NewSource(seed)),
	}
}

// Generates a random Int32Array
//
// size the size of the array to generate
// min the lower bound of the uniform distribution
// max the upper bound of the uniform distribution
// null_probability the probability of a row being null
//
// returns a generated Array
func (r *RandomArrayGenerator) Int32(size int, min int32, max int32, nullProbability float64 /* default = 0 */, mem memory.Allocator) *array.Int32 {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	builder := array.NewInt32Builder(mem)
	defer builder.Release()

	for i := 0; i < size; i++ {
		if r.boolWithProbability(nullProbability) {
			builder.AppendNull()
		} else {
			builder.Append(r.rd.Int31n(max))
		}
	}

	return builder.NewInt32Array()
}

func (r *RandomArrayGenerator) boolWithProbability(p float64) bool {
	return r.rd.Float64() < p
}
