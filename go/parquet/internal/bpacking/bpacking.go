package bpacking

import (
	"fmt"

	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
)

func unpack1_32(in []uint32, out []uint32) []uint32 {
	// TODO(nickpoorman): Implement SafeLoad?
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) & 1
	idx++
	out[idx] = (inl >> 1) & 1
	idx++
	out[idx] = (inl >> 2) & 1
	idx++
	out[idx] = (inl >> 3) & 1
	idx++
	out[idx] = (inl >> 4) & 1
	idx++
	out[idx] = (inl >> 5) & 1
	idx++
	out[idx] = (inl >> 6) & 1
	idx++
	out[idx] = (inl >> 7) & 1
	idx++
	out[idx] = (inl >> 8) & 1
	idx++
	out[idx] = (inl >> 9) & 1
	idx++
	out[idx] = (inl >> 10) & 1
	idx++
	out[idx] = (inl >> 11) & 1
	idx++
	out[idx] = (inl >> 12) & 1
	idx++
	out[idx] = (inl >> 13) & 1
	idx++
	out[idx] = (inl >> 14) & 1
	idx++
	out[idx] = (inl >> 15) & 1
	idx++
	out[idx] = (inl >> 16) & 1
	idx++
	out[idx] = (inl >> 17) & 1
	idx++
	out[idx] = (inl >> 18) & 1
	idx++
	out[idx] = (inl >> 19) & 1
	idx++
	out[idx] = (inl >> 20) & 1
	idx++
	out[idx] = (inl >> 21) & 1
	idx++
	out[idx] = (inl >> 22) & 1
	idx++
	out[idx] = (inl >> 23) & 1
	idx++
	out[idx] = (inl >> 24) & 1
	idx++
	out[idx] = (inl >> 25) & 1
	idx++
	out[idx] = (inl >> 26) & 1
	idx++
	out[idx] = (inl >> 27) & 1
	idx++
	out[idx] = (inl >> 28) & 1
	idx++
	out[idx] = (inl >> 29) & 1
	idx++
	out[idx] = (inl >> 30) & 1
	idx++
	out[idx] = (inl >> 31)
	return in[1:]
}

func unpack2_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 26) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 28) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 26) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 28) % (uint32(1) << 2)
	idx++
	out[idx] = (inl >> 30)

	return in[1:]
}

func unpack3_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 21) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 27) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0] // TODO(nickpoorman): Can probably make these faster by refrencing the index directly instead of reslicing on previous line
	out[idx] |= (inl % (uint32(1) << 1)) << (3 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 19) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 25) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 28) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (3 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 17) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 23) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 26) % (uint32(1) << 3)
	idx++
	out[idx] = (inl >> 29)

	return in[1:]
}

func unpack4_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 4)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	idx++

	return in
}

func unpack5_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 25) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (5 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 23) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (5 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 21) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 26) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (5 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 19) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (5 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 17) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 5)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	idx++

	return in
}

func unpack6_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (6 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (6 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (6 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (6 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 6)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	idx++

	return in
}

func unpack7_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 21) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (7 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 17) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 24) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (7 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (7 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 23) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (7 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 19) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (7 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (7 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 7)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	idx++

	return in
}

func unpack8_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 8)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	idx++

	return in
}

func unpack9_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (9 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 22) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (9 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 17) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (9 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 21) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (9 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (9 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (9 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (9 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 19) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (9 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 9)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	idx++

	return in
}

func unpack10_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (10 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (10 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (10 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (10 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (10 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (10 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (10 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (10 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 10)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	idx++

	return in
}

func unpack11_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (11 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (11 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (11 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (11 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (11 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (11 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 17) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (11 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (11 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 19) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (11 - 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 20) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (11 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 11)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	idx++

	return in
}

func unpack12_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (12 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (12 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (12 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (12 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (12 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (12 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (12 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (12 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 12)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	idx++

	return in
}

func unpack13_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (13 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (13 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (13 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (13 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (13 - 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (13 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (13 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (13 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 17) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (13 - 11)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (13 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 18) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (13 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (13 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 13)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	idx++

	return in
}

func unpack14_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (14 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (14 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (14 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (14 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (14 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (14 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (14 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (14 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (14 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (14 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (14 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (14 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 14)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	idx++

	return in
}

func unpack15_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 15) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (15 - 13)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (15 - 11)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (15 - 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (15 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (15 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (15 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (15 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 16) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (15 - 14)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (15 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (15 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (15 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (15 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (15 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (15 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 15)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	idx++

	return in
}

func unpack16_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	idx++

	return in
}

func unpack17_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (17 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (17 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (17 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (17 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (17 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (17 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (17 - 14)
	idx++
	out[idx] = (inl >> 14) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (17 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (17 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (17 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (17 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (17 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (17 - 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (17 - 11)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (17 - 13)
	idx++
	out[idx] = (inl >> 13) % (uint32(1) << 17)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (17 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	idx++

	return in
}

func unpack18_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (18 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (18 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (18 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (18 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (18 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (18 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (18 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (18 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (18 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (18 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (18 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (18 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (18 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (18 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (18 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 18)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (18 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	idx++

	return in
}

func unpack19_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (19 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (19 - 12)
	idx++
	out[idx] = (inl >> 12) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (19 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (19 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (19 - 11)
	idx++
	out[idx] = (inl >> 11) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (19 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (19 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (19 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (19 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (19 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (19 - 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (19 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (19 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (19 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (19 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (19 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (19 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 19)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (19 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	idx++

	return in
}

func unpack20_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (20 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (20 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (20 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (20 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (20 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (20 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (20 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (20 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (20 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (20 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (20 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (20 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (20 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (20 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (20 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 20)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (20 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	idx++

	return in
}

func unpack21_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (21 - 10)
	idx++
	out[idx] = (inl >> 10) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (21 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (21 - 9)
	idx++
	out[idx] = (inl >> 9) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 19)) << (21 - 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (21 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (21 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (21 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (21 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (21 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (21 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (21 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (21 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (21 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (21 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (21 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (21 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (21 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (21 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (21 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 21)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (21 - 11)
	idx++
	out[idx] = (inl >> 11)
	in = in[1:]
	idx++

	return in
}

func unpack22_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (22 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (22 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (22 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (22 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (22 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (22 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (22 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (22 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (22 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (22 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (22 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (22 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (22 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (22 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (22 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (22 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (22 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (22 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 22)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (22 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (22 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	idx++

	return in
}

func unpack23_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (23 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (23 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 19)) << (23 - 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (23 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (23 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (23 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (23 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (23 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (23 - 11)
	idx++
	out[idx] = (inl >> 11)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (23 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (23 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (23 - 7)
	idx++
	out[idx] = (inl >> 7) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 21)) << (23 - 21)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (23 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (23 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (23 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (23 - 8)
	idx++
	out[idx] = (inl >> 8) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (23 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (23 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (23 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 23)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (23 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (23 - 9)
	idx++
	out[idx] = (inl >> 9)
	in = in[1:]
	idx++

	return in
}

func unpack24_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (24 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (24 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	idx++

	return in
}

func unpack25_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (25 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (25 - 11)
	idx++
	out[idx] = (inl >> 11)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (25 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (25 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (25 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (25 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (25 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 19)) << (25 - 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (25 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (25 - 5)
	idx++
	out[idx] = (inl >> 5) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 23)) << (25 - 23)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (25 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (25 - 9)
	idx++
	out[idx] = (inl >> 9)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (25 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (25 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (25 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (25 - 6)
	idx++
	out[idx] = (inl >> 6) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (25 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (25 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (25 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (25 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 25)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 21)) << (25 - 21)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (25 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (25 - 7)
	idx++
	out[idx] = (inl >> 7)
	in = in[1:]
	idx++

	return in
}

func unpack26_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (26 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (26 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (26 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (26 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 26)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (26 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (26 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (26 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (26 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 26)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (26 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (26 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (26 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (26 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (26 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (26 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (26 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (26 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 26)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (26 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (26 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (26 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (26 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 26)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (26 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (26 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (26 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (26 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	idx++

	return in
}

func unpack27_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 27)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (27 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (27 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (27 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (27 - 7)
	idx++
	out[idx] = (inl >> 7)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (27 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 27)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (27 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 19)) << (27 - 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (27 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (27 - 9)
	idx++
	out[idx] = (inl >> 9)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (27 - 4)
	idx++
	out[idx] = (inl >> 4) % (uint32(1) << 27)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 26)) << (27 - 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 21)) << (27 - 21)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (27 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (27 - 11)
	idx++
	out[idx] = (inl >> 11)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (27 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (27 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 27)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 23)) << (27 - 23)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (27 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (27 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (27 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (27 - 3)
	idx++
	out[idx] = (inl >> 3) % (uint32(1) << 27)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 25)) << (27 - 25)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (27 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (27 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (27 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (27 - 5)
	idx++
	out[idx] = (inl >> 5)
	in = in[1:]
	idx++

	return in
}

func unpack28_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (28 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (28 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (28 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (28 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (28 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (28 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (28 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (28 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (28 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (28 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (28 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (28 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (28 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (28 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (28 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (28 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (28 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (28 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (28 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (28 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (28 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (28 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (28 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (28 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	idx++

	return in
}

func unpack29_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 29)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 26)) << (29 - 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 23)) << (29 - 23)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (29 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (29 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (29 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (29 - 11)
	idx++
	out[idx] = (inl >> 11)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (29 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (29 - 5)
	idx++
	out[idx] = (inl >> 5)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (29 - 2)
	idx++
	out[idx] = (inl >> 2) % (uint32(1) << 29)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 28)) << (29 - 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 25)) << (29 - 25)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (29 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 19)) << (29 - 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (29 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (29 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (29 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (29 - 7)
	idx++
	out[idx] = (inl >> 7)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (29 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (29 - 1)
	idx++
	out[idx] = (inl >> 1) % (uint32(1) << 29)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 27)) << (29 - 27)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (29 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 21)) << (29 - 21)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (29 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (29 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (29 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (29 - 9)
	idx++
	out[idx] = (inl >> 9)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (29 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (29 - 3)
	idx++
	out[idx] = (inl >> 3)
	in = in[1:]
	idx++

	return in
}

func unpack30_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 30)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 28)) << (30 - 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 26)) << (30 - 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (30 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (30 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (30 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (30 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (30 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (30 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (30 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (30 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (30 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (30 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (30 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (30 - 2)
	idx++
	out[idx] = (inl >> 2)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0) % (uint32(1) << 30)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 28)) << (30 - 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 26)) << (30 - 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (30 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (30 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (30 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (30 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (30 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (30 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (30 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (30 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (30 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (30 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (30 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (30 - 2)
	idx++
	out[idx] = (inl >> 2)
	in = in[1:]
	idx++

	return in
}

func unpack31_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0) % (uint32(1) << 31)
	idx++
	out[idx] = (inl >> 31)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 30)) << (31 - 30)
	idx++
	out[idx] = (inl >> 30)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 29)) << (31 - 29)
	idx++
	out[idx] = (inl >> 29)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 28)) << (31 - 28)
	idx++
	out[idx] = (inl >> 28)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 27)) << (31 - 27)
	idx++
	out[idx] = (inl >> 27)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 26)) << (31 - 26)
	idx++
	out[idx] = (inl >> 26)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 25)) << (31 - 25)
	idx++
	out[idx] = (inl >> 25)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 24)) << (31 - 24)
	idx++
	out[idx] = (inl >> 24)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 23)) << (31 - 23)
	idx++
	out[idx] = (inl >> 23)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 22)) << (31 - 22)
	idx++
	out[idx] = (inl >> 22)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 21)) << (31 - 21)
	idx++
	out[idx] = (inl >> 21)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 20)) << (31 - 20)
	idx++
	out[idx] = (inl >> 20)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 19)) << (31 - 19)
	idx++
	out[idx] = (inl >> 19)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 18)) << (31 - 18)
	idx++
	out[idx] = (inl >> 18)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 17)) << (31 - 17)
	idx++
	out[idx] = (inl >> 17)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 16)) << (31 - 16)
	idx++
	out[idx] = (inl >> 16)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 15)) << (31 - 15)
	idx++
	out[idx] = (inl >> 15)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 14)) << (31 - 14)
	idx++
	out[idx] = (inl >> 14)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 13)) << (31 - 13)
	idx++
	out[idx] = (inl >> 13)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 12)) << (31 - 12)
	idx++
	out[idx] = (inl >> 12)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 11)) << (31 - 11)
	idx++
	out[idx] = (inl >> 11)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 10)) << (31 - 10)
	idx++
	out[idx] = (inl >> 10)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 9)) << (31 - 9)
	idx++
	out[idx] = (inl >> 9)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 8)) << (31 - 8)
	idx++
	out[idx] = (inl >> 8)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 7)) << (31 - 7)
	idx++
	out[idx] = (inl >> 7)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 6)) << (31 - 6)
	idx++
	out[idx] = (inl >> 6)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 5)) << (31 - 5)
	idx++
	out[idx] = (inl >> 5)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 4)) << (31 - 4)
	idx++
	out[idx] = (inl >> 4)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 3)) << (31 - 3)
	idx++
	out[idx] = (inl >> 3)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 2)) << (31 - 2)
	idx++
	out[idx] = (inl >> 2)
	in = in[1:]
	inl = in[0]
	out[idx] |= (inl % (uint32(1) << 1)) << (31 - 1)
	idx++
	out[idx] = (inl >> 1)
	in = in[1:]
	idx++

	return in
}

func unpack32_32(in []uint32, out []uint32) []uint32 {
	idx := 0
	var inl uint32 = in[idx]
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	inl = in[0]
	idx++
	out[idx] = (inl >> 0)
	in = in[1:]
	idx++

	return in
}

func nullunpacker32(in []uint32, out []uint32) []uint32 {
	for k := 0; k < 32; k++ {
		out[k] = 0
	}
	return in
}

func Unpack32(in []uint32, out []uint32, batchSize int, numBits int) int {
	// debug.Print("Unpack32-first - batchSize: %d | newBatchSize: %d | numBits: %d | numLoops: %d | in: %#b | out: %#b\n", batchSize, -1, numBits, -1, in, out)

	newBatchSize := batchSize / 32 * 32
	numLoops := newBatchSize / 32

	// debug.Print("Unpack32-second - batchSize: %d | newBatchSize: %d | numBits: %d | numLoops: %d | in: %#b | out: %#b\n", batchSize, newBatchSize, numBits, numLoops, in, out)

	batchSize = newBatchSize
	// if numLoops == 0 {
	// 	numLoops = 1
	// }

	// debug.Print("Unpack32-third - batchSize: %d | newBatchSize: %d | numBits: %d | numLoops: %d | in: %#b | out: %#b\n", batchSize, newBatchSize, numBits, numLoops, in, out)

	switch numBits {
	case 0:
		for i := 0; i < numLoops; i++ {
			in = nullunpacker32(in, out[i*32:])
		}
	case 1:
		for i := 0; i < numLoops; i++ {
			in = unpack1_32(in, out[i*32:])
		}
	case 2:
		for i := 0; i < numLoops; i++ {
			in = unpack2_32(in, out[i*32:])
		}
	case 3:
		for i := 0; i < numLoops; i++ {
			in = unpack3_32(in, out[i*32:])
		}
	case 4:
		for i := 0; i < numLoops; i++ {
			in = unpack4_32(in, out[i*32:])
		}
	case 5:
		for i := 0; i < numLoops; i++ {
			in = unpack5_32(in, out[i*32:])
		}
	case 6:
		for i := 0; i < numLoops; i++ {
			in = unpack6_32(in, out[i*32:])
		}
	case 7:
		for i := 0; i < numLoops; i++ {
			in = unpack7_32(in, out[i*32:])
		}
	case 8:
		for i := 0; i < numLoops; i++ {
			in = unpack8_32(in, out[i*32:])
		}
	case 9:
		for i := 0; i < numLoops; i++ {
			in = unpack9_32(in, out[i*32:])
		}
	case 10:
		for i := 0; i < numLoops; i++ {
			in = unpack10_32(in, out[i*32:])
		}
	case 11:
		for i := 0; i < numLoops; i++ {
			in = unpack11_32(in, out[i*32:])
		}
	case 12:
		for i := 0; i < numLoops; i++ {
			in = unpack12_32(in, out[i*32:])
		}
	case 13:
		for i := 0; i < numLoops; i++ {
			in = unpack13_32(in, out[i*32:])
		}
	case 14:
		for i := 0; i < numLoops; i++ {
			in = unpack14_32(in, out[i*32:])
		}
	case 15:
		for i := 0; i < numLoops; i++ {
			in = unpack15_32(in, out[i*32:])
		}
	case 16:
		for i := 0; i < numLoops; i++ {
			in = unpack16_32(in, out[i*32:])
		}
	case 17:
		for i := 0; i < numLoops; i++ {
			in = unpack17_32(in, out[i*32:])
		}
	case 18:
		for i := 0; i < numLoops; i++ {
			in = unpack18_32(in, out[i*32:])
		}
	case 19:
		for i := 0; i < numLoops; i++ {
			in = unpack19_32(in, out[i*32:])
		}
	case 20:
		for i := 0; i < numLoops; i++ {
			in = unpack20_32(in, out[i*32:])
		}
	case 21:
		for i := 0; i < numLoops; i++ {
			in = unpack21_32(in, out[i*32:])
		}
	case 22:
		for i := 0; i < numLoops; i++ {
			in = unpack22_32(in, out[i*32:])
		}
	case 23:
		for i := 0; i < numLoops; i++ {
			in = unpack23_32(in, out[i*32:])
		}
	case 24:
		for i := 0; i < numLoops; i++ {
			in = unpack24_32(in, out[i*32:])
		}
	case 25:
		for i := 0; i < numLoops; i++ {
			in = unpack25_32(in, out[i*32:])
		}
	case 26:
		for i := 0; i < numLoops; i++ {
			in = unpack26_32(in, out[i*32:])
		}
	case 27:
		for i := 0; i < numLoops; i++ {
			in = unpack27_32(in, out[i*32:])
		}
	case 28:
		for i := 0; i < numLoops; i++ {
			in = unpack28_32(in, out[i*32:])
		}
	case 29:
		for i := 0; i < numLoops; i++ {
			in = unpack29_32(in, out[i*32:])
		}
	case 30:
		for i := 0; i < numLoops; i++ {
			in = unpack30_32(in, out[i*32:])
		}
	case 31:
		for i := 0; i < numLoops; i++ {
			in = unpack31_32(in, out[i*32:])
		}
	case 32:
		for i := 0; i < numLoops; i++ {
			in = unpack32_32(in, out[i*32:])
		}
	default:
		debug.Assert(false, fmt.Sprintf("Unsupported numBits: %d", numBits))
	}

	return batchSize
}
