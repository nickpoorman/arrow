// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/arrow/bitutil"
)

const (
	// BooleanSizeBytes specifies the number of bytes required to store a single bool in memory
	BooleanSizeBytes = int(unsafe.Sizeof(bool(false)))
)

type booleanTraits struct{}

var BooleanTraits booleanTraits

// BytesRequired returns the number of bytes required to store n elements in memory.
func (booleanTraits) BytesRequired(n int) int { return bitutil.CeilByte(n) / 8 }

// PutValue
func (booleanTraits) PutValue(b []byte, v bool) {
	if v {
		b[0] = 1
	} else {
		b[0] = 0
	}
}

// GetValue returns a single bool from the slice of bytes b.
func (booleanTraits) GetValue(b []byte) bool {
	return b[0] != 0
}

// CastFromBytes reinterprets the slice b to a slice of type bool.
//
// NOTE: len(b) must be a multiple of BooleanSizeBytes.
func (booleanTraits) CastFromBytes(b []byte) []bool {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []bool
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / BooleanSizeBytes
	s.Cap = h.Cap / BooleanSizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (booleanTraits) CastToBytes(b []bool) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * BooleanSizeBytes
	s.Cap = h.Cap * BooleanSizeBytes

	return res
}
