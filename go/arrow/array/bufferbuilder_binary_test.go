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

package array

import (
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBinaryBufferBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bb := NewBinaryBufferBuilder(mem)
	exp := []byte{0x01, 0x05, 0x05, 0x09, 0x02, 0x03}
	bb.AppendValues(exp[:3])
	bb.AppendValues(exp[3:])

	expBuf := []byte{0x01, 0x05, 0x05, 0x09, 0x02, 0x03}
	assert.Equal(t, expBuf, bb.Bytes(), "unexpected byte values")
	assert.Equal(t, exp, bb.Values(), "unexpected byte values")
	assert.Equal(t, len(exp), bb.Len(), "unexpected Len()")

	buflen := bb.Len()
	bfr := bb.Finish()
	assert.Equal(t, buflen, bfr.Len(), "Buffer was not resized")
	assert.Len(t, bfr.Bytes(), bfr.Len(), "Buffer.Bytes() != Buffer.Len()")
	bfr.Release()

	assert.Len(t, bb.Bytes(), 0, "BufferBuilder was not reset after Finish")
	assert.Zero(t, bb.Len(), "BufferBuilder was not reset after Finish")
	bb.Release()
}

func TestBinaryBufferBuilder_AppendValue(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bb := NewBinaryBufferBuilder(mem)
	exp := []byte{0x01, 0x05, 0x05, 0x09, 0x02, 0x03}
	for _, v := range exp {
		bb.AppendValue(v)
	}

	expBuf := []byte{0x01, 0x05, 0x05, 0x09, 0x02, 0x03}
	assert.Equal(t, expBuf, bb.Bytes(), "unexpected byte values")
	assert.Equal(t, exp, bb.Values(), "unexpected byte values")
	assert.Equal(t, len(exp), bb.Len(), "unexpected Len()")
	bb.Release()
}
