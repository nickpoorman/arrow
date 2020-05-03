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

package memory_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewResizableBuffer(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)
	buf.Retain() // refCount == 2

	exp := 10
	buf.Resize(exp)
	assert.NotNil(t, buf.Bytes())
	assert.Equal(t, exp, len(buf.Bytes()))
	assert.Equal(t, exp, buf.Len())

	buf.Release() // refCount == 1
	assert.NotNil(t, buf.Bytes())

	buf.Release() // refCount == 0
	assert.Nil(t, buf.Bytes())
	assert.Zero(t, buf.Len())
}

func TestBufferReset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)

	newBytes := []byte("some-new-bytes")
	buf.Reset(newBytes)
	assert.Equal(t, newBytes, buf.Bytes())
	assert.Equal(t, len(newBytes), buf.Len())
}

func TestBufferEqualsWithSameContent(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	bufferSize := 128 * 1024
	rawBuffer1 := pool.Allocate(bufferSize)
	memory.Set(rawBuffer1, 12)
	rawBuffer2 := pool.Allocate(bufferSize)
	memory.Set(rawBuffer2, 12)
	rawBuffer3 := pool.Allocate(bufferSize)
	memory.Set(rawBuffer3, 3)

	buffer1 := memory.NewBufferBytes(rawBuffer1)
	buffer2 := memory.NewBufferBytes(rawBuffer2)
	buffer3 := memory.NewBufferBytes(rawBuffer3)

	assert.True(t, buffer1.Equals(buffer1))
	assert.True(t, buffer1.Equals(buffer2))
	assert.False(t, buffer1.Equals(buffer3))

	pool.Free(rawBuffer1)
	pool.Free(rawBuffer2)
	pool.Free(rawBuffer3)
}

func TestBufferEqualsWithSameBuffer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	bufferSize := 128 * 1024
	rawBuffer := pool.Allocate(bufferSize)
	memory.Set(rawBuffer, 111)

	buffer1 := memory.NewBufferBytes(rawBuffer)
	buffer2 := memory.NewBufferBytes(rawBuffer)
	assert.True(t, buffer1.Equals(buffer2))

	nbytes := bufferSize / 2
	buffer3 := memory.NewBufferBytes(rawBuffer[:nbytes])
	assert.True(t, buffer1.EqualsLen(buffer3, nbytes))
	assert.False(t, buffer1.EqualsLen(buffer3, nbytes+1))

	pool.Free(rawBuffer)
}
