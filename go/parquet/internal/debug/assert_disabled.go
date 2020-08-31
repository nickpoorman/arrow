// Copyright 2019 Nick Poorman
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !assert

package debug

// Assert will panic with msg if cond is false.
func Assert(cond bool, msg interface{}) {}

// AssertGT asserts that the first element is greater than the second
//
//    assert.AssertGT(t, 2, 1)
//    assert.AssertGT(t, float64(2), float64(1))
//    assert.AssertGT(t, "b", "a")
func AssertGT(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {}

// AssertGE asserts that the first element is greater than or equal to the second
//
//    assert.AssertGE(t, 2, 1)
//    assert.AssertGE(t, 2, 2)
//    assert.AssertGE(t, "b", "a")
//    assert.AssertGE(t, "b", "b")
func AssertGE(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {}

// AssertLT asserts that the first element is less than the second
//
//    assert.AssertLT(t, 1, 2)
//    assert.AssertLT(t, float64(1), float64(2))
//    assert.AssertLT(t, "a", "b")
func AssertLT(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {}

// AssertLE asserts that the first element is less than or equal to the second
//
//    assert.AssertLE(t, 1, 2)
//    assert.AssertLE(t, 2, 2)
//    assert.AssertLE(t, "a", "b")
//    assert.AssertLE(t, "b", "b")
func AssertLE(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {}
