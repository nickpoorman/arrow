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

// +build assert

package debug

import (
	"fmt"
	"reflect"
)

type CompareType int

const (
	compareLess CompareType = iota - 1
	compareEqual
	compareGreater
)

// Assert will panic with msg if cond is false.
func Assert(cond bool, msg interface{}) {
	if !cond {
		panic(msg)
	}
}

// AssertGT asserts that the first element is greater than the second
//
//    assert.AssertGT(t, 2, 1)
//    assert.AssertGT(t, float64(2), float64(1))
//    assert.AssertGT(t, "b", "a")
func AssertGT(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {
	compareTwoValues(e1, e2, []CompareType{compareGreater}, "\"%v\" is not greater than \"%v\"", msgAndArgs)
}

// AssertGE asserts that the first element is greater than or equal to the second
//
//    assert.AssertGE(t, 2, 1)
//    assert.AssertGE(t, 2, 2)
//    assert.AssertGE(t, "b", "a")
//    assert.AssertGE(t, "b", "b")
func AssertGE(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {
	compareTwoValues(e1, e2, []CompareType{compareGreater, compareEqual}, "\"%v\" is not greater than or equal to \"%v\"", msgAndArgs)
}

// AssertLT asserts that the first element is less than the second
//
//    assert.AssertLT(t, 1, 2)
//    assert.AssertLT(t, float64(1), float64(2))
//    assert.AssertLT(t, "a", "b")
func AssertLT(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {
	compareTwoValues(e1, e2, []CompareType{compareLess}, "\"%v\" is not less than \"%v\"", msgAndArgs)
}

// AssertLE asserts that the first element is less than or equal to the second
//
//    assert.AssertLE(t, 1, 2)
//    assert.AssertLE(t, 2, 2)
//    assert.AssertLE(t, "a", "b")
//    assert.AssertLE(t, "b", "b")
func AssertLE(e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) {
	compareTwoValues(e1, e2, []CompareType{compareLess, compareEqual}, "\"%v\" is not less than or equal to \"%v\"", msgAndArgs)
}

func compareTwoValues(e1 interface{}, e2 interface{}, allowedComparesResults []CompareType, failMessage string, msgAndArgs ...interface{}) {
	e1Kind := reflect.ValueOf(e1).Kind()
	e2Kind := reflect.ValueOf(e2).Kind()
	if e1Kind != e2Kind {
		panic(messageFromMsgAndArgs(fmt.Sprintf("Elements should be the same type"), msgAndArgs...))
	}

	compareResult, isComparable := compare(e1, e2, e1Kind)
	if !isComparable {
		panic(messageFromMsgAndArgs(fmt.Sprintf("Can not compare type \"%s\"", reflect.TypeOf(e1)), msgAndArgs...))
	}

	if !containsValue(allowedComparesResults, compareResult) {
		panic(messageFromMsgAndArgs(fmt.Sprintf(failMessage, e1, e2), msgAndArgs...))
	}
}

func containsValue(values []CompareType, value CompareType) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}

	return false
}

func compare(obj1, obj2 interface{}, kind reflect.Kind) (CompareType, bool) {
	switch kind {
	case reflect.Int:
		{
			intobj1 := obj1.(int)
			intobj2 := obj2.(int)
			if intobj1 > intobj2 {
				return compareGreater, true
			}
			if intobj1 == intobj2 {
				return compareEqual, true
			}
			if intobj1 < intobj2 {
				return compareLess, true
			}
		}
	case reflect.Int8:
		{
			int8obj1 := obj1.(int8)
			int8obj2 := obj2.(int8)
			if int8obj1 > int8obj2 {
				return compareGreater, true
			}
			if int8obj1 == int8obj2 {
				return compareEqual, true
			}
			if int8obj1 < int8obj2 {
				return compareLess, true
			}
		}
	case reflect.Int16:
		{
			int16obj1 := obj1.(int16)
			int16obj2 := obj2.(int16)
			if int16obj1 > int16obj2 {
				return compareGreater, true
			}
			if int16obj1 == int16obj2 {
				return compareEqual, true
			}
			if int16obj1 < int16obj2 {
				return compareLess, true
			}
		}
	case reflect.Int32:
		{
			int32obj1 := obj1.(int32)
			int32obj2 := obj2.(int32)
			if int32obj1 > int32obj2 {
				return compareGreater, true
			}
			if int32obj1 == int32obj2 {
				return compareEqual, true
			}
			if int32obj1 < int32obj2 {
				return compareLess, true
			}
		}
	case reflect.Int64:
		{
			int64obj1 := obj1.(int64)
			int64obj2 := obj2.(int64)
			if int64obj1 > int64obj2 {
				return compareGreater, true
			}
			if int64obj1 == int64obj2 {
				return compareEqual, true
			}
			if int64obj1 < int64obj2 {
				return compareLess, true
			}
		}
	case reflect.Uint:
		{
			uintobj1 := obj1.(uint)
			uintobj2 := obj2.(uint)
			if uintobj1 > uintobj2 {
				return compareGreater, true
			}
			if uintobj1 == uintobj2 {
				return compareEqual, true
			}
			if uintobj1 < uintobj2 {
				return compareLess, true
			}
		}
	case reflect.Uint8:
		{
			uint8obj1 := obj1.(uint8)
			uint8obj2 := obj2.(uint8)
			if uint8obj1 > uint8obj2 {
				return compareGreater, true
			}
			if uint8obj1 == uint8obj2 {
				return compareEqual, true
			}
			if uint8obj1 < uint8obj2 {
				return compareLess, true
			}
		}
	case reflect.Uint16:
		{
			uint16obj1 := obj1.(uint16)
			uint16obj2 := obj2.(uint16)
			if uint16obj1 > uint16obj2 {
				return compareGreater, true
			}
			if uint16obj1 == uint16obj2 {
				return compareEqual, true
			}
			if uint16obj1 < uint16obj2 {
				return compareLess, true
			}
		}
	case reflect.Uint32:
		{
			uint32obj1 := obj1.(uint32)
			uint32obj2 := obj2.(uint32)
			if uint32obj1 > uint32obj2 {
				return compareGreater, true
			}
			if uint32obj1 == uint32obj2 {
				return compareEqual, true
			}
			if uint32obj1 < uint32obj2 {
				return compareLess, true
			}
		}
	case reflect.Uint64:
		{
			uint64obj1 := obj1.(uint64)
			uint64obj2 := obj2.(uint64)
			if uint64obj1 > uint64obj2 {
				return compareGreater, true
			}
			if uint64obj1 == uint64obj2 {
				return compareEqual, true
			}
			if uint64obj1 < uint64obj2 {
				return compareLess, true
			}
		}
	case reflect.Float32:
		{
			float32obj1 := obj1.(float32)
			float32obj2 := obj2.(float32)
			if float32obj1 > float32obj2 {
				return compareGreater, true
			}
			if float32obj1 == float32obj2 {
				return compareEqual, true
			}
			if float32obj1 < float32obj2 {
				return compareLess, true
			}
		}
	case reflect.Float64:
		{
			float64obj1 := obj1.(float64)
			float64obj2 := obj2.(float64)
			if float64obj1 > float64obj2 {
				return compareGreater, true
			}
			if float64obj1 == float64obj2 {
				return compareEqual, true
			}
			if float64obj1 < float64obj2 {
				return compareLess, true
			}
		}
	case reflect.String:
		{
			stringobj1 := obj1.(string)
			stringobj2 := obj2.(string)
			if stringobj1 > stringobj2 {
				return compareGreater, true
			}
			if stringobj1 == stringobj2 {
				return compareEqual, true
			}
			if stringobj1 < stringobj2 {
				return compareLess, true
			}
		}
	}

	return compareEqual, false
}

func messageFromMsgAndArgs(message string, msgAndArgs ...interface{}) string {
	if len(msgAndArgs) == 0 || msgAndArgs == nil {
		return message
	}
	if len(msgAndArgs) == 1 {
		msg := msgAndArgs[0]
		if msgAsStr, ok := msg.(string); ok {
			return fmt.Sprintf("%s: %+v", message, msgAsStr)
		}
		return fmt.Sprintf("%s: %+v", message, msg)
	}
	if len(msgAndArgs) > 1 {
		return fmt.Sprintf("%s: %+v", message, fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...))
	}
	return message
}
