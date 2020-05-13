package testutil

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// ----------------------------------------------------------------------
// Assert Helpers

func padMsg(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg != "" {
		msg = fmt.Sprintf(" %s", msg)
	}
	return msg
}

func AssertDeepEq(t *testing.T, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AssertDeepEq: got=%+v; want=%+v\n", got, want)
	}
}

func AssertDeepEqM(t *testing.T, got, want interface{}, msg string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AssertDeepEq: got=%+v; want=%+v%s\n", got, want, padMsg(msg))
	}
}

func AssertNotDeepEq(t *testing.T, got, want interface{}) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		t.Errorf("AssertNotDeepEq: got=%+v; !want=%+v\n", got, want)
	}
}

func AssertEqString(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("AssertEqString: got=\n%s\nwant=\n%s\n", got, want)
	}
}

func AssertEqInt(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("AssertEqInt: got=%d; want=%d\n", got, want)
	}
}

func AssertTrue(t *testing.T, got bool) {
	t.Helper()
	if !got {
		t.Errorf("AssertTrue: got=%t; want=%t\n", got, true)
	}
}

func AssertTrueM(t *testing.T, got bool, msg string) {
	t.Helper()
	if !got {
		t.Errorf("AssertTrue: got=%t; want=%t%s\n", got, true, padMsg(msg))
	}
}

func AssertFalse(t *testing.T, got bool) {
	t.Helper()
	if got {
		t.Errorf("AssertFalse: got=%t; want=%t\n", got, false)
	}
}

func AssertFalseM(t *testing.T, got bool, msg string) {
	t.Helper()
	if got {
		t.Errorf("AssertFalse: got=%t; want=%t%s\n", got, false, padMsg(msg))
	}
}

func AssertNil(t *testing.T, o interface{}) {
	t.Helper()
	if o != nil {
		t.Errorf("AssertNil: got=%v; want=%v\n", o, nil)
	}
}

func AssertNotNil(t *testing.T, o interface{}) {
	t.Helper()
	if o == nil {
		t.Errorf("AssertNotNil: got=%v\n", o)
	}
}

func AssertErrorIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("AssertErrorIs: got=%v; want=%v\n", got, want)
	}
}

func AssertLT(t *testing.T, got, want int) {
	t.Helper()
	if !(got < want) {
		t.Errorf("AssertLT: got=%d; want=<%d\n", got, want)
	}
}

func AssertGT(t *testing.T, got, want int) {
	t.Helper()
	if !(got > want) {
		t.Errorf("AssertGT: got=%d; want=<%d\n", got, want)
	}
}

func AssertBytesEq(t *testing.T, got, want []byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("AssertBytesEq: got=%v; want=%v\n", got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("AssertBytesEq: got=%v; want=%v\n", got, want)
		}
	}
}

func AssertBitsEq(t *testing.T, got, want []byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("AssertBitsEq: got=%v; want=%v\n", got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("AssertBitsEq: \ngot=\n%#b\nwant=\n%#b\n", got, want)
		}
	}
}
