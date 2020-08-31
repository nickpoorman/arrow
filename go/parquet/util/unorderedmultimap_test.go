package util

import "testing"

func TestUnorderedMultiMapStringIntFind(t *testing.T) {
	mm := NewUnorderedMultimapStringInt()
	mm.Emplace("a", 1)
	mm.Insert("a", 2)
	mm.Insert("a", 3)

	v, ok := mm.Find("a")
	if !ok {
		t.Errorf("Expected a to exist")
	}
	if v != 1 {
		t.Errorf("Expected v to equal 1")
	}

	v2, ok2 := mm.Find("b")
	if ok2 {
		t.Errorf("Expected b to not exist")
	}
	if v2 != -1 {
		t.Errorf("Expected v to equal -1")
	}
}

func TestUnorderedMultiMapStringIntGet(t *testing.T) {
	mm := NewUnorderedMultimapStringInt()
	mm.Emplace("a", 1)
	mm.Insert("a", 2)
	mm.Insert("a", 3)

	v, ok := mm.Get("a")
	if !ok {
		t.Errorf("Expected a to exist")
	}
	values := []int{1, 2, 3}
	if len(values) != len(v) {
		t.Errorf("Expected v and values length to be equal")
	}
	for i, value := range values {
		if value != values[i] {
			t.Errorf("Expected value (%d) to equal value[i] (%d)", value, values[i])
		}
	}

	_, ok2 := mm.Get("b")
	if ok2 {
		t.Errorf("Expected b to not exist")
	}
}
