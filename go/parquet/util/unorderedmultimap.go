package util

type UnorderedMultimapStringInt struct {
	m map[string][]int
}

func NewUnorderedMultimapStringInt() *UnorderedMultimapStringInt {
	return &UnorderedMultimapStringInt{
		m: make(map[string][]int),
	}
}

// Clear clears the contents
func (mm *UnorderedMultimapStringInt) Clear() {
	mm.m = make(map[string][]int)
}

// Emplace is an alias for Insert.
func (mm *UnorderedMultimapStringInt) Emplace(key string, value int) {
	mm.Insert(key, value)
}

// Insert a new element into the container.
func (mm *UnorderedMultimapStringInt) Insert(key string, value int) {
	mm.m[key] = append(mm.m[key], value)
}

// If several values share the same key, it is unspecified which one is returned.
// If no value is found, the boolean return value will be false.
func (mm *UnorderedMultimapStringInt) Find(key string) (int, bool) {
	values, found := mm.m[key]
	if !found {
		return -1, false
	}
	if len(values) > 0 {
		return values[0], true
	}
	return -1, false
}

func (mm *UnorderedMultimapStringInt) Get(key string) ([]int, bool) {
	values, found := mm.m[key]
	return values, found
}
