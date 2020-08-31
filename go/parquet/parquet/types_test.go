package parquet

import "testing"

func TestTypeToString(t *testing.T) {
	cases := []struct {
		want string
		t    Type
	}{
		{"BOOLEAN", Type_BOOLEAN},
		{"INT32", Type_INT32},
		{"INT64", Type_INT64},
		{"INT96", Type_INT96},
		{"FLOAT", Type_FLOAT},
		{"DOUBLE", Type_DOUBLE},
		{"BYTE_ARRAY", Type_BYTE_ARRAY},
		{"FIXED_LEN_BYTE_ARRAY", Type_FIXED_LEN_BYTE_ARRAY},
	}

	for _, c := range cases {
		got := TypeToString(c.t)
		if c.want != got {
			t.Errorf("got=%s; want=%s\n", got, c.want)
		}
	}
}

func TestConvertedTypeToString(t *testing.T) {
	cases := []struct {
		want string
		t    ConvertedType
	}{
		{"NONE", ConvertedType_NONE},
		{"UTF8", ConvertedType_UTF8},
		{"MAP", ConvertedType_MAP},
		{"MAP_KEY_VALUE", ConvertedType_MAP_KEY_VALUE},
		{"LIST", ConvertedType_LIST},
		{"ENUM", ConvertedType_ENUM},
		{"DECIMAL", ConvertedType_DECIMAL},
		{"DATE", ConvertedType_DATE},
		{"TIME_MILLIS", ConvertedType_TIME_MILLIS},
		{"TIME_MICROS", ConvertedType_TIME_MICROS},
		{"TIMESTAMP_MILLIS", ConvertedType_TIMESTAMP_MILLIS},
		{"TIMESTAMP_MICROS", ConvertedType_TIMESTAMP_MICROS},
		{"UINT_8", ConvertedType_UINT_8},
		{"UINT_16", ConvertedType_UINT_16},
		{"UINT_32", ConvertedType_UINT_32},
		{"UINT_64", ConvertedType_UINT_64},
		{"INT_8", ConvertedType_INT_8},
		{"INT_16", ConvertedType_INT_16},
		{"INT_32", ConvertedType_INT_32},
		{"INT_64", ConvertedType_INT_64},
		{"JSON", ConvertedType_JSON},
		{"BSON", ConvertedType_BSON},
		{"INTERVAL", ConvertedType_INTERVAL},
	}

	for _, c := range cases {
		got := ConvertedTypeToString(c.t)
		if c.want != got {
			t.Errorf("got=%s; want=%s\n", got, c.want)
		}
	}
}
