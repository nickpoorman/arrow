package main

// go build -o ~/go/bin/ctogo ./cmd/ctogo/main.go

import (
	"bufio"
	"log"
	"os"
	"regexp"
)

type findReplace struct {
	Find    *regexp.Regexp
	Replace []byte
}

func main() {
	CToGo()
}

func CToGo() {
	ReadStdIn()
}

func ReadStdIn() {
	stdin := bufio.NewScanner(os.Stdin)
	stdout := bufio.NewWriter(os.Stdout)
	defer stdout.Flush()

	for stdin.Scan() {
		processedLine := ProcessLine(stdin.Bytes())
		stdout.Write(processedLine)
		stdout.WriteByte('\n')
	}

	if err := stdin.Err(); err != nil {
		log.Fatal("Got error: ", err)
	}
}

func ProcessLine(line []byte) []byte {
	for _, re := range typeRegexps {
		line = re.Find.ReplaceAll(line, re.Replace)
	}
	return line
}

var typeRegexps = []findReplace{
	{
		Find:    regexp.MustCompile(`R"\((.*)\)"`),
		Replace: []byte("`$1`"),
	},
	{
		Find:    regexp.MustCompile(`Type::BOOLEAN`),
		Replace: []byte("Type_BOOLEAN"),
	},
	{
		Find:    regexp.MustCompile(`Type::INT32`),
		Replace: []byte("Type_INT32"),
	},
	{
		Find:    regexp.MustCompile(`Type::INT64`),
		Replace: []byte("Type_INT64"),
	},
	{
		Find:    regexp.MustCompile(`Type::INT96`),
		Replace: []byte("Type_INT96"),
	},
	{
		Find:    regexp.MustCompile(`Type::FLOAT`),
		Replace: []byte("Type_FLOAT"),
	},
	{
		Find:    regexp.MustCompile(`Type::DOUBLE`),
		Replace: []byte("Type_DOUBLE"),
	},
	{
		Find:    regexp.MustCompile(`Type::BYTE_ARRAY`),
		Replace: []byte("Type_BYTE_ARRAY"),
	},
	{
		Find:    regexp.MustCompile(`Type::FIXED_LEN_BYTE_ARRAY`),
		Replace: []byte("Type_FIXED_LEN_BYTE_ARRAY"),
	},
	{
		Find:    regexp.MustCompile(`Type::UNDEFINED`),
		Replace: []byte("Type_UNDEFINED"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::NONE`),
		Replace: []byte("ConvertedType_NONE"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::UTF8`),
		Replace: []byte("ConvertedType_UTF8"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::MAP`),
		Replace: []byte("ConvertedType_MAP"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::MAP_KEY_VALUE`),
		Replace: []byte("ConvertedType_MAP_KEY_VALUE"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::LIST`),
		Replace: []byte("ConvertedType_LIST"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::ENUM`),
		Replace: []byte("ConvertedType_ENUM"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::DECIMAL`),
		Replace: []byte("ConvertedType_DECIMAL"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::DATE`),
		Replace: []byte("ConvertedType_DATE"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::TIME_MILLIS`),
		Replace: []byte("ConvertedType_TIME_MILLIS"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::TIME_MICROS`),
		Replace: []byte("ConvertedType_TIME_MICROS"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::TIMESTAMP_MILLIS`),
		Replace: []byte("ConvertedType_TIMESTAMP_MILLIS"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::TIMESTAMP_MICROS`),
		Replace: []byte("ConvertedType_TIMESTAMP_MICROS"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::UINT_8`),
		Replace: []byte("ConvertedType_UINT_8"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::UINT_16`),
		Replace: []byte("ConvertedType_UINT_16"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::UINT_32`),
		Replace: []byte("ConvertedType_UINT_32"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::UINT_64`),
		Replace: []byte("ConvertedType_UINT_64"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::INT_8`),
		Replace: []byte("ConvertedType_INT_8"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::INT_16`),
		Replace: []byte("ConvertedType_INT_16"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::INT_32`),
		Replace: []byte("ConvertedType_INT_32"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::INT_64`),
		Replace: []byte("ConvertedType_INT_64"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::JSON`),
		Replace: []byte("ConvertedType_JSON"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::BSON`),
		Replace: []byte("ConvertedType_BSON"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::INTERVAL`),
		Replace: []byte("ConvertedType_INTERVAL"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::NA`),
		Replace: []byte("ConvertedType_NA"),
	},
	{
		Find:    regexp.MustCompile(`ConvertedType::UNDEFINED`),
		Replace: []byte("ConvertedType_UNDEFINED"),
	},
	{
		Find:    regexp.MustCompile(`Repetition::REQUIRED`),
		Replace: []byte("RepetitionType_REQUIRED"),
	},
	{
		Find:    regexp.MustCompile(`Repetition::OPTIONAL`),
		Replace: []byte("RepetitionType_OPTIONAL"),
	},
	{
		Find:    regexp.MustCompile(`Repetition::REPEATED`),
		Replace: []byte("RepetitionType_REPEATED"),
	},
	{
		Find:    regexp.MustCompile(`Repetition::UNDEFINED`),
		Replace: []byte("RepetitionType_UNDEFINED"),
	},
	{
		Find:    regexp.MustCompile(`SortOrder::UNKNOWN`),
		Replace: []byte("SortOrder_UNKNOWN"),
	},
	{
		Find:    regexp.MustCompile(`SortOrder::SIGNED`),
		Replace: []byte("SortOrder_SIGNED"),
	},
	{
		Find:    regexp.MustCompile(`SortOrder::UNSIGNED`),
		Replace: []byte("SortOrder_UNSIGNED"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Unknown\(\)`),
		Replace: []byte("NewUnknownLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::String\(\)`),
		Replace: []byte("NewStringLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Map\(\)`),
		Replace: []byte("NewMapLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::List\(\)`),
		Replace: []byte("NewListLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Enum\(\)`),
		Replace: []byte("NewEnumLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Decimal\((\d+)\)`),
		Replace: []byte("NewDecimalLogicalType($1, 0)"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Decimal\((\d+),\s+(\d+)\)`),
		Replace: []byte("NewDecimalLogicalType($1, $2)"),
	},
	{
		Find:    regexp.MustCompile(`NewDecimalLogicalType\((\d+),\s+(\d+)\)`),
		Replace: []byte("newDecimalLogicalType(t, $1, $2)"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Date\(\)`),
		Replace: []byte("NewDateLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Time\((.*)\)`),
		Replace: []byte("NewTimeLogicalType($1)"),
	},
	{
		Find:    regexp.MustCompile(`NewTimeLogicalType\((.*)\)`),
		Replace: []byte("newTimeLogicalType(t, $1)"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Timestamp\((.*),\s+(L.*(N|S))\)`),
		Replace: []byte("NewTimestampLogicalType($1, $2, false, false)"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Timestamp\((.*),\s+(L.*(N|S)),\s+(true|false),\s+(true|false)\)`),
		Replace: []byte("NewTimestampLogicalType($1, $2, $4, $5)"),
	},
	{
		Find:    regexp.MustCompile(`NewTimestampLogicalType\((.*)\)`),
		Replace: []byte("newTimestampLogicalType(t, $1)"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Interval\(\)`),
		Replace: []byte("NewIntervalLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Int\((\d+),\s+(true|false)\)`),
		Replace: []byte("NewIntLogicalType($1, $2)"),
	},
	{
		Find:    regexp.MustCompile(`NewIntLogicalType\((.*)\)`),
		Replace: []byte("newIntLogicalType(t, $1)"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::Null\(\)`),
		Replace: []byte("NewNullLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::JSON\(\)`),
		Replace: []byte("NewJSONLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::BSON\(\)`),
		Replace: []byte("NewBSONLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::UUID\(\)`),
		Replace: []byte("NewUUIDLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::None\(\)`),
		Replace: []byte("NewNoLogicalType()"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::UNKNOWN`),
		Replace: []byte("LogicalTypeKind_UNKNOWN"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::STRING`),
		Replace: []byte("LogicalTypeKind_STRING"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::MAP`),
		Replace: []byte("LogicalTypeKind_MAP"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::LIST`),
		Replace: []byte("LogicalTypeKind_LIST"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::ENUM`),
		Replace: []byte("LogicalTypeKind_ENUM"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::DECIMAL`),
		Replace: []byte("LogicalTypeKind_DECIMAL"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::DATE`),
		Replace: []byte("LogicalTypeKind_DATE"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::TIME`),
		Replace: []byte("LogicalTypeKind_TIME"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::TIMESTAMP`),
		Replace: []byte("LogicalTypeKind_TIMESTAMP"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::INTERVAL`),
		Replace: []byte("LogicalTypeKind_INTERVAL"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::INT`),
		Replace: []byte("LogicalTypeKind_INT"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::NIL`),
		Replace: []byte("LogicalTypeKind_NIL"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::JSON`),
		Replace: []byte("LogicalTypeKind_JSON"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::BSON`),
		Replace: []byte("LogicalTypeKind_BSON"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::UUID`),
		Replace: []byte("LogicalTypeKind_UUID"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::NONE`),
		Replace: []byte("LogicalTypeKind_NONE"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::TimeUnit::UNKNOWN`),
		Replace: []byte("LogicalTypeTimeUnit_UNKNOWN"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::TimeUnit::MILLIS`),
		Replace: []byte("LogicalTypeTimeUnit_MILLIS"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::TimeUnit::MICROS`),
		Replace: []byte("LogicalTypeTimeUnit_MICROS"),
	},
	{
		Find:    regexp.MustCompile(`LogicalType::TimeUnit::NANOS`),
		Replace: []byte("LogicalTypeTimeUnit_NANOS"),
	},
}
