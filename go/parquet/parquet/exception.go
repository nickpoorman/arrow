package parquet

import (
	"errors"
	"fmt"
)

var ParquetException = errors.New("parquet exception")
var ParquetEofException = fmt.Errorf("Unexpected end of stream: %w", ParquetException)
var ParquetNYIException = fmt.Errorf("Not yet implemented: %w", ParquetException)
