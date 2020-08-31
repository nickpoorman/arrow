package arrow

import (
	"errors"
	"fmt"
)

var ArrowException = errors.New("arrow exception")
var ArrowNYIException = fmt.Errorf("Not yet implemented: %w", ArrowException)
