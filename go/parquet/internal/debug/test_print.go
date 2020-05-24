package debug

import "fmt"

func Print(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}
