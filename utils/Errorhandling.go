package utils

import (
	"fmt"
	"runtime"
)

func StackError(err error, args ...interface{}) error {

	//Get the function name in which the error occured
	pc := make([]uintptr, 1)
	n := runtime.Callers(1, pc)
	pc = pc[:n]
	frames := runtime.CallersFrames(pc)
	frame, _ := frames.Next()

	var msg string
	if len(args) > 0 {
		msg = fmt.Sprintf(args[0].(string), args[1:])
	}

	return fmt.Errorf("%s: %s\n%s", frame.Function, msg, err)
}
