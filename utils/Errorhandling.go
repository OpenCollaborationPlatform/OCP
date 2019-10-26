package utils

import (
	"fmt"
	"runtime"
)

func StackError(err error, args ...interface{}) error {

	//Get the function name in which the error occured
	pc := make([]uintptr, 1)
	n := runtime.Callers(2, pc)
	pc = pc[:n]
	frames := runtime.CallersFrames(pc)
	frame, _ := frames.Next()

	var msg string
	if len(args) > 1 {
		msg = fmt.Sprintf(args[0].(string), args[1:]...)
	} else if len(args) == 1 {
		msg = args[0].(string)
	}

	return fmt.Errorf("%v (Line %v): %v\n%v", frame.Function, frame.Line, msg, err)
}
