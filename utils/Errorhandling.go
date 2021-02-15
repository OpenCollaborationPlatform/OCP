package utils

import (
	"fmt"
	"runtime"
	"strings"
)

type OCPErrorClass string

var Internal = OCPErrorClass("internal")       //Fatal internal errors
var Connection = OCPErrorClass("connection")   //Problems with any connection to the outside world
var Application = OCPErrorClass("application") //Problems that occure based on current application state
var Type = OCPErrorClass("type")

//the default error used for all OCP related errors
type OCPError interface {
	error

	Class() OCPErrorClass
	Source() string
	Reason() string
	Arguments() []interface{}
	Stack() []string
	ErrorType() string

	AddToStack(string)
}

func NewError(class OCPErrorClass, source, reason string, args ...interface{}) *Error {

	reason = strings.Replace(reason, " ", "_", -1)
	return &Error{class, source, reason, args, make([]string, 0)}
}

type Error struct {
	class  OCPErrorClass
	source string
	reason string
	args   []interface{}
	stack  []string
}

func (self *Error) Class() OCPErrorClass {
	return self.class
}

func (self *Error) Source() string {
	return self.source
}

func (self *Error) Reason() string {
	return self.reason
}

func (self *Error) Arguments() []interface{} {
	return self.args
}

func (self *Error) Stack() []string {
	return self.stack
}

func (self *Error) AddToStack(val string) {
	self.stack = append(self.stack, val)
}

func (self *Error) ErrorType() string {
	return fmt.Sprintf("ocp.error.%v.%v.%v", string(self.class), self.source, self.reason)
}

func (self *Error) Error() string {

	str := self.ErrorType()
	if len(self.stack) != 0 {
		str += ": " + self.stack[0]
	}

	return str
}

func StackError(err error, args ...interface{}) error {

	//Get the function name in which the error occured
	pc := make([]uintptr, 1)
	n := runtime.Callers(2, pc)
	pc = pc[:n]
	frames := runtime.CallersFrames(pc)
	frame, _ := frames.Next()

	//build the message
	var msg string
	if len(args) > 1 {
		msg = fmt.Sprintf(args[0].(string), args[1:]...)
	} else if len(args) == 1 {
		msg = args[0].(string)
	}
	msg = fmt.Sprintf("%v (Line %v): %v", frame.Function, frame.Line, msg)

	if ocperr, ok := err.(OCPError); ok {
		ocperr.AddToStack(msg)
		err = ocperr

	} else {
		ocperr := NewError(Internal, "library", "failure")
		ocperr.AddToStack(err.Error())
		ocperr.AddToStack(msg)
		err = ocperr
	}

	return err
}

func StackOnError(err error, args ...interface{}) error {

	if err != nil {
		return StackError(err, args...)
	}
	return nil
}
