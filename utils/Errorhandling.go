package utils

import (
	"fmt"
	"runtime"
	"strings"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
)

type OCPErrorClass string

var Internal = OCPErrorClass("internal")       //Fatal internal errors
var Connection = OCPErrorClass("connection")   //Problems with any connection to the outside world
var Application = OCPErrorClass("application") //Problems that occure based on current application state
var Type = OCPErrorClass("type")               //Type errors
var User = OCPErrorClass("user")               //Problems that occure based on user input

//the default error used for all OCP related errors
type OCPError interface {
	error

	Class() OCPErrorClass
	Source() string
	Reason() string
	Message() string
	Arguments() []interface{}
	Stack() []string
	ErrorType() string
	ErrorWithStacktrace() string

	AddToStack(string)
}

/* Creates a new Error from the following informations:
 * - class: one of the predefined OCPErrorClass types
 * - source: custom identifier specifying the source of the error (e.g. datastore, host etc.)
 * - reason: custom identifier specifying the general reason. This is not intended as description, but a kind of grouping for reasons, e.g. "key_not_existant"
 * - message: detailed information of what has gone wrong
 * - args: Arguments further detail the error with runtime information provided in log style doubles, e.g. "MyNAme", name, "MyId", id
 */
func NewError(class OCPErrorClass, source, reason, message string, args ...interface{}) *Error {

	reason = strings.Replace(reason, " ", "_", -1)

	//build the origin (Assuming NewError is called in second row
	pc := make([]uintptr, 1)
	n := runtime.Callers(3, pc)
	pc = pc[:n]
	frames := runtime.CallersFrames(pc)
	frame, _ := frames.Next()
	origin := fmt.Sprintf("%v (Line %v)", frame.Function, frame.Line)

	return &Error{class, source, reason, message, origin, args, make([]string, 0)}
}

type Error struct {
	class   OCPErrorClass
	source  string
	reason  string
	message string
	origin  string
	args    []interface{}
	stack   []string
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

func (self *Error) Message() string {
	return self.message
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

//Print the error type only in the form of ocp.error.class.source.reason
func (self *Error) ErrorType() string {
	return fmt.Sprintf("ocp.error.%v.%v.%v", string(self.class), self.source, self.reason)
}

//Prints the error in the for of ErrorType: message (arguments)
func (self *Error) Error() string {

	str := self.ErrorType() + ": " + self.message

	//args are important information
	if len(self.args) > 0 {
		str += " ("
		for i := 0; i < int(len(self.args)/2); i++ {
			str += fmt.Sprintf("%v: %v, ", self.args[i*2], self.args[i*2+1])
		}
		str += ")"
	}

	return str
}

//Prints the error in the form of:
//ErrorType
//origin: message (Arguments)
//Stacktraces...
func (self *Error) ErrorWithStacktrace() string {

	str := self.ErrorType() + "\n" + self.origin + ": " + self.message
	//args are important information
	if len(self.args) > 0 {
		str += " ("
		for i := 0; i < int(len(self.args)/2); i++ {
			str += fmt.Sprintf("%v: %v, ", self.args[i*2], self.args[i*2+1])
		}
		str += ")"
	}
	str += "\n"
	for _, value := range self.stack {
		str += value + "\n"
	}

	return str
}

func PrintWithStacktrace(err error) {

	if err == nil {
		return
	}

	if ocperr, ok := err.(OCPError); ok {
		fmt.Println(ocperr.ErrorWithStacktrace())
	}
}

func StackError(err error, args ...interface{}) error {

	if err != nil {

		//Get the function name in which the error occured
		pc := make([]uintptr, 1)
		n := runtime.Callers(2, pc)
		pc = pc[:n]
		frames := runtime.CallersFrames(pc)
		frame, _ := frames.Next()

		//build the message
		var msg string
		if len(args) > 1 {

			if strings.Contains(args[0].(string), "%") {
				//build msg printf style
				msg = fmt.Sprintf(args[0].(string), args[1:]...)

			} else {
				//build message log style
				additional := args[1:]
				if len(additional)%2 != 0 {
					additional = append(additional, "MISSING ARGUMENT")
				}
				msg = args[0].(string) + " ("
				for i := 0; i < len(additional); i = i + 2 {
					msg = msg + fmt.Sprintf("%v: %v, ", additional[i], additional[i+1])
				}
				msg = msg + ")"
			}
		} else if len(args) == 1 {
			msg = args[0].(string)
		}
		msg = fmt.Sprintf("%v (Line %v): %v", frame.Function, frame.Line, msg)

		if ocperr, ok := err.(OCPError); ok {
			ocperr.AddToStack(msg)
			err = ocperr

		} else {
			ocperr := NewError(Internal, "library", "failure", err.Error())
			ocperr.AddToStack(msg)
			err = ocperr
		}

		return err
	}
	return nil
}

func ErrorToWampResult(err error) nxclient.InvokeResult {

	if err == nil {
		return nxclient.InvokeResult{}
	}

	if ocperr, ok := err.(OCPError); ok {
		if len(ocperr.Stack()) > 0 {
			return nxclient.InvokeResult{Args: wamp.List{ocperr.Stack()[0]}, Err: wamp.URI(ocperr.ErrorType())}
		} else {
			return nxclient.InvokeResult{Err: wamp.URI(ocperr.ErrorType())}
		}
	}
	return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error.internal.library.failure")}
}
