package dml

import (
	"fmt"
	"reflect"

	"github.com/dop251/goja"
)

type Method interface {
	Call(args ...interface{}) interface{}
}

func NewMethod(fnc interface{}) (Method, error) {

	jsfnc, isJs := fnc.(func(goja.FunctionCall) goja.Value)
	if isJs {
		return &jsMethod{fnc: jsfnc}, nil
	}

	value := reflect.ValueOf(fnc)
	if value.Type().Kind() != reflect.Func {
		return nil, fmt.Errorf("Expected function, not %s", value.Type().String())
	}

	return &method{value}, nil
}

func MustNewMethod(fnc interface{}) Method {
	m, err := NewMethod(fnc)
	if err != nil {
		panic(err)
	}
	return m
}

type method struct {
	fnc reflect.Value
}

func (self *method) Call(args ...interface{}) interface{} {

	rfargs := make([]reflect.Value, len(args))
	for i, arg := range args {
		rfargs[i] = reflect.ValueOf(arg)
	}
	res := self.fnc.Call(rfargs)

	if len(res) == 0 {
		return nil
	}

	return res[0].Interface()
}

//special method type that handles js functions
type jsMethod struct {
	fnc   func(goja.FunctionCall) goja.Value
	jsvm  *goja.Runtime
	jsobj *goja.Object
}

func (self *jsMethod) Call(args ...interface{}) interface{} {

	//build the function call argument
	jsargs := make([]goja.Value, len(args))
	for i, arg := range args {
		jsargs[i] = self.jsvm.ToValue(arg)
	}
	res := self.fnc(goja.FunctionCall{Arguments: jsargs, This: self.jsobj})
	return res.Export()
}

type MethodHandler interface {
	AddMethod(name string, method Method)
	HasMethod(name string) bool
	GetMethod(name string) Method

	SetupJSMethods(vm *goja.Runtime, obj *goja.Object) error
}

func NewMethodHandler() methodHandler {
	return methodHandler{make(map[string]Method, 0)}
}

type methodHandler struct {
	methods map[string]Method
}

func (self *methodHandler) HasMethod(name string) bool {
	_, ok := self.methods[name]
	return ok
}

func (self *methodHandler) AddMethod(name string, method Method) {

	if self.HasMethod(name) {
		return
	}
	self.methods[name] = method
}

func (self *methodHandler) GetMethod(name string) Method {
	return self.methods[name]
}

func (self *methodHandler) SetupJSMethods(vm *goja.Runtime, obj *goja.Object) error {

	for name, method := range self.methods {

		thisMethod := method

		jsmethod, isJS := thisMethod.(*jsMethod)
		if isJS {
			//we only need to setup the runtime and object
			jsmethod.jsobj = obj
			jsmethod.jsvm = vm
			obj.Set(name, jsmethod.fnc)

		} else {
			//if not js we need to wrapp
			wrapped := func(jsargs goja.FunctionCall) goja.Value {

				//js args to go args
				args := make([]interface{}, len(jsargs.Arguments))
				for i, jsarg := range jsargs.Arguments {
					args[i] = jsarg.Export()
				}
				//call the function
				res := thisMethod.Call(args...)

				//check if we have a error and if it is not nil to panic for goja
				err, ok := res.(error)
				if ok && err != nil {
					panic(vm.ToValue(err))
				}

				//go return values to js return values
				return vm.ToValue(res)
			}
			jsmethod := vm.ToValue(wrapped)
			obj.Set(name, jsmethod)
		}
	}

	return nil
}
