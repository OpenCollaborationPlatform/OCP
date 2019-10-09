package dml

import (
	"fmt"
	"reflect"

	"github.com/dop251/goja"
)

type Method interface {
	Call(args ...interface{}) interface{}
	CallBoolReturn(args ...interface{}) (bool, error)
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

	} else if len(res) == 1 {
		return res[0].Interface()

	} else if len(res) == 2 {

		//the second one must be error. If set it is returned instead of the
		//the value which is the first res
		if !res[1].IsNil() {
			err, ok := res[1].Interface().(error)
			if !ok {
				return fmt.Errorf("Second return type of function must be error, not %T", res[1].Interface())
			}
			if err != nil {
				return err
			}
		}
		return res[0].Interface()
	}

	return fmt.Errorf("Function returns too many results: not supported")
}

func (self *method) CallBoolReturn(args ...interface{}) (bool, error) {

	result := self.Call(args...)
	boolean, ok := result.(bool)
	if !ok {
		return true, fmt.Errorf("Return value must be bool")
	}
	return boolean, nil
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

func (self *jsMethod) CallBoolReturn(args ...interface{}) (bool, error) {

	result := self.Call(args...)
	boolean, ok := result.(bool)
	if !ok {
		return true, fmt.Errorf("Return value must be bool")
	}
	return boolean, nil
}

type MethodHandler interface {
	AddMethod(name string, method Method)
	HasMethod(name string) bool
	GetMethod(name string) Method
	Methods() []string

	SetupJSMethods(vm *Runtime, obj *goja.Object) error
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

	self.methods[name] = method
}

func (self *methodHandler) GetMethod(name string) Method {
	return self.methods[name]
}

func (self *methodHandler) Methods() []string {

	meths := make([]string, 0)
	for name, _ := range self.methods {
		meths = append(meths, name)
	}

	return meths
}

func (self *methodHandler) SetupJSMethods(rntm *Runtime, obj *goja.Object) error {

	keys := obj.Keys()
	for name, method := range self.methods {

		//check if method is already set up
		cont := false
		for _, key := range keys {
			if key == name {
				cont = true
				break
			}
		}
		if cont {
			continue
		}

		thisMethod := method

		jsmethod, isJS := thisMethod.(*jsMethod)
		if isJS {
			//we only need to setup the runtime and object
			jsmethod.jsobj = obj
			jsmethod.jsvm = rntm.jsvm
			obj.Set(name, jsmethod.fnc)

		} else {
			//if not js we need to wrapp
			wrapped := func(jsargs goja.FunctionCall) goja.Value {

				//js args to go args
				args := extractArgs(jsargs.Arguments, rntm)

				//call the function
				res := thisMethod.Call(args...)

				//check if we have a error and if it is not nil to panic for goja
				err, ok := res.(error)
				if ok && err != nil {
					panic(rntm.jsvm.ToValue(err.Error()))
				}

				//object has special return value
				retobj, ok := res.(Object)
				if ok {
					return retobj.GetJSObject()
				}

				//go return values to js return values
				return rntm.jsvm.ToValue(res)
			}
			jsmethod := rntm.jsvm.ToValue(wrapped)
			obj.Set(name, jsmethod)
		}
	}

	return nil
}
