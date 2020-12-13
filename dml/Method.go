package dml

import (
	"fmt"
	"reflect"

	"github.com/dop251/goja"
)

type Method interface {
	Call(args ...interface{}) (interface{}, error)
	CallBoolReturn(args ...interface{}) (bool, error)
	IsConst() bool
}

func NewMethod(fnc interface{}, constant bool) (Method, error) {

	jsfnc, isJs := fnc.(func(goja.FunctionCall) goja.Value)
	if isJs {
		return &jsMethod{fnc: jsfnc, constant: constant}, nil
	}

	value := reflect.ValueOf(fnc)
	if value.Type().Kind() != reflect.Func {
		return nil, fmt.Errorf("Expected function, not %s", value.Type().String())
	}

	first := value.Type().In(0)
	if first != reflect.TypeOf(Identifier{}) {
		return nil, fmt.Errorf("First Argument of method needs to be Identifier, not %s", first.String())
	}

	return &method{value, constant}, nil
}

func MustNewMethod(fnc interface{}, constant bool) Method {
	m, err := NewMethod(fnc, constant)
	if err != nil {
		panic(err)
	}
	return m
}

type method struct {
	fnc      reflect.Value
	constant bool
}

func (self *method) Call(args ...interface{}) (interface{}, error) {

	if len(args) == 0 {
		return nil, fmt.Errorf("Identifier needs to be provided for function call")
	}

	if _, ok := args[0].(Identifier); !ok {
		return nil, fmt.Errorf("First argument needs to be identifier")
	}

	rfargs := make([]reflect.Value, len(args))
	for i, arg := range args {
		rfargs[i] = reflect.ValueOf(arg)
	}
	res := self.fnc.Call(rfargs)

	if len(res) == 0 {
		return nil, nil

	} else if len(res) == 1 {
		err, ok := res[0].Interface().(error)
		if ok {
			return nil, err
		}
		return res[0].Interface(), nil

	} else if len(res) == 2 {

		//the second one must be error. If set it is returned instead of the
		//the value which is the first res
		if !res[1].IsNil() {
			err, ok := res[1].Interface().(error)
			if !ok {
				return nil, fmt.Errorf("Second return type of function must be error, not %T", res[1].Interface())
			}
			return nil, err
		}
		return res[0].Interface(), nil
	}

	return nil, fmt.Errorf("Function returns too many results: not supported")
}

func (self *method) CallBoolReturn(args ...interface{}) (bool, error) {

	result, err := self.Call(args...)
	if err != nil {
		return false, err
	}
	boolean, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("Return value must be bool")
	}
	return boolean, nil
}

func (self *method) IsConst() bool {
	return self.constant
}

//special method type that handles js functions
type jsMethod struct {
	fnc      func(goja.FunctionCall) goja.Value
	rntm     *Runtime
	jsobj    *goja.Object
	constant bool
}

func (self *jsMethod) Call(args ...interface{}) (result interface{}, err error) {

	//goja panics as form of error reporting...
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
			result = nil
		}
	}()

	//build the function call argument
	jsargs := make([]goja.Value, len(args))
	for i, arg := range args {
		jsargs[i] = self.rntm.jsvm.ToValue(arg)
	}

	err = nil
	res := self.fnc(goja.FunctionCall{Arguments: jsargs, This: self.jsobj})

	result = extractValue(res, self.rntm)
	return
}

func (self *jsMethod) CallBoolReturn(args ...interface{}) (bool, error) {

	result, err := self.Call(args...)
	if err != nil {
		return false, err
	}
	boolean, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("Return value must be bool")
	}
	return boolean, nil
}

func (self *jsMethod) IsConst() bool {
	return self.constant
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
			jsmethod.rntm = rntm
			obj.Set(name, jsmethod.fnc)

		} else {
			//if not js we need to wrapp
			wrapped := func(jsargs goja.FunctionCall) goja.Value {

				//js args to go args
				args := extractValues(jsargs.Arguments, rntm)

				//call the function
				res, err := thisMethod.Call(args...)

				//check if we have a error and if it is not nil we panic for goja
				if err != nil {
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
