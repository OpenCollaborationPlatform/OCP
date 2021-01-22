package dml

import (
	"fmt"
	"reflect"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/dop251/goja"
)

type Method interface {
	Call(id Identifier, args ...interface{}) (interface{}, error)
	CallBoolReturn(id Identifier, args ...interface{}) (bool, error)
	IsConst() bool
}

func NewMethod(fnc interface{}, constant bool) (Method, error) {

	jsfnc, isJs := fnc.(func(goja.FunctionCall) goja.Value)
	if isJs {
		return &jsMethod{fnc: jsfnc, constant: constant}, nil
	}

	value := reflect.ValueOf(fnc)
	if value.Type().Kind() != reflect.Func {
		return nil, newInternalError(Error_Fatal, "Expected function, not %s", value.Type().String())
	}

	first := value.Type().In(0)
	if first != reflect.TypeOf(Identifier{}) {
		return nil, newInternalError(Error_Fatal, "First Argument of method needs to be Identifier, not %s", first.String())
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

func (self *method) Call(id Identifier, args ...interface{}) (interface{}, error) {

	rfargs := make([]reflect.Value, len(args)+1)
	for i, arg := range args {
		rfargs[i+1] = reflect.ValueOf(arg)
	}
	rfargs[0] = reflect.ValueOf(id)
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
				return nil, newInternalError(Error_Fatal, "Second return type of function must be error, not %T", res[1].Interface())
			}
			return nil, err
		}
		return res[0].Interface(), nil
	}

	return nil, newInternalError(Error_Fatal, "Function returns too many results: not supported")
}

func (self *method) CallBoolReturn(id Identifier, args ...interface{}) (bool, error) {

	result, err := self.Call(id, args...)
	if err != nil {
		return false, err
	}
	boolean, ok := result.(bool)
	if !ok {
		return false, newInternalError(Error_Fatal, "Return value must be bool")
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
	jsProto  *goja.Object
	constant bool
	name     string
}

func (self *jsMethod) Call(id Identifier, args ...interface{}) (result interface{}, err error) {

	//goja panics as form of error reporting...
	defer func() {
		if e := recover(); e != nil {
			if er, ok := e.(error); ok {
				ocpErr := wrapJSError(er, self.rntm.jsvm)
				ocpErr.AddToStack(fmt.Sprintf("JavaScript function %v failed in %v", self.name, id))
				err = ocpErr
			} else {
				err = newInternalError(Error_Fatal, fmt.Sprintf("Unknown error occured during JavaScript function %v in %v: %v", self.name, id, e), e)
			}
		}
	}()

	//build the function call argument
	jsargs := make([]goja.Value, len(args))
	for i, arg := range args {
		jsargs[i] = self.rntm.jsvm.ToValue(arg)
	}

	//build the object to call on
	obj := self.rntm.jsvm.CreateObject(self.jsProto)
	obj.Set("identifier", self.rntm.jsvm.ToValue(id))

	//call and return
	err = nil
	res := self.fnc(goja.FunctionCall{Arguments: jsargs, This: obj})
	result = extractValue(res, self.rntm)
	return
}

func (self *jsMethod) CallBoolReturn(id Identifier, args ...interface{}) (bool, error) {

	result, err := self.Call(id, args...)
	if err != nil {
		return false, err
	}
	boolean, ok := result.(bool)
	if !ok {
		return false, newInternalError(Error_Fatal, "Return value must be bool")
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
			jsmethod.jsProto = obj
			jsmethod.rntm = rntm
			jsmethod.name = name
			obj.Set(name, jsmethod.fnc)

		} else {
			//if not js we need to wrapp
			wrapped := func(jsargs goja.FunctionCall) goja.Value {

				//js args to go args
				args := extractValues(jsargs.Arguments, rntm)

				//get the identifier from whom we are called
				id := jsargs.This.ToObject(rntm.jsvm).Get("identifier").Export()
				ident, ok := id.(Identifier)
				if !ok || !ident.Valid() {
					panic(rntm.jsvm.ToValue(newInternalError(Error_Fatal, "Called object does not have identifier setup correctly")))
				}

				//call the function
				res, err := thisMethod.Call(ident, args...)

				//check if we have a error and if it is not nil we panic for goja
				if err != nil {
					callname := name
					panic(rntm.jsvm.ToValue(utils.StackError(err, "%v: Called from JS, returned with error", callname)))
				}

				//Identifier has special return value (need to build the correct js obj)
				if ident, ok = res.(Identifier); ok {
					set, err := rntm.getObjectSet(ident)
					if err != nil {
						panic(rntm.jsvm.ToValue(newInternalError(Error_Fatal, "Object is returned, but unable to build JS representation")))
					}
					return rntm.jsvm.ToValue(set.obj.GetJSObject(set.id))
				}

				//dmlSet has special return value
				if set, ok := res.(dmlSet); ok {
					return rntm.jsvm.ToValue(set.obj.GetJSObject(set.id))
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
