package dml

import (
	"fmt"

	"github.com/dop251/goja"
)

type EventCallback func(...interface{})

type Event interface {
	JSObject

	Emit(args ...interface{}) error
	RegisterCallback(cb EventCallback) error
	RegisterJSCallback(cb func(goja.FunctionCall) goja.Value) error
}

func NewEvent(vm *goja.Runtime, args ...DataType) Event {

	evt := &event{parameterTypes: args, callbacks: make([]EventCallback, 0)}

	//now the js object
	evtObj := vm.NewObject()

	fnc := vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {
		err := evt.Emit(extractArgs(call.Arguments)...)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return
	})
	evtObj.Set("Emit", fnc)

	fnc = vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {

		//we get a normal goja function. this must be bridged to our callback format
		jsFnc, ok := call.Argument(0).Export().(func(goja.FunctionCall) goja.Value)
		if !ok {
			panic(vm.ToValue("Argument is not a function!"))
		}
		err := evt.RegisterJSCallback(jsFnc)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return
	})
	evtObj.Set("RegisterCallback", fnc)

	evt.jsobj = evtObj
	evt.jsvm = vm

	return evt
}

type event struct {
	parameterTypes []DataType
	callbacks      []EventCallback

	jsvm  *goja.Runtime
	jsobj *goja.Object
}

func (self *event) Emit(args ...interface{}) error {

	//check if all required types are given
	if len(args) != len(self.parameterTypes) {
		return fmt.Errorf("No enough types provided, expected %i, received %i", len(self.parameterTypes), len(args))
	}
	for i, pt := range self.parameterTypes {
		err := mustBeType(pt, args[i])
		if err != nil {
			return err
		}
	}

	//now call all registered functions
	for _, fnc := range self.callbacks {
		fnc(args...)
	}

	return nil
}

func (self *event) RegisterCallback(cb EventCallback) error {

	self.callbacks = append(self.callbacks, cb)
	return nil
}

func (self *event) RegisterJSCallback(cb func(goja.FunctionCall) goja.Value) error {

	return self.RegisterCallback(func(args ...interface{}) {
		jsArgs := make([]goja.Value, len(args))
		for i, arg := range args {
			jsArgs[i] = self.jsvm.ToValue(arg)
		}
		cb(goja.FunctionCall{This: self.jsobj, Arguments: jsArgs})
	})
}

func (self *event) GetJSObject() *goja.Object {
	return self.jsobj
}

type EventHandler interface {
	HasEvent(name string) bool
	AddEvent(name string, evt Event) error
	GetEvent(name string) Event
}

func NewEventHandler() eventHandler {
	return eventHandler{make(map[string]Event, 0)}
}

//unifies handling of multiple events
type eventHandler struct {
	events map[string]Event
}

func (self *eventHandler) HasEvent(name string) bool {

	_, ok := self.events[name]
	return ok
}

func (self *eventHandler) AddEvent(name string, evt Event) error {

	if self.HasEvent(name) {
		return fmt.Errorf("Event already exists")
	}
	self.events[name] = evt
	return nil
}

func (self *eventHandler) GetEvent(name string) Event {

	return self.events[name]
}

//little helper to extract the call arguments
func extractArgs(values []goja.Value) []interface{} {

	res := make([]interface{}, len(values))
	for i, val := range values {
		res[i] = val.Export()
	}
	return res
}
