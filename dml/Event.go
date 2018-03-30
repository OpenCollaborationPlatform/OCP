package dml

import (
	"fmt"

	"github.com/dop251/goja"
)

type Event interface {
	Emit(args ...interface{}) error
	RegisterCallback(cb func(...interface{})) error
}

func NewEvent(args ...PropertyType) Event {
	return &event{args, make([]func(...interface{}), 0)}
}

type event struct {
	parameterTypes []PropertyType
	callbacks      []func(...interface{})
}

func (self *event) Emit(args ...interface{}) error {

	//check if all required types are given
	if len(args) != len(self.parameterTypes) {
		return fmt.Errorf("No enough types provided, expected %i, received %i", len(self.parameterTypes), len(args))
	}

	//now call all registered functions
	for _, fnc := range self.callbacks {
		fnc(args...)
	}

	return nil
}

func (self *event) RegisterCallback(cb func(...interface{})) error {

	self.callbacks = append(self.callbacks, cb)
	return nil
}

type EventHandler interface {
	HasEvent(name string) bool
	AddEvent(name string, evt Event) error
	GetEvent(name string) Event

	SetupJSEvents(vm *goja.Runtime, obj *goja.Object) error
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

func (self *eventHandler) SetupJSEvents(vm *goja.Runtime, obj *goja.Object) error {

	//a event is a object within the object, which allows to add callbacks, emit etc...
	for name, _ := range self.events {

		evtName := name //to not use references in the created anym functions
		evtObj := vm.NewObject()

		//add the functions to it
		fnc := vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {
			err := self.GetEvent(evtName).Emit(extractArgs(call.Arguments)...)
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
			err := self.GetEvent(evtName).RegisterCallback(func(args ...interface{}) {
				jsArgs := make([]goja.Value, len(args))
				for i, arg := range args {
					jsArgs[i] = vm.ToValue(arg)
				}
				jsFnc(goja.FunctionCall{This: evtObj, Arguments: jsArgs})
			})
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return
		})
		evtObj.Set("RegisterCallback", fnc)

		err := obj.Set(evtName, evtObj)
		if err != nil {
			return err
		}
	}
	return nil
}
