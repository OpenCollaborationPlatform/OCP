package dml

import (
	"errors"
	"fmt"

	"github.com/dop251/goja"
)

type EventCallback func(...interface{}) error

type Event interface {
	JSObject
	MethodHandler

	Emit(args ...interface{}) error
	RegisterCallback(cb EventCallback) error
	Enable() error
	Disable() error
	RegisterJSCallback(cb func(goja.FunctionCall) goja.Value) error
}

func NewEvent(jsparent *goja.Object, rntm *Runtime) Event {

	evt := &event{
		methodHandler:	NewMethodHandler(),
		callbacks:    	make([]EventCallback, 0),
		enabled: 		true}

	//now the js object
	evtObj := rntm.jsvm.NewObject()

	emitMethod, _ := NewMethod(evt.Emit, false)
	evt.AddMethod("Emit", emitMethod)
	registerMethod, _ := NewMethod(evt.RegisterJSCallback, false)
	evt.AddMethod("RegisterCallback", registerMethod)
	enableMethod, _ := NewMethod(evt.Enable, false)
	evt.AddMethod("Enable", enableMethod)
	disableMethod, _ := NewMethod(evt.Disable, false)
	evt.AddMethod("Disable", disableMethod)

	evt.jsobj = evtObj
	evt.jsvm = rntm.jsvm
	evt.jsparent = jsparent
	evt.SetupJSMethods(rntm, evtObj)

	return evt
}

type event struct {
	methodHandler

	callbacks 	[]EventCallback
	enabled 		bool

	jsvm     *goja.Runtime
	jsobj    *goja.Object
	jsparent *goja.Object //needed to be passed as "this" to event functions
}

func (self *event) Emit(args ...interface{}) error {

	if !self.enabled {
		return nil
	}

	//call all registered functions
	var err error
	for _, fnc := range self.callbacks {
		err = fnc(args...)
		if err != nil {
			break
		}
	}

	return err
}

func (self *event) RegisterCallback(cb EventCallback) error {

	self.callbacks = append(self.callbacks, cb)
	return nil
}

func (self *event) Enable() error {

	self.enabled = true
	return nil
}

func (self *event) Disable() error {

	self.enabled = false
	return nil
}

func (self *event) RegisterJSCallback(cb func(goja.FunctionCall) goja.Value) error {

	return self.RegisterCallback(func(args ...interface{}) (err error) {

		defer func() {
			// recover from panic if one occured. Set err to nil otherwise.
			errval := recover()
			if errval != nil {
				str := "Error during event processing"
				if jserr, ok := errval.(*goja.Exception); ok {
					str = jserr.Value().Export().(string)

				} else if jsval, ok := errval.(goja.Value); ok {
					str = jsval.ToString().String()

				} else if strval, ok := errval.(string); ok {
					str = strval
				}

				err = errors.New(str)
			}
		}()

		jsArgs := make([]goja.Value, len(args))
		for i, arg := range args {
			jsArgs[i] = self.jsvm.ToValue(arg)
		}
		cb(goja.FunctionCall{This: self.jsparent, Arguments: jsArgs})

		return err
	})
}

func (self *event) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *event) GetJSRuntime() *goja.Runtime {
	return self.jsvm
}

type EventHandler interface {
	HasEvent(name string) bool
	AddEvent(name string, evt Event) error
	GetEvent(name string) Event
	Events() []string
	SetupJSEvents(*goja.Object) error
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

func (self *eventHandler) Events() []string {

	res := make([]string, len(self.events))
	cnt := 0
	for key, _ := range self.events {
		res[cnt] = key
		cnt = cnt + 1
	}
	return res
}

func (self *eventHandler) SetupJSEvents(jsobj *goja.Object) error {

	for key, evt := range self.events {
		jsobj.Set(key, evt.GetJSObject())
	}
	return nil
}

//little helper to extract the call arguments
func extractArgs(values []goja.Value, rntm *Runtime) []interface{} {

	res := make([]interface{}, len(values))
	for i, val := range values {
		//check if it is a goja object
		obj, _ := objectFromJSValue(val, rntm)
		if obj != nil {
			res[i] = obj
			continue
		}

		//no, normal values!
		res[i] = val.Export()
	}
	return res
}
