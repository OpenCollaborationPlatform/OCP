package dml

import (
	"errors"
	"fmt"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/dop251/goja"
)

type EventCallback func(Identifier, ...interface{}) error

type Event interface {
	JSObject
	MethodHandler

	Emit(Identifier, ...interface{}) error
	Enabled(Identifier) (bool, error)
	Enable(Identifier) error
	Disable(Identifier) error

	RegisterCallback(EventCallback) error
	RegisterJSCallback(func(goja.FunctionCall) goja.Value) error
}

func NewEvent(jsParentProto *goja.Object, rntm *Runtime) Event {

	evt := &event{
		methodHandler: NewMethodHandler(),
		callbacks:     make([]EventCallback, 0),
	}

	//now the js object
	evtObj := rntm.jsvm.NewObject()

	emitMethod, _ := NewMethod(evt.Emit, false)
	evt.AddMethod("Emit", emitMethod)
	enabledMethod, _ := NewMethod(evt.Enabled, true)
	evt.AddMethod("Enabled", enabledMethod)
	enableMethod, _ := NewMethod(evt.Enable, false)
	evt.AddMethod("Enable", enableMethod)
	disableMethod, _ := NewMethod(evt.Disable, false)
	evt.AddMethod("Disable", disableMethod)

	evt.jsProto = evtObj
	evt.rntm = rntm
	evt.jsParentProto = jsParentProto
	evt.SetupJSMethods(rntm, evtObj)

	return evt
}

type event struct {
	methodHandler

	callbacks []EventCallback
	enabled   bool

	rntm          *Runtime
	jsProto       *goja.Object
	jsParentProto *goja.Object //needs to be passed as "this" to event functions
}

func (self *event) Emit(id Identifier, args ...interface{}) error {

	enabled, err := self.Enabled(id)
	if err != nil {
		return utils.StackError(err, "Unable to emit as status query failed")
	}

	if !enabled {
		return nil
	}

	//call all registered functions
	for _, fnc := range self.callbacks {
		err = fnc(id, args...)
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

func (self *event) Enabled(id Identifier) (bool, error) {

	value, err := valueVersionedFromStore(self.rntm.datastore, id, []byte("__enabled"))
	if err != nil {
		return false, utils.StackError(err, "Unable to access event status from DB")
	}

	enabled, err := value.Read()
	if err != nil {
		return false, utils.StackError(err, "Unable to query event status from DB")
	}

	result, ok := enabled.(bool)
	if !ok {
		return false, fmt.Errorf("Event status wrongly stored in DB")
	}

	return result, nil
}

func (self *event) Enable(id Identifier) error {

	value, err := valueVersionedFromStore(self.rntm.datastore, id, []byte("__enabled"))
	if err != nil {
		return utils.StackError(err, "Unable to access event status from DB")
	}

	err = value.Write(true)
	if err != nil {
		return utils.StackError(err, "Unable so write event status")
	}
	return nil
}

func (self *event) Disable(id Identifier) error {

	value, err := valueVersionedFromStore(self.rntm.datastore, id, []byte("__enabled"))
	if err != nil {
		return utils.StackError(err, "Unable to access event status from DB")
	}

	err = value.Write(false)
	if err != nil {
		return utils.StackError(err, "Unable so write event status")
	}
	return nil
}

func (self *event) RegisterJSCallback(cb func(goja.FunctionCall) goja.Value) error {

	return self.RegisterCallback(func(id Identifier, args ...interface{}) (err error) {

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

		//the arguments for the function call
		jsArgs := make([]goja.Value, len(args))
		for i, arg := range args {
			jsArgs[i] = self.rntm.jsvm.ToValue(arg)
		}

		//build the object to call on
		obj := self.rntm.jsvm.CreateObject(self.jsParentProto)
		obj.Set("identifier", self.rntm.jsvm.ToValue(id))

		cb(goja.FunctionCall{This: obj, Arguments: jsArgs})
		return
	})
}

func (self *event) GetJSObject(id Identifier) *goja.Object {
	obj := self.rntm.jsvm.CreateObject(self.jsProto)
	obj.Set("identifier", self.rntm.jsvm.ToValue(id))
	return obj
}

func (self *event) GetJSPrototype() *goja.Object {
	return self.jsProto
}

func (self *event) GetJSRuntime() *goja.Runtime {
	return self.rntm.jsvm
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
	/*
		for key, evt := range self.events {
			jsobj.Set(key, evt.GetJSObject())
		}*/
	//TODO: event need to be handled as property with setter/getter to allow
	//		access to the identifier and to create the correct object than
	return nil
}
