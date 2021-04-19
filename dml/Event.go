package dml

import (
	"encoding/gob"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/dop251/goja"
)

func init() {
	gob.Register(new(EventObjectCallback))
}

type EventCallback func(Identifier, ...interface{}) error

type EventObjectCallback struct {
	Id       Identifier
	Function string
}

type EventEmitNotifyer interface {
	EventEmitted(Identifier, string, ...interface{}) error
}

type Event interface {
	JSObject
	MethodHandler

	GetName() string
	SetNotifyer(EventEmitNotifyer)

	Emit(Identifier, ...interface{}) error
	Enabled(Identifier) (bool, error)
	Enable(Identifier) error
	Disable(Identifier) error

	//DB individual, add to a certain Identifier a callback as function to call from annother identifier
	RegisterCallback(Identifier, Identifier, string) error

	//Object based, add callback functions to all identifiers of the object
	//note: only allowed for callbacks in the same object the event lives in. The reason is the
	//passed identifier: it is always the event parent object that emits the event. Hence if a function
	//to annother object is passed the identifier may have different type than the object called
	//Therefore any JS function registered is handled as it would be a object function.
	RegisterObjectJSCallback(func(goja.FunctionCall) goja.Value) error
	RegisterObjectGoCallback(EventCallback) error
}

func NewEvent(name string, owner Object) Event {

	evt := &event{
		methodHandler: NewMethodHandler(),
		callbacks:     make([]EventCallback, 0),
		enabled:       true,
		name:          name,
		owner:         owner,
		notifyer:      nil,
	}

	//now the js object
	evtObj := owner.GetJSRuntime().NewObject()

	emitMethod, _ := NewIdMethod(evt.Emit, false)
	evt.AddMethod("Emit", emitMethod)
	enabledMethod, _ := NewIdMethod(evt.Enabled, true)
	evt.AddMethod("Enabled", enabledMethod)
	enableMethod, _ := NewIdMethod(evt.Enable, false)
	evt.AddMethod("Enable", enableMethod)
	disableMethod, _ := NewIdMethod(evt.Disable, false)
	evt.AddMethod("Disable", disableMethod)
	registerMethod, _ := NewIdMethod(evt.RegisterCallback, false)
	evt.AddMethod("RegisterCallback", registerMethod)

	evt.jsProto = evtObj
	evt.SetupJSMethods(owner.GetRuntime(), evtObj)

	return evt
}

type event struct {
	methodHandler

	callbacks []EventCallback
	enabled   bool
	name      string

	owner    Object
	notifyer EventEmitNotifyer
	jsProto  *goja.Object
}

func (self *event) GetName() string {
	return self.name
}

func (self *event) SetNotifyer(een EventEmitNotifyer) {
	self.notifyer = een
}

func (self *event) Emit(id Identifier, args ...interface{}) error {

	enabled, err := self.Enabled(id)
	if err != nil {
		return utils.StackError(err, "Unable to query event status")
	}

	if !enabled {
		return nil
	}

	//all default event callbacks
	for _, cb := range self.callbacks {
		err := cb(id, args...)
		if err != nil {
			return utils.StackError(err, "Go callback failed")
		}
	}

	//call all runtime created callbacks
	var cbs datastore.ListVersioned
	cbs, err = listVersionedFromStore(self.owner.GetRuntime().datastore, id, []byte("__event_"+self.name))
	if err != nil {
		return utils.StackError(err, "Unable to access DB eventcallbacks")
	}
	entries, err := cbs.GetEntries()
	if err != nil {
		return utils.StackError(err, "Unable to access DB for runtime callbacks")
	}
	if len(entries) > 0 {

		for _, entry := range entries {
			var val interface{}
			val, err = entry.Read()
			if err != nil {
				return utils.StackError(err, "Unable to access DB eventcallback")
			}
			cb, ok := val.(*EventObjectCallback)
			if !ok {
				return newInternalError(Error_Fatal, "Event callback stored with wrong type")
			}

			//get the object to call
			var set dmlSet
			set, err = self.owner.GetRuntime().getObjectSet(cb.Id)
			if err != nil {
				return utils.StackError(err, "Unable to get object %v", cb.Id)
			}

			if !set.obj.HasMethod(cb.Function) {
				return newUserError(Error_Key_Not_Available, "Registerd callback %v not available in object %v", cb.Function, cb.Id.Name)
			}
			args = append([]interface{}{set.id}, args...)
			if _, err = set.obj.GetMethod(cb.Function).Call(args...); err != nil {
				return utils.StackError(err, "Function %v failed", cb.Function)
			}
		}
	}

	//inform runtime about event
	if self.notifyer != nil {
		return utils.StackOnError(self.notifyer.EventEmitted(id, self.name, args...), "Object function failed for event emit")
	}
	return nil
}

func (self *event) Enabled(id Identifier) (bool, error) {

	value, err := valueVersionedFromStore(self.owner.GetRuntime().datastore, id, []byte("__enabled"))
	if err != nil {
		return false, err
	}

	enabled, err := value.Read()
	if err != nil {
		return false, utils.StackError(err, "Unable to read from DB")
	}

	result, ok := enabled.(bool)
	if !ok {
		return false, newInternalError(Error_Fatal, "Event status stored as wrong type")
	}

	return result, nil
}

func (self *event) Enable(id Identifier) error {

	value, err := valueVersionedFromStore(self.owner.GetRuntime().datastore, id, []byte("__enabled"))
	if err != nil {
		return err
	}

	err = value.Write(true)
	if err != nil {
		return utils.StackError(err, "Unable so write to DB")
	}
	return nil
}

func (self *event) Disable(id Identifier) error {

	value, err := valueVersionedFromStore(self.owner.GetRuntime().datastore, id, []byte("__enabled"))
	if err != nil {
		return err
	}

	err = value.Write(false)
	if err != nil {
		return utils.StackError(err, "Unable so write to DB")
	}
	return nil
}

//This function allows to register object functions as event callbacks
//note that this registers a callback to a special object, not all objects of the
//type
func (self *event) RegisterCallback(id Identifier, cbID Identifier, function string) error {

	cbs, err := listVersionedFromStore(self.owner.GetRuntime().datastore, id, []byte("__event_"+self.name))
	if err != nil {
		return err
	}
	_, err = cbs.Add(EventObjectCallback{cbID, function})
	if err != nil {
		return utils.StackError(err, "Unable to write to DB")
	}

	return nil
}

//this is to be used for anonymous callbacks. Note that this is only allowed for "static"
//callbacks, not runtime deoendend ones. Hence  use this only for dml event assignents
//note that it registers a callback for all objects of the type,
func (self *event) RegisterObjectJSCallback(cb func(goja.FunctionCall) goja.Value) error {

	callback := func(id Identifier, args ...interface{}) (err error) {

		//goja panics as form of error reporting...
		defer func() {
			if e := recover(); e != nil {
				if er, ok := e.(error); ok {
					ocpErr := wrapJSError(er, self.GetJSRuntime())
					ocpErr.AddToStack(fmt.Sprintf("JavaScript callback for event %v failed in %v", self.name, id))
					err = ocpErr

				} else {
					err = newInternalError(Error_Fatal, fmt.Sprintf("Unknown error occured during JavaScript callback of %v: %v", self.name, e), e)
				}
			}
		}()

		jsArgs := make([]goja.Value, len(args))
		for i, arg := range args {
			jsArgs[i] = self.owner.GetJSRuntime().ToValue(arg)
		}
		jsObj := self.owner.GetJSRuntime().CreateObject(self.owner.GetJSPrototype())
		jsObj.Set("identifier", id)
		cb(goja.FunctionCall{This: jsObj, Arguments: jsArgs})

		return err
	}

	self.callbacks = append(self.callbacks, callback)
	return nil
}

func (self *event) RegisterObjectGoCallback(cb EventCallback) error {

	self.callbacks = append(self.callbacks, cb)
	return nil
}

func (self *event) GetJSObject(id Identifier) *goja.Object {
	obj := self.GetJSRuntime().CreateObject(self.jsProto)
	obj.Set("identifier", self.GetJSRuntime().ToValue(id))
	return obj
}

func (self *event) GetJSPrototype() *goja.Object {
	return self.jsProto
}

func (self *event) GetJSRuntime() *goja.Runtime {
	return self.owner.GetRuntime().jsvm
}

type EventHandler interface {
	HasEvent(name string) bool
	AddEvent(evt Event) error
	GetEvent(name string) Event
	Events() []string
	SetupJSEvents(*goja.Object) error
	SetupEventNotifyer(EventEmitNotifyer)
	InitializeEventDB(Identifier) error
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

func (self *eventHandler) AddEvent(evt Event) error {

	if self.HasEvent(evt.GetName()) {
		return newInternalError(Error_Operation_Invalid, "Event already exists")
	}

	self.events[evt.GetName()] = evt
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

	for _, evt := range self.events {

		evt := evt
		getter := evt.GetJSRuntime().ToValue(func(call goja.FunctionCall) goja.Value {

			id := call.This.ToObject(evt.GetJSRuntime()).Get("identifier").Export()
			identifier, ok := id.(Identifier)
			if !ok {
				panic(evt.GetJSRuntime().ToValue(newInternalError(Error_Fatal, fmt.Sprintf("Called object does not have identifier setup correctly: %v", id))))
			}
			return self.GetEvent(evt.GetName()).GetJSObject(identifier)
		})
		jsobj.DefineAccessorProperty(evt.GetName(), getter, nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	}

	return nil
}

func (self *eventHandler) SetupEventNotifyer(een EventEmitNotifyer) {

	for _, evt := range self.events {
		evt.SetNotifyer(een)
	}
}

func (self *eventHandler) InitializeEventDB(id Identifier) error {

	//the only data in the DB is the enabled flag of the events. Write it to default
	//enabled
	for _, event := range self.events {
		err := event.Enable(id)
		if err != nil {
			return utils.StackError(err, "Unable to initialize event DB")
		}
	}
	return nil
}
