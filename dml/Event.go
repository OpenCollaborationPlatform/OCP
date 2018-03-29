package dml

import "fmt"

type Event interface {
	Emit(args ...interface{}) error
	RegisterCallback(cb func(...interface{}))
}

func makeEvent(args ...PropertyType) Event {
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

	return nil
}

func (self *event) RegisterCallback(cb func(...interface{})) {

	self.callbacks = append(self.callbacks, cb)
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
