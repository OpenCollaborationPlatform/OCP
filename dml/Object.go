package dml

import "fmt"

type Object interface {
	PropertyHandler
	EventHandler

	AddChild(name string, obj Object) error
}

type object struct {
	propertyHandler
	eventHandler

	functions map[string]interface{}
	children  map[string]Object
}

func (self *object) AddChild(name string, obj Object) error {

	_, ok := self.children[name]
	if ok {
		return fmt.Errorf("Child already exist")
	}
	self.children[name] = obj
	return nil
}
