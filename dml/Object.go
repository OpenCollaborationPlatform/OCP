package dml

import (
	"CollaborationNode/datastores"
	"fmt"

	"github.com/dop251/goja"
)

type Object interface {
	PropertyHandler
	EventHandler
	MethodHandler
	JSObject

	GetDataStore() datastore.Store

	//Object functions
	AddChild(name string, obj Object) error
}

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions. It has no other special
//functionality. It is intended as dml grouping object as well as base object for
//other types
type Group struct {
	propertyHandler
	eventHandler
	methodHandler

	store     datastore.Store
	functions map[string]interface{}
	children  map[string]Object

	jsobj *goja.Object
}

func (self *Group) AddChild(name string, obj Object) error {

	_, ok := self.children[name]
	if ok {
		return fmt.Errorf("Child already exist")
	}
	self.children[name] = obj
	return nil
}

func (self *Group) GetDataStore() datastore.Store {
	return self.store
}

func (self *Group) GetJSObject() *goja.Object {
	return self.jsobj
}

func NewGroup(ds *datastore.Datastore, name string, vm *goja.Runtime) Object {

	jsobj := vm.NewObject()

	grp := Group{
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		ds.GetOrCreateStore(datastore.KeyValue, name),
		make(map[string]interface{}, 0),
		make(map[string]Object, 0),
		jsobj,
	}

	id, err := NewProperty("id", String, grp.GetDataStore(), vm)
	if err != nil {
		return nil
	}
	grp.AddProperty("id", id)

	return &grp
}
