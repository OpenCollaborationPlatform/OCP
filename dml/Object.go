package dml

import (
	"CollaborationNode/datastores"

	"github.com/dop251/goja"
)

type Object interface {
	PropertyHandler
	EventHandler
	MethodHandler
	JSObject

	GetDataStore() datastore.Store

	//Object functions
	Id() string

	AddChild(obj Object)
	GetChildren() []Object
	GetChildById(id string) Object
	SetParent(parent Object)
	GetParent() Object
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
	children  []Object
	parent    Object
	id        string

	jsobj *goja.Object
}

func (self *Group) Id() string {
	return self.id
}

func (self *Group) AddChild(obj Object) {

	self.children = append(self.children, obj)
}

func (self *Group) GetChildren() []Object {
	return self.children
}

func (self *Group) GetChildById(id string) Object {
	return nil
}

func (self *Group) SetParent(parent Object) {
	self.parent = parent
}

func (self *Group) GetParent() Object {
	return self.parent
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
		make([]Object, 0),
		nil,
		name,
		jsobj,
	}

	id, err := NewProperty("id", String, grp.GetDataStore(), vm)
	if err != nil {
		return nil
	}
	grp.AddProperty("id", id)

	return &grp
}
