package dml

import (
	"CollaborationNode/datastores"
	"fmt"

	"github.com/dop251/goja"
)

//Interface of an object: All objects, data and behaviour, must be able to handle
//  - Properties
//  - Events
//  - Methods
//Furthermore must both be available in JS, Global by id an in the child hirarchy
type Object interface {
	PropertyHandler
	EventHandler
	MethodHandler
	JSObject

	//Object functions
	Id() identifier
	GetDataStore() datastore.Store

	//Object hirarchy
	AddChild(obj Object)
	GetChildren() []Object
	GetChildById(id string) Object
	SetParent(parent Object)
	GetParent() Object
}

//the most basic implementation of an dml Object. It is intended as dml grouping
//object as well as base object for other types
type object struct {
	propertyHandler
	eventHandler
	methodHandler

	children []Object
	parent   Object
	id       identifier
	oType    string
	store    datastore.Store

	jsobj *goja.Object
	jsrtm *goja.Runtime
}

func (self *object) Id() identifier {
	return self.id
}

func (self *object) AddChild(obj Object) {

	self.children = append(self.children, obj)
}

func (self *object) GetChildren() []Object {
	return self.children
}

func (self *object) GetChildById(id string) Object {
	return nil
}

func (self *object) SetParent(parent Object) {
	self.parent = parent
	self.id.Parent = parent.Id().hash()
}

func (self *object) GetParent() Object {
	return self.parent
}

func (self *object) GetDataStore() datastore.Store {
	return self.store
}

func (self *object) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *object) GetJSRuntime() *goja.Runtime {
	return self.jsrtm
}

//missing function from property handler
func (self *object) AddProperty(name string, dtype DataType) error {

	if self.HasProperty(name) {
		return fmt.Errorf("Property %s already exists", name)
	}

	//we add properties!
	prop, err := NewProperty(name, dtype, self.GetDataStore(), self.GetJSRuntime())
	if err != nil {
		return err
	}

	self.propertyHandler.properties[name] = prop
	return nil
}

func NewObject(name string, oType string, vm *goja.Runtime, store datastore.Store) *object {

	jsobj := vm.NewObject()

	obj := object{
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		make([]Object, 0),
		nil,
		identifier{[32]byte{}, oType, name},
		oType,
		store,
		jsobj,
		vm,
	}

	return &obj
}
