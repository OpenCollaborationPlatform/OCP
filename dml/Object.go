package dml

import "github.com/dop251/goja"

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
	Id() string

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
	id       string

	jsobj *goja.Object
	jsrtm *goja.Runtime
}

func (self *object) Id() string {
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
}

func (self *object) GetParent() Object {
	return self.parent
}

func (self *object) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *object) GetJSRuntime() *goja.Runtime {
	return self.jsrtm
}

func NewObject(name string, vm *goja.Runtime) *object {

	jsobj := vm.NewObject()

	obj := object{
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		make([]Object, 0),
		nil,
		name,
		jsobj,
		vm,
	}

	return &obj
}
