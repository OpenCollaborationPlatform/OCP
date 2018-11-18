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
	datastore.VersionManager
	PropertyHandler
	EventHandler
	MethodHandler
	JSObject

	//Object functions
	Id() identifier

	//Object hirarchy
	AddChild(obj Object)
	GetChildren() []Object
	GetChildById(id string) (Object, error)
	GetParent() Object

	//Subobject handling (child + extras)
	ForEachSubobject(recursive bool, fnc func(obj Object) error) error //to be overriden
}

//the most basic implementation of an dml Object. It is intended as dml grouping
//object as well as base object for other types
type object struct {
	datastore.VersionManagerImp
	propertyHandler
	eventHandler
	methodHandler

	//object
	children []Object
	parent   Object
	id       identifier
	oType    string

	//javascript
	jsobj *goja.Object
	jsrtm *goja.Runtime
}

func NewObject(parent Object, name string, oType string, vm *goja.Runtime, store *datastore.Datastore) *object {

	jsobj := vm.NewObject()

	var hash [32]byte
	if parent != nil {
		hash = parent.Id().hash()
	}
	id := identifier{hash, oType, name}

	obj := object{
		datastore.NewVersionManager(id.hash(), store),
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		make([]Object, 0),
		parent,
		id,
		oType,
		jsobj,
		vm,
	}

	return &obj
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

func (self *object) GetChildById(id string) (Object, error) {

	for _, child := range self.children {

		if child.Id().Name == id {
			return child, nil
		}
	}

	return nil, fmt.Errorf("No child available with id %v", id)
}

func (self *object) GetParent() Object {
	return self.parent
}

//not only childs, but also other subobjects like vector elements. Must be
//implemented by subclasses if other subobjects than children exist
func (self *object) ForEachSubobject(recursive bool, fnc func(obj Object) error) error {

	for _, child := range self.children {
		fnc(child)
		if recursive {
			err := child.ForEachSubobject(true, fnc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *object) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *object) GetJSRuntime() *goja.Runtime {
	return self.jsrtm
}

//missing function from property handler
func (self *object) AddProperty(name string, dtype DataType, constprop bool) error {

	if self.HasProperty(name) {
		return fmt.Errorf("Property %s already exists", name)
	}

	//we add properties
	set, ok := self.GetDatabaseSet(datastore.ValueType).(*datastore.ValueVersionedSet)
	if !ok {
		return fmt.Errorf("Unable to create database set")
	}
	prop, err := NewProperty(name, dtype, set, self.GetJSRuntime(), constprop)
	if err != nil {
		return err
	}

	self.propertyHandler.properties[name] = prop
	return nil
}
