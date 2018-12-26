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
	GetChildByName(name string) (Object, error)
	GetParent() Object
}

//the most basic implementation of an dml Object. It is intended as dml grouping
//object as well as base object for other types
type object struct {
	datastore.VersionManagerImp
	propertyHandler
	eventHandler
	methodHandler

	//object
	rntm     *Runtime
	children []identifier
	parent   identifier
	id       identifier
	oType    string

	//javascript
	jsobj *goja.Object
}

func NewObject(parent identifier, name string, oType string, rntm *Runtime) *object {

	jsobj := rntm.jsvm.NewObject()
	id := identifier{parent.hash(), oType, name}

	obj := object{
		datastore.NewVersionManager(id.hash(), rntm.datastore),
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		rntm,
		make([]identifier, 0),
		parent,
		id,
		oType,
		jsobj,
	}

	rntm.objects[id] = &obj
	return &obj
}

func (self *object) Id() identifier {
	return self.id
}

func (self *object) AddChild(obj Object) {

	self.children = append(self.children, obj.Id())
}

func (self *object) GetChildren() []Object {

	result := make([]Object, len(self.children))
	for i, child := range self.children {
		result[i] = self.rntm.objects[child]
	}
	return result
}

func (self *object) GetChildByName(name string) (Object, error) {

	for _, child := range self.children {
		if child.Name == name {
			return self.rntm.objects[child], nil
		}
	}
	return nil, fmt.Errorf("No such object available")
}

func (self *object) GetParent() Object {

	return self.rntm.objects[self.parent]
}

////not only childs, but also other subobjects like vector elements. Must be
////implemented by subclasses if other subobjects than children exist
//func (self *object) ForEachSubobject(recursive bool, fnc func(obj Object) error) error {

//	for _, child := range self.children {
//		fnc(child)
//		if recursive {
//			err := child.ForEachSubobject(true, fnc)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}

//func (self *Object) GetSubobject(id Identifier, recursive bool) (Object, bool) {

//	for _, child := range self.children {

//		if child.Id.equal(id) {
//			return child, true
//		}
//		if recursive {
//			obj, ok := child.GetSubobject(id, true)
//			if ok {
//				return obj, ok
//			}
//		}
//	}
//	return nil, false
//}

//func (self *object) GetSubobjectById(id identifier, recursive bool) (Object, error) {

//	for _, child := range self.children {

//		if child.Id.equal(id) {
//			return child, true
//		}
//		if recursive {
//			obj, ok := child.GetSubobject(id, true)
//			if ok {
//				return obj, ok
//			}
//		}
//	}

//	return nil, fmt.Errorf("No child available with id %v", id)
//}

func (self *object) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *object) GetJSRuntime() *goja.Runtime {
	return self.rntm.jsvm
}

//missing function from property handler
func (self *object) AddProperty(name string, dtype DataType, constprop bool) error {

	if self.HasProperty(name) {
		return fmt.Errorf("Property %s already exists", name)
	}

	//we add properties
	set, err := self.GetDatabaseSet(datastore.ValueType)
	if err != nil {
		return err
	}
	vSet, ok := set.(*datastore.ValueVersionedSet)
	if !ok {
		return fmt.Errorf("Unable to create database set")
	}
	prop, err := NewProperty(name, dtype, vSet, self.GetJSRuntime(), constprop)
	if err != nil {
		return err
	}

	self.propertyHandler.properties[name] = prop
	return nil
}
