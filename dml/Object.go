package dml

import (
	"fmt"
	"github.com/ickby/CollaborationNode/datastores"

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
	Id() Identifier
	GetParent() Object

	//type handling (full type desciption)
	DataType() DataType
	SetDataType(DataType)

	//Genertic
	GetRuntime() *Runtime
}

//the most basic implementation of an dml Object. It is intended as dml grouping
//object as well as base object for other types
type object struct {
	datastore.VersionManagerImp
	propertyHandler
	eventHandler
	methodHandler

	//object static state
	rntm     *Runtime
	parent   Identifier
	id       Identifier
	dataType DataType

	//javascript
	jsobj *goja.Object
}

func NewObject(id Identifier, parent Identifier, rntm *Runtime) (*object, error) {

	jsobj := rntm.jsvm.NewObject()

	//the versionmanager to access the datastores correctly
	versManager := datastore.NewVersionManager(id.Hash(), rntm.datastore)

	//build the object
	obj := object{
		versManager,
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		rntm,
		parent,
		id,
		DataType{},
		jsobj,
	}

	//default properties
	obj.AddProperty("name", MustNewDataType("string"), "", true)

	//add default methods
	obj.AddMethod("Identifier", MustNewMethod(obj.Id().Encode, true))

	//add default events
	obj.AddEvent("onPropertyChanged", NewEvent(obj.GetJSObject(), rntm))

	return &obj, nil
}

func (self *object) Id() Identifier {
	return self.id
}

func (self *object) GetParent() Object {

	return self.rntm.objects[self.parent]
}

func (self *object) DataType() DataType {
	return self.dataType
}

func (self *object) SetDataType(t DataType) {
	self.dataType = t
}

func (self *object) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *object) GetJSRuntime() *goja.Runtime {
	return self.rntm.jsvm
}

//missing function from property handler
func (self *object) AddProperty(name string, dtype DataType, default_val interface{}, constprop bool) error {

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
	prop, err := NewProperty(name, dtype, default_val, vSet, self.GetRuntime(), self.GetJSObject(), constprop)
	if err != nil {
		return err
	}

	self.propertyHandler.properties[name] = prop

	//register change event
	prop.GetEvent("onChanged").RegisterCallback(func(...interface{}) error {
		return self.GetEvent("onPropertyChanged").Emit(name)
	})

	return nil
}

func (self *object) GetRuntime() *Runtime {
	return self.rntm
}
