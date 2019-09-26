package dml

import (
	
	datastore "github.com/ickby/CollaborationNode/datastores"
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

	//garbage collection
	IncreaseRefcount() error
	DecreaseRefcount() error
	GetRefcount() (uint64, error)
	SetRefcount(uint64) error

	//Object functions
	Id() Identifier
	GetParent() Object

	//type handling (full type desciption)
	DataType() DataType
	SetDataType(DataType)
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

	//object dynamic state (hence in db)
	refCount datastore.ValueVersioned

	//javascript
	jsobj *goja.Object
}

func NewObject(parent Identifier, name string, oType string, rntm *Runtime) *object {

	jsobj := rntm.jsvm.NewObject()
	id := Identifier{parent.Hash(), oType, name}

	//the versionmanager to access the datastores correctly
	versManager := datastore.NewVersionManager(id.Hash(), rntm.datastore)

	//the store for the refcount
	set, err := versManager.GetDatabaseSet(datastore.ValueType)
	if err != nil {
		return nil
	}
	vvset := set.(*datastore.ValueVersionedSet)
	vvRecCnt, err := vvset.GetOrCreateValue([]byte("__refcount"))
	if err != nil {
		return nil
	}

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
		*vvRecCnt,
		jsobj,
	}

	//default properties
	obj.AddProperty("id", MustNewDataType("string"), "", true)

	//add default methods
	obj.AddMethod("Identifier", MustNewMethod(obj.Id().Encode))

	return &obj
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

func (self *object) IncreaseRefcount() error {
	current, err := self.GetRefcount()
	if err != nil {
		return err
	}
	self.refCount.Write(current + 1)
	return nil
}

func (self *object) DecreaseRefcount() error {
	current, err := self.GetRefcount()
	if err != nil {
		return err
	}
	self.refCount.Write(current - 1)
	return nil
}

func (self *object) GetRefcount() (uint64, error) {

	if !self.refCount.IsValid() {
		return 0, fmt.Errorf("Unable to access refcount")
	}

	var current uint64
	self.refCount.ReadType(&current)
	return current, nil
}

func (self *object) SetRefcount(val uint64) error {
	return self.refCount.Write(val)
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
	prop, err := NewProperty(name, dtype, default_val, vSet, self.GetJSRuntime(), constprop)
	if err != nil {
		return err
	}

	self.propertyHandler.properties[name] = prop
	return nil
}
