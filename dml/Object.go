package dml

import (
	datastore "CollaborationNode/datastores"
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
	IncreaseRefcount()
	DecreaseRefcount()
	GetRefcount() uint64
	SetRefcount(uint64)

	//Object functions
	Id() identifier
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

	//object
	rntm     *Runtime
	parent   identifier
	id       identifier
	dataType DataType
	refCount datastore.ValueVersioned

	//javascript
	jsobj *goja.Object
}

func NewObject(parent identifier, name string, oType string, rntm *Runtime) *object {

	jsobj := rntm.jsvm.NewObject()
	id := identifier{parent.hash(), oType, name}

	//the versionmanager to access the datastores correctly
	versManager := datastore.NewVersionManager(id.hash(), rntm.datastore)

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
	obj.AddMethod("Identifier", MustNewMethod(obj.Id().encode))

	return &obj
}

func (self *object) Id() identifier {
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

func (self *object) IncreaseRefcount() {
	current := self.GetRefcount()
	self.refCount.Write(current + 1)
}

func (self *object) DecreaseRefcount() {
	current := self.GetRefcount()
	self.refCount.Write(current - 1)
}

func (self *object) GetRefcount() uint64 {
	var current uint64
	self.refCount.ReadType(&current)
	return current
}

func (self *object) SetRefcount(val uint64) {
	self.refCount.Write(val)
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
