package dml

import (
	"fmt"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"

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

	//object dynamic state (hence in db)
	refCount datastore.ValueVersioned

	//javascript
	jsobj *goja.Object
}

func NewObject(id Identifier, parent Identifier, rntm *Runtime) (*object, error) {

	jsobj := rntm.jsvm.NewObject()

	//the versionmanager to access the datastores correctly
	versManager := datastore.NewVersionManager(id.Hash(), rntm.datastore)

	//the store for the refcount
	set, err := versManager.GetDatabaseSet(datastore.ValueType)
	if err != nil {
		return nil, utils.StackError(err, "Unable to acess database set")
	}
	vvset := set.(*datastore.ValueVersionedSet)
	vvRefCnt, err := vvset.GetOrCreateValue([]byte("__refcount"))
	if err != nil {
		return nil, utils.StackError(err, "Unable to create refcount database entry")
	}
	
	//default value
	holds, err := vvRefCnt.HoldsValue()
	if err != nil {
		return nil, utils.StackError(err, "Unable to write ref count default value")
	}
	if !holds {
		vvRefCnt.Write(uint64(0))
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
		*vvRefCnt,
		jsobj,
	}

	//default properties
	obj.AddProperty("name", MustNewDataType("string"), "", true)

	//add default methods
	obj.AddMethod("Identifier", MustNewMethod(obj.Id().Encode, true))

	//add default events
	obj.AddEvent("onPropertyChanged", NewEvent(obj.GetJSObject(), rntm, MustNewDataType("string")))

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

	val, err := self.refCount.Read()
	if err != nil {
		return 0, err
	}
	return val.(uint64), nil
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
	prop, err := NewProperty(name, dtype, default_val, vSet, self.GetRuntime(), constprop)
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
