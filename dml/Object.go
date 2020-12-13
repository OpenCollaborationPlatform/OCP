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

	//Object functions
	Parent(Identifier) (Identifier, error)
	SetParent(Identifier, Identifier) error

	//type handling (full type desciption)
	DataType(Identifier) (DataType, error)
	SetDataType(Identifier, DataType) error

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

func NewObject(rntm *Runtime) (*object, error) {

	jsobj := rntm.jsvm.NewObject()

	//the versionmanager to access the datastores correctly
	//versManager := datastore.NewVersionManager(id.Hash(), rntm.datastore)

	//build the object
	obj := object{
		nil, //versManager,
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
	obj.AddEvent("onBeforePropertyChange", NewEvent(obj.GetJSObject(), rntm))
	obj.AddEvent("onPropertyChanged", NewEvent(obj.GetJSObject(), rntm))
	obj.AddEvent("onBeforeChange", NewEvent(obj.GetJSObject(), rntm))
	obj.AddEvent("onChanged", NewEvent(obj.GetJSObject(), rntm))

	return &obj, nil
}

// helper methods for DB access
func valueFromStore(store datastore.Datastore, id Identifier, key []byte) (*datastore.Value, error) {

	set, err := store.GetOrCreateSet(datastore.ValueType, false, id.Hash())
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to load %s from database", string(key))
	}
	vset, err := set.(*datastore.ValueSet)
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return value, nil
}

func (self *object) Parent(id Identifier) (Identifier, error) {

	value, err := valueFromStore(self.rntm.datastore, id, []byte("__parent"))
	if err != nil {
		return Identifier{}, err
	}
	parent, err := value.Read()
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to decode parent from DB")
	}

	return parent.(Identifier), nil
}

func (self *object) SetParent(id Identifier, parent Identifier) error {

	value, err := valueFromStore(self.rntm.datastore, id, []byte("__parent"))
	if err != nil {
		return err
	}
	err = value.Write(parent)
	if err != nil {
		return utils.StackError(err, "Unable to decode parent into DB")
	}
	return nil
}

func (self *object) DataType(id Identifier) (DataType, error) {

	value, err := valueFromStore(self.rntm.datastore, id, []byte("__datatype"))
	if err != nil {
		return DataType{}, err
	}
	dt, err := value.Read()
	if err != nil {
		return DataType{}, utils.StackError(err, "Unable to decode datatype from DB")
	}

	return parent.(DataType), nil
}

func (self *object) SetDataType(id Identifier, dt DataType) error {

	value, err := valueFromStore(self.rntm.datastore, id, []byte("__datatype"))
	if err != nil {
		return err
	}
	err = value.Write(dt)
	if err != nil {
		return utils.StackError(err, "Unable to decode datatype into DB")
	}
	return nil
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

	//register change events
	prop.GetEvent("onBeforeChange").RegisterCallback(func(...interface{}) error {
		err := self.GetEvent("onBeforePropertyChange").Emit(name)
		if err != nil {
			return err
		}
		return self.GetEvent("onBeforeChange").Emit()
	})
	prop.GetEvent("onChanged").RegisterCallback(func(...interface{}) error {
		err := self.GetEvent("onPropertyChanged").Emit(name)
		if err != nil {
			return err
		}
		return self.GetEvent("onChanged").Emit()
	})

	//everthing went without error, now we can set this property
	self.propertyHandler.properties[name] = prop

	return nil
}

func (self *object) GetRuntime() *Runtime {
	return self.rntm
}
