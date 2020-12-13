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
//Furthermore must both be available in JS, Global by id an in the child hirarchy.
//It also implements the VersionedData interface, but on identifier basis
type Object interface {
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

	//VersionedData interface based on Identifiers
	HasUpdates(Identifier) (bool, error)
	HasVersions(Identifier) (bool, error)
	ResetHead(Identifier) error
	FixStateAsVersion(Identifier) (datastore.VersionID, error)
	LoadVersion(Identifier, datastore.VersionID) error
	GetLatestVersion(Identifier) (datastore.VersionID, error)
	GetCurrentVersion(Identifier) (datastore.VersionID, error)
	RemoveVersionsUpTo(Identifier, datastore.VersionID) error
	RemoveVersionsUpFrom(Identifier, datastore.VersionID) error
}

//the most basic implementation of an dml Object. It is intended as dml grouping
//object as well as base object for other types
type object struct {
	propertyHandler
	eventHandler
	methodHandler

	//object static state
	rntm     *Runtime
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
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		rntm,
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

	return dt.(DataType), nil
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
	prop, err := NewProperty(name, dtype, default_val, self.GetRuntime(), self.GetJSObject(), constprop)
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

//Versioned Data Interface
func (self *object) HasUpdates(id Identifier) (bool, error) {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.HasUpdates()
}

func (self *object) HasVersions(id Identifier) (bool, error) {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.HasVersions()
}

func (self *object) ResetHead(id Identifier) error {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.ResetHead()
}

func (self *object) FixStateAsVersion(id Identifier) (datastore.VersionID, error) {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.FixStateAsVersion()
}

func (self *object) LoadVersion(id Identifier, vId datastore.VersionID) error {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.LoadVersion(vId)
}

func (self *object) GetLatestVersion(id Identifier) (datastore.VersionID, error) {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.GetLatestVersion()
}

func (self *object) GetCurrentVersion(id Identifier) (datastore.VersionID, error) {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.GetCurrentVersion()
}

func (self *object) RemoveVersionsUpTo(id Identifier, vId datastore.VersionID) error {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.RemoveVersionsUpTo(vId)
}

func (self *object) RemoveVersionsUpFrom(id Identifier, vId datastore.VersionID) error {
	mngr := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	return mngr.RemoveVersionsUpFrom(vId)
}
