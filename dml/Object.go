package dml

import (
	"fmt"

	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"

	"github.com/dop251/goja"
)

var parentKey []byte = []byte("__parent")
var dtKey []byte = []byte("__datatype")

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
	GetParentIdentifier(Identifier) (Identifier, error)
	SetParentIdentifier(Identifier, Identifier) error
	GetParent(Identifier) (dmlSet, error)

	//Object type handling (full type desciption of this object)
	GetObjectDataType() DataType
	SetObjectDataType(DataType)

	//Identifier type handling. It could be, that a certain object is used to access
	//the database for a object of different DataType
	GetDataType(Identifier) (DataType, error)
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

	//initialization function
	InitializeDB(Identifier) error
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

	//javascript prototype for this object
	jsProto *goja.Object
}

func NewObject(rntm *Runtime) (*object, error) {

	jsProto := rntm.jsvm.NewObject()
	//jsProto.DefineDataProperty("identifier", rntm.jsvm.ToValue(nil), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_FALSE)

	//build the object
	obj := object{
		NewPropertyHandler(),
		NewEventHandler(),
		NewMethodHandler(),
		rntm,
		DataType{},
		jsProto,
	}

	//default properties
	obj.AddProperty("name", MustNewDataType("string"), "", true)

	//add default events
	obj.AddEvent(NewEvent("onBeforePropertyChange", obj.GetJSPrototype(), rntm))
	obj.AddEvent(NewEvent("onPropertyChanged", obj.GetJSPrototype(), rntm))
	obj.AddEvent(NewEvent("onBeforeChange", obj.GetJSPrototype(), rntm))
	obj.AddEvent(NewEvent("onChanged", obj.GetJSPrototype(), rntm))

	return &obj, nil
}

func (self *object) GetParentIdentifier(id Identifier) (Identifier, error) {

	value, err := valueFromStore(self.rntm.datastore, id, parentKey)
	if err != nil {
		return Identifier{}, err
	}
	parent, err := value.Read()
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to decode parent from DB")
	}

	return *parent.(*Identifier), nil
}

func (self *object) SetParentIdentifier(id Identifier, parent Identifier) error {

	value, err := valueFromStore(self.rntm.datastore, id, parentKey)
	if err != nil {
		return err
	}
	err = value.Write(parent)
	if err != nil {
		return utils.StackError(err, "Unable to decode parent into DB")
	}
	return nil
}

func (self *object) GetParent(id Identifier) (dmlSet, error) {

	parent, err := self.GetParentIdentifier(id)
	if err != nil {
		return dmlSet{}, err
	}

	dt, err := self.GetDataType(parent)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access parent datatype")
	}

	obj, ok := self.rntm.objects[dt]
	if !ok {
		return dmlSet{}, fmt.Errorf("Parent is not setup correctly: no logic object available")
	}

	return dmlSet{obj: obj, id: parent}, nil
}

func (self *object) GetDataType(id Identifier) (DataType, error) {

	value, err := valueFromStore(self.rntm.datastore, id, dtKey)
	if err != nil {
		return DataType{}, err
	}
	dt, err := value.Read()
	if err != nil {
		return DataType{}, utils.StackError(err, "Unable to decode datatype from DB")
	}

	return *dt.(*DataType), nil
}

func (self *object) SetDataType(id Identifier, dt DataType) error {

	value, err := valueFromStore(self.rntm.datastore, id, dtKey)
	if err != nil {
		return err
	}
	err = value.Write(dt)
	if err != nil {
		return utils.StackError(err, "Unable to encode datatype into DB")
	}

	return nil
}

func (self *object) GetObjectDataType() DataType {
	return self.dataType
}

func (self *object) SetObjectDataType(dt DataType) {
	self.dataType = dt
}

func (self *object) GetJSObject(id Identifier) *goja.Object {

	obj := self.rntm.jsvm.CreateObject(self.jsProto)
	obj.Set("identifier", self.rntm.jsvm.ToValue(id))
	return obj
}

func (self *object) GetJSPrototype() *goja.Object {
	return self.jsProto
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
	prop, err := NewProperty(name, dtype, default_val, self.GetRuntime(), self.GetJSPrototype(), constprop)
	if err != nil {
		return err
	}
	/*
		//register change events
		prop.GetEvent("onBeforeChange").RegisterCallback(func(id Identifier, args ...interface{}) error {
			err := self.GetEvent("onBeforePropertyChange").Emit(id, name)
			if err != nil {
				return err
			}
			return self.GetEvent("onBeforeChange").Emit(id)
		})
		prop.GetEvent("onChanged").RegisterCallback(func(id Identifier, args ...interface{}) error {
			err := self.GetEvent("onPropertyChanged").Emit(id, name)
			if err != nil {
				return err
			}
			return self.GetEvent("onChanged").Emit(id)
		})*/

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

func (self *object) InitializeDB(id Identifier) error {

	//first all handlers
	self.InitializeEventDB(id)

	//now our own DB entries
	self.SetParentIdentifier(id, Identifier{})
	self.SetDataType(id, DataType{})

	return nil
}
