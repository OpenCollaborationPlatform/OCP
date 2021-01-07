package dml

import (
	"fmt"

	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"

	"github.com/dop251/goja"
)

var parentKey []byte = []byte("__object_parent")
var dtKey []byte = []byte("__object_datatype")
var pathKey []byte = []byte("__object_path")

//Interface of an object: All objects, data and behaviour, must be able to handle
//  - Properties
//  - Events
//  - Methods
//Furthermore must both be available in JS, Global by id an in the child hirarchy.
//It also implements the VersionedData interface, but on identifier basis
type Object interface {
	PropertyHandler
	PropertyChangeNotifyer
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

	//Let the object know it's path in the dml runtime. This is important for event
	//emitting, as it needs to know what exact uri to use. Note that the path cannot
	//be determined at runtime from an object, as in maps or vectors it is not possible
	//to get the key easily by value (only by iterating, but thats expensive for each
	//event emit)
	GetObjectPath(Identifier) (string, error)
	SetObjectPath(Identifier, string) error //sets the full path including the object name

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

	//helpers method for getting database access
	GetDBValue(Identifier, []byte) (datastore.Value, error)
	GetDBValueVersioned(Identifier, []byte) (datastore.ValueVersioned, error)
	GetDBMap(Identifier, []byte) (datastore.Map, error)
	GetDBMapVersioned(Identifier, []byte) (datastore.MapVersioned, error)
	GetDBList(Identifier, []byte) (datastore.List, error)
	GetDBListVersioned(Identifier, []byte) (datastore.ListVersioned, error)

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
	obj := &object{
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
	obj.AddEvent(NewEvent("onBeforePropertyChange", obj))
	obj.AddEvent(NewEvent("onPropertyChanged", obj))
	obj.AddEvent(NewEvent("onBeforeChange", obj))
	obj.AddEvent(NewEvent("onChanged", obj))

	return obj, nil
}

func (self *object) GetParentIdentifier(id Identifier) (Identifier, error) {

	value, err := self.GetDBValue(id, parentKey)
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

	value, err := self.GetDBValue(id, parentKey)
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

	if !parent.Valid() {
		return dmlSet{}, fmt.Errorf("Object has no parent")
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

	value, err := self.GetDBValue(id, dtKey)
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

	value, err := self.GetDBValue(id, dtKey)
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

func (self *object) GetObjectPath(id Identifier) (string, error) {

	value, err := self.GetDBValueVersioned(id, pathKey)
	if err != nil {
		return "", utils.StackError(err, "Unable to retreive path")
	}
	path, err := value.Read()
	if err != nil {
		return "", utils.StackError(err, "Unable to retreive path")
	}

	return path.(string), nil
}

//set the path without this object in it
func (self *object) SetObjectPath(id Identifier, path string) error {

	value, err := self.GetDBValueVersioned(id, pathKey)
	if err != nil {
		return utils.StackError(err, "Unable to retreive path DB")
	}

	err = value.Write(path)
	if err != nil {
		return utils.StackError(err, "Unable to write path")
	}

	return nil
}

func (self *object) BeforePropertyChange(id Identifier, name string) error {
	self.GetEvent("onBeforePropertyChange").Emit(id, name)
	return nil
}

func (self *object) PropertyChanged(id Identifier, name string) error {
	self.GetEvent("onPropertyChanged").Emit(id, name)
	return nil
}

func (self *object) EventEmitted(id Identifier, name string, args ...interface{}) error {

	path, err := self.GetObjectPath(id)
	if err != nil {
		return err
	}
	return self.rntm.emitEvent(path, name, args...)
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

func (self *object) GetDBValue(id Identifier, key []byte) (datastore.Value, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ValueType, false, id.Hash())
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	vset, done := set.(*datastore.ValueSet)
	if !done {
		return datastore.Value{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *value, nil
}

func (self *object) GetDBValueVersioned(id Identifier, key []byte) (datastore.ValueVersioned, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ValueType, true, id.Hash())
	if err != nil {
		return datastore.ValueVersioned{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	vset, done := set.(*datastore.ValueVersionedSet)
	if !done {
		return datastore.ValueVersioned{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.ValueVersioned{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *value, nil
}

func (self *object) GetDBMap(id Identifier, key []byte) (datastore.Map, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.MapType, false, id.Hash())
	if err != nil {
		return datastore.Map{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	mset, done := set.(*datastore.MapSet)
	if !done {
		return datastore.Map{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	map_, err := mset.GetOrCreateMap(key)
	if err != nil {
		return datastore.Map{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *map_, nil
}

func (self *object) GetDBMapVersioned(id Identifier, key []byte) (datastore.MapVersioned, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.MapType, true, id.Hash())
	if err != nil {
		return datastore.MapVersioned{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	mset, done := set.(*datastore.MapVersionedSet)
	if !done {
		return datastore.MapVersioned{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	map_, err := mset.GetOrCreateMap(key)
	if err != nil {
		return datastore.MapVersioned{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *map_, nil
}

func (self *object) GetDBList(id Identifier, key []byte) (datastore.List, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ListType, false, id.Hash())
	if err != nil {
		return datastore.List{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	lset, done := set.(*datastore.ListSet)
	if !done {
		return datastore.List{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	list, err := lset.GetOrCreateList(key)
	if err != nil {
		return datastore.List{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *list, nil
}

func (self *object) GetDBListVersioned(id Identifier, key []byte) (datastore.ListVersioned, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ListType, true, id.Hash())
	if err != nil {
		return datastore.ListVersioned{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	lset, done := set.(*datastore.ListVersionedSet)
	if !done {
		return datastore.ListVersioned{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	list, err := lset.GetOrCreateList(key)
	if err != nil {
		return datastore.ListVersioned{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *list, nil
}
