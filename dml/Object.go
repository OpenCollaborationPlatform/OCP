package dml

import (
	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"

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
	EventEmitNotifyer
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

	//VersionedData interface based on Identifiers (For whole object)
	HasUpdates(Identifier) (bool, error)
	HasVersions(Identifier) (bool, error)
	ResetHead(Identifier) error
	FixStateAsVersion(Identifier) (datastore.VersionID, error)
	LoadVersion(Identifier, datastore.VersionID) error
	GetLatestVersion(Identifier) (datastore.VersionID, error)
	GetCurrentVersion(Identifier) (datastore.VersionID, error)
	RemoveVersionsUpTo(Identifier, datastore.VersionID) error
	RemoveVersionsUpFrom(Identifier, datastore.VersionID) error

	//VersionedData interface based on keys, and subkeys
	BuildVersionedKey(Identifier, datastore.StorageType, []byte, []interface{}) datastore.Key
	KeysAnyHasUpdates([]datastore.Key) (bool, error)   //true if any of the given keys has an update
	KeysAllHaveVersions([]datastore.Key) (bool, error) //true if all of the given keys have updates
	KeysResetHead([]datastore.Key) error
	KeysFixStateAsVersion([]datastore.Key) ([]datastore.VersionID, error)
	KeysLoadVersion([]datastore.Key, []datastore.VersionID) error
	KeysGetLatestVersion([]datastore.Key) ([]datastore.VersionID, error)
	KeysGetCurrentVersion([]datastore.Key) ([]datastore.VersionID, error)
	KeysRemoveVersionsUpTo([]datastore.Key, []datastore.VersionID) error
	KeysRemoveVersionsUpFrom([]datastore.Key, []datastore.VersionID) error

	//Key handling for generic access to Data, events, properties, methods etc.
	GetByKey(Identifier, Key) (interface{}, error)    //Returns whatever the key represents in the Dataobject
	HasKey(Identifier, Key) (bool, error)             //Returns true if the provided key exists
	GetKeys(Identifier) ([]Key, error)                //returns all available keys
	keyRemoved(Identifier, Key) error                 //internal: Called whenever a key gets removed by external code
	keyToDS(Identifier, Key) ([]datastore.Key, error) //internal: Translates a object key to its coresponding datastore keys.

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

	return obj, nil
}

func (self *object) GetParentIdentifier(id Identifier) (Identifier, error) {

	value, err := self.GetDBValue(id, parentKey)
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to access DB value")
	}
	parent, err := value.Read()
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to read parent from DB")
	}

	return *parent.(*Identifier), nil
}

func (self *object) SetParentIdentifier(id Identifier, parent Identifier) error {

	value, err := self.GetDBValue(id, parentKey)
	if err != nil {
		return utils.StackError(err, "Unable to access DB value")
	}
	err = value.Write(parent)
	if err != nil {
		return utils.StackError(err, "Unable to write parent into DB")
	}
	return nil
}

func (self *object) GetParent(id Identifier) (dmlSet, error) {

	parent, err := self.GetParentIdentifier(id)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to get parent identifier")
	}

	if !parent.Valid() {
		return dmlSet{}, newInternalError(Error_Operation_Invalid, "Object has no parent")
	}

	dt, err := self.GetDataType(parent)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access parent datatype")
	}

	obj, ok := self.rntm.objects[dt]
	if !ok {
		return dmlSet{}, newInternalError(Error_Setup_Invalid, "Parent is not setup correctly: no logic object available")
	}

	return dmlSet{obj: obj, id: parent}, nil
}

func (self *object) GetDataType(id Identifier) (DataType, error) {

	value, err := self.GetDBValue(id, dtKey)
	if err != nil {
		return DataType{}, utils.StackError(err, "Unable to access DB value")
	}
	dt, err := value.Read()
	if err != nil {
		return DataType{}, utils.StackError(err, "Unable to read datatype from DB")
	}

	return *dt.(*DataType), nil
}

func (self *object) SetDataType(id Identifier, dt DataType) error {

	value, err := self.GetDBValue(id, dtKey)
	if err != nil {
		return utils.StackError(err, "Unable to access DB value")
	}
	err = value.Write(dt)
	if err != nil {
		return utils.StackError(err, "Unable to write datatype into DB")
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
		return "", utils.StackError(err, "Unable to access DB value")
	}
	path, err := value.Read()
	if err != nil {
		return "", utils.StackError(err, "Unable to read from DB value")
	}

	return path.(string), nil
}

//set the path without this object in it
func (self *object) SetObjectPath(id Identifier, path string) error {

	value, err := self.GetDBValueVersioned(id, pathKey)
	if err != nil {
		return utils.StackError(err, "Unable to access DB value")
	}

	err = value.Write(path)
	if err != nil {
		return utils.StackError(err, "Unable to write into DB value")
	}

	return nil
}

func (self *object) BeforePropertyChange(id Identifier, name string) error {
	return self.GetEvent("onBeforePropertyChange").Emit(id, name)
}

func (self *object) PropertyChanged(id Identifier, name string) error {
	return self.GetEvent("onPropertyChanged").Emit(id, name)
}

func (self *object) EventEmitted(id Identifier, name string, args ...interface{}) error {

	path, err := self.GetObjectPath(id)
	if err != nil {
		return utils.StackError(err, "Unable to retreive object path")
	}
	return utils.StackError(self.rntm.emitEvent(path, name, args...), "Event emittion failed")
}

func (self *object) GetRuntime() *Runtime {
	return self.rntm
}

func (self *object) InitializeDB(id Identifier) error {

	//first all handlers
	if err := self.InitializeEventDB(id); err != nil {
		return err
	}

	//now our own DB entries
	if err := self.SetParentIdentifier(id, Identifier{}); err != nil {
		return utils.StackError(err, "Unable to set parent identifier for %v", id)
	}
	if err := self.SetDataType(id, DataType{}); err != nil {
		return utils.StackError(err, "Unable to set data type for %v", id)
	}
	if err := self.SetObjectPath(id, ""); err != nil {
		return utils.StackError(err, "Unable to set object path for %v", id)
	}

	return nil
}

// Key handling
// *************************************************************************************************************
func (self *object) GetByKey(id Identifier, key Key) (interface{}, error) {

	accessor := key.AsString()
	if self.HasProperty(accessor) {
		return self.GetProperty(accessor), nil
	}
	if self.HasMethod(accessor) {
		return self.GetMethod(accessor), nil
	}
	if self.HasEvent(accessor) {
		return self.GetEvent(accessor), nil
	}

	return nil, newUserError(Error_Key_Not_Available, "Key not available in object")
}

func (self *object) HasKey(id Identifier, key Key) (bool, error) {
	//we only hold properties/methods/events, hence we check those
	accessor := key.AsString()
	has := self.HasProperty(accessor) || self.HasMethod(accessor) || self.HasEvent(accessor)
	return has, nil
}

func (self *object) GetKeys(id Identifier) ([]Key, error) {

	props := self.GetProperties()
	result := make([]Key, len(props))
	for i, name := range props {
		result[i] = MustNewKey(name)
	}

	methods := self.Methods()
	for _, name := range methods {
		result = append(result, MustNewKey(name))
	}

	events := self.Events()
	for _, name := range events {
		result = append(result, MustNewKey(name))
	}

	return result, nil
}

func (self *object) keyRemoved(id Identifier, key Key) error {
	//Properties cannot be removed: fail!
	return newInternalError(Error_Operation_Invalid, "object property cannot be removed", "Key", key)
}

func (self *object) keyToDS(id Identifier, key Key) ([]datastore.Key, error) {

	if !self.HasProperty(key.AsString()) {
		//events and methods do not have DS keys assiociated
		return []datastore.Key{}, nil
	}

	prop := self.GetProperty(key.AsString())
	if prop.IsConst() {
		return nil, newInternalError(Error_Operation_Invalid, "Const property does not have DS key")
	}

	dataProp, ok := prop.(*dataProperty)
	if !ok {
		return nil, newInternalError(Error_Setup_Invalid, "Property s of wrong type")
	}
	return []datastore.Key{dataProp.getDSKey(id)}, nil
}

// Database Handling
// *************************************************************************************************************

//Versioned Data Interface with identifiers for whole object
func (self *object) HasUpdates(id Identifier) (bool, error) {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return false, utils.StackError(err, "Unable to access DB version manager")
	}
	res, err := mngr.HasUpdates()
	return res, utils.StackError(err, "Unable to query DB for updates")
}

func (self *object) HasVersions(id Identifier) (bool, error) {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return false, utils.StackError(err, "Unable to access DB version manager")
	}
	res, err := mngr.HasVersions()
	return res, utils.StackError(err, "Unable to query DB for versions")
}

func (self *object) ResetHead(id Identifier) error {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return utils.StackError(err, "Unable to access DB version manager")
	}
	return utils.StackError(mngr.ResetHead(), "Unable to reset head in DB")
}

func (self *object) FixStateAsVersion(id Identifier) (datastore.VersionID, error) {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return datastore.VersionID(datastore.INVALID), utils.StackError(err, "Unable to access DB version manager")
	}
	res, err := mngr.FixStateAsVersion()
	return res, utils.StackError(err, "Unable to fix state")
}

func (self *object) LoadVersion(id Identifier, vId datastore.VersionID) error {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return utils.StackError(err, "Unable to access DB version manager")
	}
	return utils.StackError(mngr.LoadVersion(vId), "Unable to load DB version")
}

func (self *object) GetLatestVersion(id Identifier) (datastore.VersionID, error) {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return datastore.VersionID(datastore.INVALID), utils.StackError(err, "Unable to access DB version manager")
	}
	res, err := mngr.GetLatestVersion()
	return res, utils.StackError(err, "Unable to access latest version in DB")
}

func (self *object) GetCurrentVersion(id Identifier) (datastore.VersionID, error) {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return datastore.VersionID(datastore.INVALID), utils.StackError(err, "Unable to access DB version manager")
	}
	res, err := mngr.GetCurrentVersion()
	return res, utils.StackError(err, "Unable to access current version in DB")
}

func (self *object) RemoveVersionsUpTo(id Identifier, vId datastore.VersionID) error {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return utils.StackError(err, "Unable to access DB version manager")
	}
	return utils.StackError(mngr.RemoveVersionsUpTo(vId), "Unable to remove versions in DB")
}

func (self *object) RemoveVersionsUpFrom(id Identifier, vId datastore.VersionID) error {
	mngr, err := datastore.NewVersionManager(id.Hash(), self.rntm.datastore)
	if err != nil {
		return utils.StackError(err, "Unable to access DB version manager")
	}
	return utils.StackError(mngr.RemoveVersionsUpFrom(vId), "Unable to remove versions in DB")
}

func (self *object) BuildVersionedKey(id Identifier, storage datastore.StorageType, key []byte, subentries []interface{}) datastore.Key {

	if subentries == nil {
		subentries = make([]interface{}, 0)
	}
	return datastore.NewKey(storage, true, id.Hash(), key, subentries)
}

func (self *object) KeysAnyHasUpdates(keys []datastore.Key) (bool, error) {

	for _, key := range keys {
		if !key.Versioned {
			return false, newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return false, utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		upd, err := entry.HasUpdates()
		if err != nil {
			return false, utils.StackError(err, "Unable to check updates for key", "Key", key)
		}
		if upd {
			return true, nil
		}
	}
	return false, nil
}

func (self *object) KeysAllHaveVersions(keys []datastore.Key) (bool, error) {

	for _, key := range keys {
		if !key.Versioned {
			return false, newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return false, utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		ver, err := entry.HasVersions()
		if err != nil {
			return false, utils.StackError(err, "Unable to check versions for key", "Key", key)
		}
		if !ver {
			return false, nil
		}
	}
	return true, nil
}

func (self *object) KeysResetHead(keys []datastore.Key) error {

	for _, key := range keys {
		if !key.Versioned {
			return newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		err = entry.ResetHead()
		if err != nil {
			return utils.StackError(err, "Unable to reset head for key", "Key", key)
		}
	}
	return nil
}

func (self *object) KeysFixStateAsVersion(keys []datastore.Key) ([]datastore.VersionID, error) {

	result := make([]datastore.VersionID, len(keys))
	for i, key := range keys {
		if !key.Versioned {
			return nil, newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		res, err := entry.FixStateAsVersion()
		if err != nil {
			return nil, utils.StackError(err, "Unable to fix version for key", "Key", key)
		}
		result[i] = res
	}
	return result, nil
}

func (self *object) KeysLoadVersion(keys []datastore.Key, versions []datastore.VersionID) error {

	if len(keys) != len(versions) {
		return newInternalError(Error_Operation_Invalid, "One versions needs to be provided for each key", "NumKeys", len(keys), "NumVersions", len(versions))
	}

	for i, key := range keys {
		if !key.Versioned {
			return newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		err = entry.LoadVersion(versions[i])
		if err != nil {
			return utils.StackError(err, "Unable to load version for key", "Key", key, "Version", versions[i])
		}
	}
	return nil
}

func (self *object) KeysGetLatestVersion(keys []datastore.Key) ([]datastore.VersionID, error) {

	result := make([]datastore.VersionID, len(keys))
	for i, key := range keys {
		if !key.Versioned {
			return nil, newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		res, err := entry.GetLatestVersion()
		if err != nil {
			return nil, utils.StackError(err, "Unable to get latest version for key", "Key", key)
		}
		result[i] = res
	}
	return result, nil
}

func (self *object) KeysGetCurrentVersion(keys []datastore.Key) ([]datastore.VersionID, error) {

	result := make([]datastore.VersionID, len(keys))
	for i, key := range keys {
		if !key.Versioned {
			return nil, newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		res, err := entry.GetCurrentVersion()
		if err != nil {
			return nil, utils.StackError(err, "Unable to get current version for key", "Key", key)
		}
		result[i] = res
	}
	return result, nil
}

func (self *object) KeysRemoveVersionsUpTo(keys []datastore.Key, versions []datastore.VersionID) error {

	if len(keys) != len(versions) {
		return newInternalError(Error_Operation_Invalid, "One versions needs to be provided for each key", "NumKeys", len(keys), "NumVersions", len(versions))
	}

	for i, key := range keys {
		if !key.Versioned {
			return newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		err = entry.RemoveVersionsUpTo(versions[i])
		if err != nil {
			return utils.StackError(err, "Unable to remove versions for key", "Key", key, "UpTo", versions[i])
		}
	}
	return nil
}

func (self *object) KeysRemoveVersionsUpFrom(keys []datastore.Key, versions []datastore.VersionID) error {

	if len(keys) != len(versions) {
		return newInternalError(Error_Operation_Invalid, "One versions needs to be provided for each key", "NumKeys", len(keys), "NumVersions", len(versions))
	}

	for i, key := range keys {
		if !key.Versioned {
			return newInternalError(Error_Operation_Invalid, "Version operation with unversioned key not possible")
		}
		entry, err := self.rntm.datastore.GetVersionedEntry(key)
		if err != nil {
			return utils.StackError(err, "Unable to get entry by key in datastore", "Key", key)
		}
		err = entry.RemoveVersionsUpFrom(versions[i])
		if err != nil {
			return utils.StackError(err, "Unable to remove versions for key", "Key", key, "UpFrom", versions[i])
		}
	}
	return nil
}

func (self *object) GetDBValue(id Identifier, key []byte) (datastore.Value, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ValueType, false, id.Hash())
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to access %s in database", id)
	}
	vset, done := set.(*datastore.ValueSet)
	if !done {
		return datastore.Value{}, newInternalError(Error_Fatal, "Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to access or create %s in DB", string(key))
	}
	return *value, nil
}

func (self *object) GetDBValueVersioned(id Identifier, key []byte) (datastore.ValueVersioned, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ValueType, true, id.Hash())
	if err != nil {
		return datastore.ValueVersioned{}, utils.StackError(err, "Unable to access or create %s in database", id)
	}
	vset, done := set.(*datastore.ValueVersionedSet)
	if !done {
		return datastore.ValueVersioned{}, newInternalError(Error_Fatal, "Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.ValueVersioned{}, utils.StackError(err, "Unable to access or create %s in DB", string(key))
	}
	return *value, nil
}

func (self *object) GetDBMap(id Identifier, key []byte) (datastore.Map, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.MapType, false, id.Hash())
	if err != nil {
		return datastore.Map{}, utils.StackError(err, "Unable to access or create %s in database", id)
	}
	mset, done := set.(*datastore.MapSet)
	if !done {
		return datastore.Map{}, newInternalError(Error_Fatal, "Database access failed: wrong set returned")
	}
	map_, err := mset.GetOrCreateMap(key)
	if err != nil {
		return datastore.Map{}, utils.StackError(err, "Unable to access or create %s in DB", string(key))
	}
	return *map_, nil
}

func (self *object) GetDBMapVersioned(id Identifier, key []byte) (datastore.MapVersioned, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.MapType, true, id.Hash())
	if err != nil {
		return datastore.MapVersioned{}, utils.StackError(err, "Unable to access or create %s in database", id)
	}
	mset, done := set.(*datastore.MapVersionedSet)
	if !done {
		return datastore.MapVersioned{}, newInternalError(Error_Fatal, "Database access failed: wrong set returned")
	}
	map_, err := mset.GetOrCreateMap(key)
	if err != nil {
		return datastore.MapVersioned{}, utils.StackError(err, "Unable to access or create %s in DB", string(key))
	}
	return *map_, nil
}

func (self *object) GetDBList(id Identifier, key []byte) (datastore.List, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ListType, false, id.Hash())
	if err != nil {
		return datastore.List{}, utils.StackError(err, "Unable to access or create %s in database", id)
	}
	lset, done := set.(*datastore.ListSet)
	if !done {
		return datastore.List{}, newInternalError(Error_Fatal, "Database access failed: wrong set returned")
	}
	list, err := lset.GetOrCreateList(key)
	if err != nil {
		return datastore.List{}, utils.StackError(err, "Unable to access or create %s in DB", string(key))
	}
	return *list, nil
}

func (self *object) GetDBListVersioned(id Identifier, key []byte) (datastore.ListVersioned, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ListType, true, id.Hash())
	if err != nil {
		return datastore.ListVersioned{}, utils.StackError(err, "Unable to access or create %s in database", id)
	}
	lset, done := set.(*datastore.ListVersionedSet)
	if !done {
		return datastore.ListVersioned{}, newInternalError(Error_Fatal, "Database access failed: wrong set returned")
	}
	list, err := lset.GetOrCreateList(key)
	if err != nil {
		return datastore.ListVersioned{}, utils.StackError(err, "Unable to access or create %s in DB", string(key))
	}
	return *list, nil
}
