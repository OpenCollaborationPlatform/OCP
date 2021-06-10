// Map
package dml

/*
 A Map is a standart datatype as avilable in all languages, sometimes nown as Dictionary.

 - Keys:		Allowed keys are all Datatype except "type" (as for this no reasonable
			Path can be build) and "complex" (as those cannot be created as key)
 - Values:	All datatypes are allowed as values
 - Paths: 	A map exposes the stored values in the DML path. For example a string->obj map
			exposes the object as MyMap.MyKey.
*/

import (
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"
)

var mapKey = []byte("__map_entries")

//map type: stores requested data type by index (0-based)
type mapImpl struct {
	*DataImpl

	//entries *datastore.MapVersioned
}

func NewMap(rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(rntm)
	if err != nil {
		return nil, err
	}

	//build the mapImpl
	mapI := &mapImpl{
		base,
	}

	//add properties
	mapI.AddProperty("key", MustNewDataType("key"), MustNewDataType("string"), true)
	mapI.AddProperty("value", MustNewDataType("type"), MustNewDataType("none"), true)

	//add methods
	mapI.AddMethod("Length", MustNewIdMethod(mapI.Length, true))
	mapI.AddMethod("Get", MustNewIdMethod(mapI.Get, true))
	mapI.AddMethod("Set", MustNewIdMethod(mapI.Set, false))
	mapI.AddMethod("Has", MustNewIdMethod(mapI.Has, true))
	mapI.AddMethod("Keys", MustNewIdMethod(mapI.Keys, true))
	mapI.AddMethod("New", MustNewIdMethod(mapI.New, false))
	mapI.AddMethod("Remove", MustNewIdMethod(mapI.Remove, false))

	//events of a mapImpl
	//mapI.AddEvent("onNewEntry", NewEvent(mapI.GetJSObject(), rntm.jsvm, MustNewDataType("int")))
	//mapI.AddEvent("onChangedEntry", NewEvent(mapI.GetJSObject(), rntm.jsvm, MustNewDataType("int")))
	//mapI.AddEvent("onRemovedEntry", NewEvent(mapI.GetJSObject(), rntm.jsvm, MustNewDataType("int")))

	return mapI, nil
}

//inverse of keyToDB
func (self *mapImpl) dbToType(key interface{}, dt DataType) (interface{}, error) {

	if dt.IsComplex() {

		id, ok := key.(*Identifier)
		if !ok {
			return nil, newInternalError(Error_Fatal, "Complex datatype, but key is not identifier")
		}
		set, err := self.rntm.getObjectSet(*id)
		if err != nil {
			return nil, err
		}
		return set, nil

	} else if dt.IsType() {

		val, _ := key.(string)
		return NewDataType(val)
	}

	//everything else is simply used
	return key, nil
}

//convert all possible key types to something usable in the DB
func (self *mapImpl) typeToDB(key interface{}, dt DataType) interface{} {

	if dt.IsComplex() {
		if set, ok := key.(dmlSet); ok {
			return set.id

		} else if id, ok := key.(Identifier); ok {
			return id
		}

	} else if dt.IsType() {

		val, _ := key.(DataType)
		return val.AsString()
	}

	//everything else is simply used as key (but unified)
	return UnifyDataType(key)

}

func (self *mapImpl) Length(id Identifier) (int64, error) {

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return -1, err
	}

	keys, err := dbEntries.GetKeys()
	if err != nil {
		return -1, utils.StackError(err, "Unable to read keys from DB")
	}
	return int64(len(keys)), nil
}

func (self *mapImpl) Keys(id Identifier) ([]interface{}, error) {

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return nil, err
	}

	kdt := self.keyDataType(id)
	keys, err := dbEntries.GetKeys()
	if err != nil {
		return nil, utils.StackError(err, "Unable to read keys from DB")
	}
	result := make([]interface{}, len(keys))
	for i, key := range keys {
		kdb, err := self.dbToType(key, kdt)
		if err != nil {
			return nil, err
		}
		result[i] = kdb
	}
	return result, nil
}

func (self *mapImpl) Has(id Identifier, key interface{}) (bool, error) {

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return false, err
	}

	//check key type
	kdt := self.keyDataType(id)
	err = kdt.MustBeTypeOf(key)
	if err != nil {
		return false, utils.StackError(err, "Key has wrong type")
	}

	dbkey := self.typeToDB(key, kdt)
	return dbEntries.HasKey(dbkey), nil
}

func (self *mapImpl) Get(id Identifier, key interface{}) (interface{}, error) {

	//check key type
	kdt := self.keyDataType(id)
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return nil, utils.StackError(err, "Key has wrong type")
	}

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return -1, err
	}

	//check if key is availbale
	dbkey := self.typeToDB(key, kdt)
	if !dbEntries.HasKey(dbkey) {
		return nil, newUserError(Error_Key_Not_Available, "Key is not available in Map")
	}

	//check if the type of the value is correct
	dt := self.valueDataType(id)
	res, err := dbEntries.Read(dbkey)
	if err != nil {
		return nil, utils.StackError(err, "Failed to read entry in DB")
	}

	return self.dbToType(res, dt)
}

func (self *mapImpl) Set(id Identifier, key interface{}, value interface{}) error {

	//check key type
	kdt := self.keyDataType(id)
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Key has wrong type")
	}

	//check if the type of the value is correct
	dt := self.valueDataType(id)
	err = dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Value has wrong type")
	}

	//check for complex, we do no set those (keep hirarchy)
	if dt.IsComplex() {
		return newUserError(Error_Operation_Invalid, "Complex datatypes cannot be set, use New")

	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return utils.StackError(err, "Abort operation due to event onBeforeChange error")
	}

	err = self.set(id, key, value)
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(id) //do not return error as setting was already successfull
	return nil
}

//internal set: careful, no checks!
func (self *mapImpl) set(id Identifier, key interface{}, value interface{}) error {

	dt := self.valueDataType(id)
	dbkey := self.typeToDB(key, self.keyDataType(id))

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}

	if dt.IsComplex() {

		if set, ok := value.(dmlSet); ok {

			err := dbEntries.Write(dbkey, set.id)
			if err != nil {
				return utils.StackError(err, "Unable to write entry into DB")
			}

		} else if ident, ok := value.(Identifier); ok {
			err := dbEntries.Write(dbkey, ident)
			if err != nil {
				return utils.StackError(err, "Unable to write entry into DB")
			}

		} else {
			return newInternalError(Error_Fatal, "Complex types need a identifier or dmlSet for set function")
		}

	} else if dt.IsType() {

		val, _ := value.(DataType)
		err := dbEntries.Write(dbkey, val.AsString())
		if err != nil {
			return utils.StackError(err, "Unable to write entry into DB")
		}

	} else {
		//plain types remain
		err := dbEntries.Write(dbkey, value)
		if err != nil {
			return utils.StackError(err, "Unable to write entry into DB")
		}
	}

	return nil
}

//creates a new entry with a all new type, returns the index of the new one
func (self *mapImpl) New(id Identifier, key interface{}) (interface{}, error) {

	//check key type
	kdt := self.keyDataType(id)
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return nil, utils.StackError(err, "Key has wrong type")
	}

	//if we already have it we cannot create new!
	if has, _ := self.Has(id, key); has {
		return nil, newUserError(Error_Operation_Invalid, "Key already exists")
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return nil, utils.StackError(err, "Abort operation due to event onBeforeChange error")
	}

	//create a new entry
	var result interface{}
	dt := self.valueDataType(id)
	if dt.IsComplex() {
		set, err := self.rntm.constructObjectSet(dt, id)
		if err != nil {
			return nil, utils.StackError(err, "Construction of new object failed")
		}
		//build the object path and set it
		path, err := self.GetObjectPath(id)
		if err != nil {
			return nil, err
		}
		if subID, ok := key.(*Identifier); ok {
			path += "." + subID.Encode()
		} else {
			path += fmt.Sprintf(".%v", key)
		}
		set.obj.SetObjectPath(set.id, path)

		result = set

	} else {
		result = dt.GetDefaultValue()
	}

	//write new entry
	err = self.set(id, key, result)
	if err != nil {
		return nil, err
	}

	if dt.IsComplex() {
		set := result.(dmlSet)
		if data, ok := set.obj.(Data); ok {
			data.Created(result.(dmlSet).id)
		}
	}

	self.GetEvent("onChanged").Emit(id) //do not return error as setting was already successfull
	return result, nil
}

//remove a entry from the mapImpl
func (self *mapImpl) Remove(id Identifier, key interface{}) error {

	//check key type
	kdt := self.keyDataType(id)
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Key has wrong type")
	}

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}

	//if we don't have it we cannot remove it!
	dbkey := self.typeToDB(key, kdt)
	if !dbEntries.HasKey(dbkey) {
		return newUserError(Error_Key_Not_Available, "Key does not exist", key)
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return utils.StackError(err, "Abort operation due to event onBeforeChange error")
	}

	//delete the key
	err = dbEntries.Remove(dbkey)
	if err != nil {
		return utils.StackError(err, "Unable to remove entry from DB")
	}

	self.GetEvent("onChanged").Emit(id) //do not return error as setting was already successfull
	return nil
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *mapImpl) GetSubobjects(id Identifier) ([]dmlSet, error) {

	//get default objects
	res, err := self.DataImpl.GetSubobjects(id)
	if err != nil {
		return nil, err
	}

	//handle value objects! (Keys cannot be objects)
	dt := self.valueDataType(id)
	if dt.IsComplex() {

		dbEntries, err := self.GetDBMapVersioned(id, entryKey)
		if err != nil {
			return nil, err
		}

		keys, err := dbEntries.GetKeys()
		if err != nil {
			return nil, utils.StackError(err, "Unable to access keys in DB")
		}
		for _, key := range keys {

			id, ok := key.(*Identifier)
			if ok {
				set, err := self.rntm.getObjectSet(*id)
				if err != nil {
					return nil, err
				}
				res = append(res, set)
			}
		}
	}

	return res, nil
}

//Key handling for generic access to Data
func (self *mapImpl) GetByKey(id Identifier, key Key) (interface{}, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return self.DataImpl.GetByKey(id, key)
	}

	dtKey, err := key.AsDataType(self.keyDataType(id))
	if err != nil {
		return nil, err
	}
	return self.Get(id, dtKey)
}

func (self *mapImpl) HasKey(id Identifier, key Key) (bool, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return true, nil
	}

	dtKey, err := key.AsDataType(self.keyDataType(id))
	if err != nil {
		return false, err
	}
	return self.Has(id, dtKey)
}

func (self *mapImpl) GetKeys(id Identifier) ([]Key, error) {

	keys, err := self.DataImpl.GetKeys(id)
	if err != nil {
		return nil, err
	}

	mapkeys, err := self.Keys(id)
	if err != nil {
		return nil, err
	}

	for _, mapkey := range mapkeys {
		keys = append(keys, MustNewKey(mapkey))
	}
	return keys, nil
}

func (self *mapImpl) keyRemoved(Identifier, Key) error {

	//TODO: check if key is object and remove it accordingly
	return nil
}

func (self *mapImpl) keyToDS(id Identifier, key Key) ([]datastore.Key, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return self.DataImpl.keyToDS(id, key)
	}

	dtKey, err := key.AsDataType(self.keyDataType(id))
	if err != nil {
		return nil, utils.StackError(err, "Unable to use key for map entries")
	}
	if has, _ := self.Has(id, dtKey); has {
		dbKeyType := self.typeToDB(dtKey, self.keyDataType(id))
		return []datastore.Key{datastore.NewKey(datastore.MapType, true, id.Hash(), entryKey, dbKeyType)}, nil
	}

	return nil, newInternalError(Error_Key_Not_Available, "Key does not exist in map")
}

func (self *mapImpl) SetObjectPath(id Identifier, path string) error {

	//ourself and children
	err := self.DataImpl.SetObjectPath(id, path)
	if err != nil {
		return err
	}

	//now we need to set all map objects, if available
	valDt := self.valueDataType(id)
	if valDt.IsComplex() {

		dbEntries, err := self.GetDBMapVersioned(id, entryKey)
		if err != nil {
			return err
		}

		keys, err := dbEntries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to read keys from DB")
		}
		for _, key := range keys {

			//build the path
			fullpath := path
			if subID, ok := key.(*Identifier); ok {
				fullpath += "." + subID.Encode()

			} else {
				fullpath += fmt.Sprintf(".%v", key)
			}

			//get the object and set the path
			val, err := dbEntries.Read(key)
			if err != nil {
				return utils.StackError(err, "Unable to read key from DB")
			}
			objId, ok := val.(*Identifier)
			if ok {
				set, err := self.rntm.getObjectSet(*objId)
				if err != nil {
					return err
				}
				set.obj.SetObjectPath(set.id, fullpath)
			}
		}
	}

	return nil
}

func (self *mapImpl) valueDataType(id Identifier) DataType {

	prop := self.GetProperty("value").GetValue(Identifier{})
	return prop.(DataType)
}

func (self *mapImpl) keyDataType(id Identifier) DataType {

	prop := self.GetProperty("key").GetValue(Identifier{})
	return prop.(DataType)
}
