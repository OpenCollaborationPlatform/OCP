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
	"strconv"

	"github.com/ickby/CollaborationNode/utils"
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
	mapI.AddProperty("key", MustNewDataType("type"), MustNewDataType("none"), true)
	mapI.AddProperty("value", MustNewDataType("type"), MustNewDataType("none"), true)

	//add methods
	mapI.AddMethod("Length", MustNewMethod(mapI.Length, true))
	mapI.AddMethod("Get", MustNewMethod(mapI.Get, true))
	mapI.AddMethod("Set", MustNewMethod(mapI.Set, false))
	mapI.AddMethod("Has", MustNewMethod(mapI.Has, true))
	mapI.AddMethod("Keys", MustNewMethod(mapI.Keys, true))
	mapI.AddMethod("New", MustNewMethod(mapI.New, false))
	mapI.AddMethod("Remove", MustNewMethod(mapI.Remove, false))

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
			return nil, fmt.Errorf("complex db entry needs to be identifier")
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
		return -1, err
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
		return nil, err
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
		return nil, fmt.Errorf("Key is not available in Map")
	}

	//check if the type of the value is correct
	dt := self.valueDataType(id)
	res, err := dbEntries.Read(dbkey)
	if err != nil {
		return nil, utils.StackError(err, "Cannot access db")
	}

	return self.dbToType(res, dt)
}

func (self *mapImpl) Set(id Identifier, key interface{}, value interface{}) error {

	//check key type
	kdt := self.keyDataType(id)
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Cannot set map entry, key has wrong type")
	}

	//check if the type of the value is correct
	dt := self.valueDataType(id)
	err = dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot set map entry, value has wrong type")
	}

	//check for complex, we do no set those (keep hirarchy)
	if dt.IsComplex() {
		return fmt.Errorf("Complex datatypes cannot be set, use New")

	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
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
				return utils.StackError(err, "Cannot set map entry")
			}

		} else if ident, ok := value.(Identifier); ok {
			err := dbEntries.Write(dbkey, ident)
			if err != nil {
				return utils.StackError(err, "Cannot set map entry")
			}

		} else {
			return fmt.Errorf("Complex types need a identifier or dmlSet for set function")
		}

	} else if dt.IsType() {

		val, _ := value.(DataType)
		err := dbEntries.Write(dbkey, val.AsString())
		if err != nil {
			return utils.StackError(err, "Cannot set map entry")
		}

	} else {
		//plain types remain
		err := dbEntries.Write(dbkey, value)
		if err != nil {
			return utils.StackError(err, "Cannot set map entry")
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
		return nil, utils.StackError(err, "Cannot create new map value, key has wrong type")
	}

	//if we already have it we cannot create new!
	if has, _ := self.Has(id, key); has {
		return nil, fmt.Errorf("Key exists already, cannot create new object")
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return nil, err
	}

	//create a new entry
	var result interface{}
	dt := self.valueDataType(id)
	if dt.IsComplex() {
		set, err := self.rntm.constructObjectSet(dt, id)
		if err != nil {
			return nil, utils.StackError(err, "Unable to append new object to mapImpl: construction failed")
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
		return utils.StackError(err, "Cannot create new map value, key has wrong type")
	}

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}

	//if we don't have it we cannot remove it!
	dbkey := self.typeToDB(key, kdt)
	if !dbEntries.HasKey(dbkey) {
		return fmt.Errorf("Key does not exist (%v), cannot be removed", key)
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	//delete the key
	err = dbEntries.Remove(dbkey)
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(id) //do not return error as setting was already successfull
	return nil
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *mapImpl) GetSubobjects(id Identifier, bhvr bool) ([]dmlSet, error) {

	//get default objects
	res, err := self.DataImpl.GetSubobjects(id, bhvr)
	if err != nil {
		return nil, err
	}

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return nil, err
	}

	//handle key objects!
	dt := self.keyDataType(id)
	if dt.IsComplex() {

		keys, err := dbEntries.GetKeys()
		if err != nil {
			return nil, err
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

	//handle value objects!
	dt = self.valueDataType(id)
	if dt.IsComplex() {

		keys, err := dbEntries.GetKeys()
		if err != nil {
			return nil, err
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

func (self *mapImpl) GetSubobjectByName(id Identifier, name string, bhvr bool) (dmlSet, error) {

	//default search
	set, err := self.DataImpl.GetSubobjectByName(id, name, bhvr)
	if err == nil {
		return set, nil
	}

	//let's see if it is a map key
	var key interface{}
	dt := self.keyDataType(id)
	switch dt.AsString() {
	case "int":
		i, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			return dmlSet{}, fmt.Errorf("No such key available")
		}
		key = self.typeToDB(i, dt)

	case "string":
		key = self.typeToDB(name, dt)

	default:
		return dmlSet{}, fmt.Errorf("Map key type %v does no allow access with %v", dt.AsString(), name)
	}

	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return dmlSet{}, err
	}

	if !dbEntries.HasKey(key) {
		return dmlSet{}, fmt.Errorf("No such key available")
	}
	val, err := dbEntries.Read(key)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to read key")
	}
	dt = self.valueDataType(id)
	result, _ := self.dbToType(val, dt)
	if dt.IsComplex() {

		return result.(dmlSet), nil
	}

	return dmlSet{}, fmt.Errorf("%v is not a subobject", name)
}

func (self *mapImpl) GetValueByName(id Identifier, name string) (interface{}, error) {

	//let's see if it is a valid key
	var key interface{}
	dt := self.keyDataType(id)
	switch dt.AsString() {
	case "int":
		i, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			return nil, err
		}
		key = self.typeToDB(i, dt)

	case "string":
		key = self.typeToDB(name, dt)

	default:
		return nil, fmt.Errorf("Only int and string keys are accessible by name")
	}

	res, err := self.Get(id, key)
	if err != nil {
		return nil, err
	}

	return res, nil
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
			return err
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
				return err
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

	prop := self.GetProperty("value").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}

func (self *mapImpl) keyDataType(id Identifier) DataType {

	prop := self.GetProperty("key").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}
