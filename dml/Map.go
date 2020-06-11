// Vector
package dml

import (
	"fmt"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
	"strconv"
)

//map type: stores requested data type by index (0-based)
type mapImpl struct {
	*DataImpl

	entries *datastore.MapVersioned
}

func NewMap(id Identifier, parent Identifier, rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(id, parent, rntm)
	if err != nil {
		return nil, err
	}

	//get the db entries
	set, _ := base.GetDatabaseSet(datastore.MapType)
	mapSet := set.(*datastore.MapVersionedSet)
	entries, _ := mapSet.GetOrCreateMap([]byte("__map_entries"))

	//build the mapImpl
	mapI := &mapImpl{
		base,
		entries,
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

		encoded, _ := key.(string)
		id, err := IdentifierFromEncoded(encoded)
		if err != nil {
			return nil, utils.StackError(err, "Invalid identifier stored")
		}
		obj, has := self.rntm.objects[id]
		if !has {
			return nil, utils.StackError(err, "Invalid object stored")
		}
		return obj, nil

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

		obj, _ := key.(Object)
		return obj.Id().Encode()

	} else if dt.IsType() {

		val, _ := key.(DataType)
		return val.AsString()
	}

	//numeric types should be unified!
	switch key.(type) {
	case int:
		key = int64(key.(int))
	case int8:
		key = int64(key.(int8))
	case int16:
		key = int64(key.(int16))
	case int32:
		key = int64(key.(int32))
	case uint:
		key = uint64(key.(uint))
	case uint8:
		key = uint64(key.(uint8))
	case uint16:
		key = uint64(key.(uint16))
	case uint32:
		key = uint64(key.(uint32))
	case float32:
		key = float64(key.(float32))
	}

	//everything else is simply used as key
	return key

}

func (self *mapImpl) Length() (int64, error) {

	keys, err := self.entries.GetKeys()
	if err != nil {
		return -1, err
	}
	return int64(len(keys)), nil
}

func (self *mapImpl) Keys() ([]interface{}, error) {

	kdt := self.keyDataType()
	keys, err := self.entries.GetKeys()
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

func (self *mapImpl) Has(key interface{}) (bool, error) {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return false, utils.StackError(err, "Key has wrong type")
	}

	dbkey := self.typeToDB(key, kdt)
	return self.entries.HasKey(dbkey), nil
}

//implement DynamicData interface
func (self *mapImpl) Load() error {

	//keys: we only need to load when we store objects
	dt := self.keyDataType()
	if dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to load mapImpl entries: Keys cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to load mapImpl: Stored identifier is invalid")
			}

			obj, err := LoadObject(self.rntm, dt, id, self.Id())
			if err != nil {
				return utils.StackError(err, "Unable to load object for mapImpl: construction failed")
			}
			self.rntm.objects[id] = obj.(Data)
		}
	}

	//values: we only need to load when we store objects
	dt = self.valueDataType()
	if dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to load mapImpl entries: Keys cannot be accessed")
		}
		for _, key := range keys {

			res, err := self.entries.Read(key)
			if err == nil {
				id, err := IdentifierFromEncoded(res.(string))
				if err != nil {
					return utils.StackError(err, "Unable to load mapImpl: Stored identifier is invalid")
				}
				obj, err := LoadObject(self.rntm, dt, id, self.Id())
				if err != nil {
					return utils.StackError(err, "Unable to load object for mapImpl: construction failed")
				}
				self.rntm.objects[id] = obj.(Data)

			} else {
				return utils.StackError(err, "Unable to load mapImpl entries: entry cannot be read")
			}
		}
	}
	return nil
}

func (self *mapImpl) Get(key interface{}) (interface{}, error) {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return nil, utils.StackError(err, "Key has wrong type")
	}

	//check if key is availbale
	dbkey := self.typeToDB(key, kdt)
	if !self.entries.HasKey(dbkey) {
		return nil, fmt.Errorf("Key is not available in Map")
	}

	//check if the type of the value is correct
	dt := self.valueDataType()
	res, err := self.entries.Read(dbkey)
	if err != nil {
		return nil, utils.StackError(err, "Cannot access db")
	}

	return self.dbToType(res, dt)
}

func (self *mapImpl) Set(key interface{}, value interface{}) error {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Cannot set map entry, key has wrong type")
	}

	//check if the type of the value is correct
	dt := self.valueDataType()
	err = dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot set map entry, value has wrong type")
	}

	//check for complex, we do no set those (keep hirarchy)
	if dt.IsComplex() {
		return fmt.Errorf("Complex datatypes cannot be set, use New")

	}
	
	//event handling
	err = self.GetEvent("onBeforeChange").Emit()
	if err != nil { 
		return err
	}

	err = self.set(key, value)
	if err != nil { 
		return err
	}
	
	self.GetEvent("onChanged").Emit() //do not return error as setting was already successfull
	return nil
}

//internal set: careful, no checks!
func (self *mapImpl) set(key interface{}, value interface{}) error {

	dt := self.valueDataType()
	dbkey := self.typeToDB(key, self.keyDataType())

	if dt.IsComplex() {

		obj, _ := value.(Object)
		err := self.entries.Write(dbkey, obj.Id().Encode())
		if err != nil {
			return utils.StackError(err, "Cannot set map entry")
		}

	} else if dt.IsType() {

		val, _ := value.(DataType)
		err := self.entries.Write(dbkey, val.AsString())
		if err != nil {
			return utils.StackError(err, "Cannot set map entry")
		}

	} else {
		//plain types remain
		err := self.entries.Write(dbkey, value)
		if err != nil {
			return utils.StackError(err, "Cannot set map entry")
		}
	}

	return nil
}

//creates a new entry with a all new type, returns the index of the new one
func (self *mapImpl) New(key interface{}) (interface{}, error) {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create new map value, key has wrong type")
	}

	//if we already have it we cannot create new!
	dbkey := self.typeToDB(key, kdt)
	if self.entries.HasKey(dbkey) {
		return nil, fmt.Errorf("Key exists already, cannot create new object")
	}
	
	//event handling
	err = self.GetEvent("onBeforeChange").Emit()
	if err != nil { 
		return nil, err
	}

	//create a new entry (with correct refcount if needed)
	var result interface{}
	dt := self.valueDataType()
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "", self.Id())
		if err != nil {
			return nil, utils.StackError(err, "Unable to append new object to mapImpl: construction failed")
		}
		result = obj

	} else {
		result = dt.GetDefaultValue()
	}

	//write new entry
	err = self.set(key, result)
	if err != nil { 
		return nil, err
	}
	
	self.GetEvent("onChanged").Emit() //do not return error as setting was already successfull
	return result, nil
}

//remove a entry from the mapImpl
func (self *mapImpl) Remove(key interface{}) error {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Cannot create new map value, key has wrong type")
	}

	//if we don't have it we cannot remove it!
	dbkey := self.typeToDB(key, kdt)
	if !self.entries.HasKey(dbkey) {
		return fmt.Errorf("Key does not exist (%v), cannot be removed", key)
	}
	
	//event handling
	err = self.GetEvent("onBeforeChange").Emit()
	if err != nil { 
		return err
	}

	dt := self.valueDataType()
	if dt.IsComplex() {
		//we have a object stored, hence delete must remove it completely!
		val, err := self.Get(key)
		if err != nil {
			return utils.StackError(err, "Unable to delete entry")
		}
		obj, ok := val.(Object)
		if ok {
			self.rntm.removeObject(obj)
		}
	}

	//delete the key
	err = self.entries.Remove(dbkey)
	if err != nil { 
		return err
	}
	
	self.GetEvent("onChanged").Emit() //do not return error as setting was already successfull
	return nil
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *mapImpl) GetSubobjects(bhvr bool) []Object {

	//get default objects
	res := self.DataImpl.GetSubobjects(bhvr)

	//handle key objects!
	dt := self.keyDataType()
	if dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return res
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err == nil {
				existing, ok := self.rntm.objects[id]
				if ok {
					res = append(res, existing)
				}
			}
		}
	}

	//handle value objects!
	dt = self.valueDataType()
	if dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return res
		}
		for _, key := range keys {

			read, err := self.entries.Read(key)
			if err == nil {
				id, err := IdentifierFromEncoded(read.(string))
				if err == nil {
					existing, ok := self.rntm.objects[id]
					if ok {
						res = append(res, existing)
					}
				}
			}
		}
	}

	return res
}

func (self *mapImpl) GetSubobjectByName(name string, bhvr bool) (Object, error) {

	//default search
	obj, err := self.DataImpl.GetSubobjectByName(name, bhvr)
	if err == nil {
		return obj, nil
	}

	//let's see if it is a map key
	var key interface{}
	dt := self.keyDataType()
	switch dt.AsString() {
	case "int":
		i, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("No such key available")
		}
		key = self.typeToDB(i, dt)

	case "string":
		key = self.typeToDB(name, dt)

	default:
		return nil, fmt.Errorf("Map key type %v does no allow access with %v", dt.AsString(), name)
	}

	if !self.entries.HasKey(key) {
		return nil, fmt.Errorf("No such key available")
	}
	val, err := self.entries.Read(key)
	if err != nil {
		return nil, utils.StackError(err, "Unable to read key")
	}
	dt = self.valueDataType()
	result, _ := self.dbToType(val, dt)
	if dt.IsComplex() {

		return result.(Object), nil
	}

	return nil, fmt.Errorf("%v is not a subobject", name)
}

func (self *mapImpl) GetValueByName(name string) interface{} {

	//let's see if it is a valid key
	var key interface{}
	dt := self.keyDataType()
	switch dt.AsString() {
	case "int":
		i, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			return nil
		}
		key = self.typeToDB(i, dt)

	case "string":
		key = self.typeToDB(name, dt)

	default:
		return nil
	}

	res, err := self.Get(key)
	if err != nil {
		return nil
	}

	return res
}

func (self *mapImpl) valueDataType() DataType {

	prop := self.GetProperty("value").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}

func (self *mapImpl) keyDataType() DataType {

	prop := self.GetProperty("key").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}
