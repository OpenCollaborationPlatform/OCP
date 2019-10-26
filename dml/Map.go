// Vector
package dml

import (
	"fmt"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
)

//map type: stores requested data type by index (0-based)
type mapImpl struct {
	*DataImpl

	entries *datastore.MapVersioned
}

func NewMap(id Identifier, parent Identifier, rntm *Runtime) (Object, error) {

	base := NewDataBaseClass(id, parent, rntm)

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
	mapI.AddMethod("Length", MustNewMethod(mapI.Length))
	mapI.AddMethod("Get", MustNewMethod(mapI.Get))
	mapI.AddMethod("Set", MustNewMethod(mapI.Set))
	mapI.AddMethod("Has", MustNewMethod(mapI.Has))
	mapI.AddMethod("Keys", MustNewMethod(mapI.Keys))
	mapI.AddMethod("New", MustNewMethod(mapI.New))
	mapI.AddMethod("Remove", MustNewMethod(mapI.Remove))

	//events of a mapImpl
	//mapI.AddEvent("onNewEntry", NewEvent(mapI.GetJSObject(), rntm.jsvm, MustNewDataType("int")))
	//mapI.AddEvent("onChangedEntry", NewEvent(mapI.GetJSObject(), rntm.jsvm, MustNewDataType("int")))
	//mapI.AddEvent("onRemovedEntry", NewEvent(mapI.GetJSObject(), rntm.jsvm, MustNewDataType("int")))

	return mapI, nil
}

//convert all possible key types to something usable in the DB
func (self *mapImpl) keyToDB(key interface{}) interface{} {
	dt := self.keyDataType()

	if dt.IsObject() || dt.IsComplex() {

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

//inverse of keyToDB
func (self *mapImpl) dbToKey(key interface{}) (interface{}, error) {
	dt := self.keyDataType()

	if dt.IsObject() || dt.IsComplex() {

		encoded, _ := key.(string)
		return IdentifierFromEncoded(encoded)

	} else if dt.IsType() {

		val, _ := key.(string)
		return NewDataType(val)
	}

	//everything else is simply used as key
	return key, nil
}

func (self *mapImpl) Length() (int64, error) {

	keys, err := self.entries.GetKeys()
	if err != nil {
		return -1, err
	}
	return int64(len(keys)), nil
}

func (self *mapImpl) Keys() ([]interface{}, error) {

	keys, err := self.entries.GetKeys()
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(keys))
	for i, key := range keys {
		kdb, err := self.dbToKey(key)
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

	dbkey := self.keyToDB(key)
	return self.entries.HasKey(dbkey), nil
}

//implement DynamicData interface
func (self *mapImpl) Load() error {

	//keys: we only need to load when we store objects
	dt := self.keyDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to load mapImpl entries: Keys cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to load mapImpl: Stored identifier is invalid")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {

				//we made sure the object does not exist. We need to load it
				obj, err := LoadObject(self.rntm, dt, id)
				if err != nil {
					return utils.StackError(err, "Unable to load object for mapImpl: construction failed")
				}
				obj.IncreaseRefcount()
				self.rntm.objects[id] = obj.(Data)
			} else {
				existing.IncreaseRefcount()
			}
		}
	}

	//values: we only need to load when we store objects
	dt = self.valueDataType()
	if dt.IsObject() || dt.IsComplex() {

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
				existing, ok := self.rntm.objects[id]
				if !ok {

					//we made sure the object does not exist. We need to load it
					obj, err := LoadObject(self.rntm, dt, id)
					if err != nil {
						return utils.StackError(err, "Unable to load object for mapImpl: construction failed")
					}
					obj.IncreaseRefcount()
					self.rntm.objects[id] = obj.(Data)
				} else {
					existing.IncreaseRefcount()
				}
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
	dbkey := self.keyToDB(key)
	if !self.entries.HasKey(dbkey) {
		return nil, fmt.Errorf("Key is not available in Map")
	}

	//check if the type of the value is correct
	dt := self.valueDataType()
	var result interface{}

	if dt.IsObject() || dt.IsComplex() {
		res, err := self.entries.Read(dbkey)
		if err == nil {
			id, err := IdentifierFromEncoded(res.(string))
			if err != nil {
				return nil, utils.StackError(err, "Invalid identifier stored in DB")
			} else {
				res, ok := self.rntm.objects[id]
				if !ok {
					return nil, fmt.Errorf("Map entry is invalid object")
				}
				result = res
			}
		}

	} else if dt.IsType() {

		res, err := self.entries.Read(dbkey)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get stored type")
		}
		result, err = NewDataType(res.(string))
		if err != nil {
			return nil, utils.StackError(err, "Invalid datatype stored")
		}

	} else {
		//plain types remain
		result, err = self.entries.Read(dbkey)
		if err != nil {
			return nil, utils.StackError(err, "Unable to access database of Map")
		}
	}

	return result, nil
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

	dbkey := self.keyToDB(key)
	if dt.IsObject() || dt.IsComplex() {

		old, olderr := self.Get(dbkey)
		obj, _ := value.(Object)
		err := self.entries.Write(dbkey, obj.Id().Encode())
		if err != nil {
			return utils.StackError(err, "Cannot set map entry")
		}

		//handle ref counts
		data, ok := value.(Data)
		if ok {
			data.IncreaseRefcount()
		}
		if olderr == nil {
			data, ok = old.(Data)
			if ok {
				data.DecreaseRefcount()
			}
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
	dbkey := self.keyToDB(key)
	if self.entries.HasKey(dbkey) {
		return nil, fmt.Errorf("Key exists already, cannot create new object")
	}

	//create a new entry (with correct refcount if needed)
	var result interface{}
	dt := self.valueDataType()
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "")
		if err != nil {
			return nil, utils.StackError(err, "Unable to append new object to mapImpl: construction failed")
		}
		obj.IncreaseRefcount()
		result = obj

	} else {
		result = dt.GetDefaultValue()
	}

	//write new entry
	return result, self.Set(key, result)
}

//remove a entry from the mapImpl
func (self *mapImpl) Remove(key interface{}) error {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Cannot create new map value, key has wrong type")
	}

	//if we already have it we cannot create new!
	dbkey := self.keyToDB(key)
	if !self.entries.HasKey(dbkey) {
		return fmt.Errorf("Key does not exist, cannot be removed")
	}

	//decrease refcount if required
	dt := self.valueDataType()
	if dt.IsComplex() || dt.IsObject() {
		//we have a object stored, hence delete must remove it completely!
		val, err := self.Get(key)
		if err != nil {
			return utils.StackError(err, "Unable to delete entry")
		}
		data, ok := val.(Data)
		if ok {
			data.DecreaseRefcount()
		}
	}

	//and delete the old key
	return self.entries.Remove(dbkey)
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

//override to handle children refcount additional to our own
func (self *mapImpl) IncreaseRefcount() error {

	//handle keys!
	dt := self.keyDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase map refcount: Keys cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to increase map refcount: Invalid child identifier stored")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {
				return fmt.Errorf("Unable to increase map refcount: Invalid child stored")
			}
			existing.IncreaseRefcount()
		}
	}

	//handle values!
	dt = self.valueDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase map refcount: Keys cannot be accessed")
		}
		for _, key := range keys {

			res, err := self.entries.Read(key)
			if err == nil {
				id, err := IdentifierFromEncoded(res.(string))
				if err != nil {
					return utils.StackError(err, "Unable to increase map refcount: Invalid child identifier stored")
				}
				existing, ok := self.rntm.objects[id]
				if !ok {
					return fmt.Errorf("Unable to increase map refcount: Invalid child stored")
				}
				existing.IncreaseRefcount()
			}
		}
	}

	//now increase our own refcount
	return self.object.IncreaseRefcount()
}

//override to handle children refcount additional to our own
func (self *mapImpl) DecreaseRefcount() error {

	//handle keys!
	dt := self.keyDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase map refcount: Keys cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to increase map refcount: Invalid child identifier stored")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {
				return fmt.Errorf("Unable to increase map refcount: Invalid child stored")
			}
			existing.DecreaseRefcount()
		}
	}

	//handle values
	dt = self.valueDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase map refcount: Keys cannot be accessed")
		}
		for _, key := range keys {

			res, err := self.entries.Read(key)
			if err == nil {
				id, err := IdentifierFromEncoded(res.(string))
				if err != nil {
					return utils.StackError(err, "Unable to increase map refcount: Invalid child identifier stored")
				}
				existing, ok := self.rntm.objects[id]
				if !ok {
					return fmt.Errorf("Unable to increase map refcount: Invalid child stored")
				}
				existing.DecreaseRefcount()
			}
		}
	}

	//now decrease our own refcount
	return self.object.DecreaseRefcount()
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
