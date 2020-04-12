// Vector
package dml

import (
	"strconv"
	"fmt"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
)

//vector type: stores requested data type by index (0-based)
type vector struct {
	*DataImpl

	entries *datastore.MapVersioned
	length  *datastore.ValueVersioned
}

func NewVector(id Identifier, parent Identifier, rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(id, parent, rntm)
	if err != nil {
		return nil, err
	}

	//get the db entries
	set, _ := base.GetDatabaseSet(datastore.MapType)
	mapSet := set.(*datastore.MapVersionedSet)
	entries, _ := mapSet.GetOrCreateMap([]byte("__vector_entries"))
	set, _ = base.GetDatabaseSet(datastore.ValueType)
	valueSet := set.(*datastore.ValueVersionedSet)
	length, _ := valueSet.GetOrCreateValue([]byte("__vector_order"))

	//initial values
	if holds, _ := length.HoldsValue(); !holds {
		length.Write(int64(0))
	}

	//build the vector
	vec := &vector{
		base,
		entries,
		length,
	}

	//add properties
	vec.AddProperty("type", MustNewDataType("type"), MustNewDataType("none"), true)

	//add methods
	vec.AddMethod("Length", MustNewMethod(vec.Length, true))
	vec.AddMethod("Get", MustNewMethod(vec.Get, true))
	vec.AddMethod("GetAll", MustNewMethod(vec.GetAll, true))
	vec.AddMethod("Set", MustNewMethod(vec.Set, false))
	vec.AddMethod("Append", MustNewMethod(vec.Append, false))
	vec.AddMethod("AppendNew", MustNewMethod(vec.AppendNew, false))
	vec.AddMethod("Insert", MustNewMethod(vec.Insert, false))
	vec.AddMethod("Remove", MustNewMethod(vec.Remove, false))
	vec.AddMethod("Swap", MustNewMethod(vec.Swap, false))
	vec.AddMethod("Move", MustNewMethod(vec.Move, false))

	//events of a vector
	vec.AddEvent("onNewEntry", NewEvent(vec.GetJSObject(), rntm))
	vec.AddEvent("onChange", NewEvent(vec.GetJSObject(), rntm))
	vec.AddEvent("onDeleteEntry", NewEvent(vec.GetJSObject(), rntm))

	return vec, nil
}

func (self *vector) Length() (int64, error) {

	result, err := self.length.Read()
	if err != nil {
		return -1, err
	}
	if result == nil {
		return 0, nil
	}
	return result.(int64), nil
}

//implement DynamicData interface
func (self *vector) Load() error {

	//we only need to load when we store objects
	dt := self.entryDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.entries.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to load vector entries: Keys cannot be accessed")
		}
		for _, key := range keys {

			res, err := self.entries.Read(key)
			if err == nil {
				id, err := IdentifierFromEncoded(res.(string))
				if err != nil {
					return utils.StackError(err, "Unable to load vector: Stored identifier is invalid")
				}
				existing, ok := self.rntm.objects[id]
				if !ok {

					//we made sure the object does not exist. We need to load it
					obj, err := LoadObject(self.rntm, dt, id)
					if err != nil {
						return utils.StackError(err, "Unable to load object for vector: construction failed")
					}
					obj.IncreaseRefcount()
					self.rntm.objects[id] = obj.(Data)
				} else {
					existing.IncreaseRefcount()
				}
			} else {
				return utils.StackError(err, "Unable to load vector entries: entry cannot be read")
			}
		}
	}
	return nil
}

func (self *vector) Get(idx int64) (interface{}, error) {

	length, _ := self.Length()
	if idx >= length || idx < 0 {
		return nil, fmt.Errorf("Index out of bounds: %v", idx)
	}

	//check if the type of the value is correct
	dt := self.entryDataType()
	var result interface{}
	var err error = nil

	if dt.IsObject() || dt.IsComplex() {
		res, err := self.entries.Read(idx)
		if err == nil {
			id, e := IdentifierFromEncoded(res.(string))
			if e != nil {
				err = e
			} else {
				res, ok := self.rntm.objects[id]
				if !ok {
					err = fmt.Errorf("Vector entry is invalid object")
				}
				result = res
			}
		}

	} else if dt.IsType() {

		res, err := self.entries.Read(idx)
		if err == nil {
			result, err = NewDataType(res.(string))
		}

	} else {
		//plain types remain
		result, err = self.entries.Read(idx)
	}

	if err != nil {
		return nil, utils.StackError(err, "Unable to read vector at idx %v", idx)
	}

	return result, nil
}

func (self *vector) GetAll() (interface{}, error) {

	length, _ := self.Length()
	list := make([]interface{}, length)
	
	for idx:=int64(0); idx<length; idx++ {

		result, err := self.Get(idx)	
		if err != nil {
			return nil, err
		}		
		list[idx] = result
	}

	return list, nil
}


func (self *vector) Set(idx int64, value interface{}) error {

	length, _ := self.Length()
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//check if the type of the value is correct
	dt := self.entryDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Unable to set vector entry")
	}

	if dt.IsObject() || dt.IsComplex() {

		old, _ := self.Get(idx)
		obj, _ := value.(Object)
		err = self.entries.Write(idx, obj.Id().Encode())

		//handle ref counts
		if err != nil {
			data, ok := value.(Data)
			if ok {
				data.IncreaseRefcount()
			}
			data, ok = old.(Data)
			if ok {
				data.DecreaseRefcount()
			}
		}

	} else if dt.IsType() {

		val, _ := value.(DataType)
		err = self.entries.Write(idx, val.AsString())

	} else {
		//plain types remain
		err = self.entries.Write(idx, value)
	}

	if err != nil {
		return utils.StackError(err, "Unable to write vector at idx %v", idx)
	}
	
	self.GetEvent("onChange").Emit()
	return nil
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) Append(value interface{}) (int64, error) {

	//check if the type of the value is correct (must be done befor increasing length)
	dt := self.entryDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return -1, utils.StackError(err, "Unable to append vector entry")
	}

	if dt.IsComplex() || dt.IsObject() {
		//we have a object here, hence handle refcount!
		data, ok := value.(Data)
		if ok {
			data.IncreaseRefcount()
		}
	}

	//now increase length
	newIdx, _ := self.Length()
	self.length.Write(newIdx + 1)
	//and set value
	err = self.Set(newIdx, value)

	self.GetEvent("onNewEntry").Emit(newIdx)
	return newIdx, err
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) AppendNew() (interface{}, error) {

	//create a new entry (with correct refcount if needed)
	length, _ := self.Length()
	result, err := self.buildNew()
	if err != nil {
		return nil, utils.StackError(err, "Unable to create new object to append to vector")
	}

	//write new entry
	if obj, ok := result.(Object); ok {
		err = self.entries.Write(length, obj.Id().Encode())
	} else {
		err = self.entries.Write(length, result)
	}
	if err != nil {
		return nil, utils.StackError(err, "Unable to write new object to vector")
	}
	//and increase length
	self.length.Write(length + 1)

	self.GetEvent("onNewEntry").Emit(length)
	return result, nil
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) Insert(idx int64, value interface{}) error {

	length, _ := self.Length()
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	appidx, err := self.Append(value)
	if err != nil {
		return utils.StackError(err, "Unable to insert value into vector")
	}
	err = self.Move(appidx, idx)
	if err != nil {
		return utils.StackError(err, "Unable to insert value into vector")
	}

	self.GetEvent("onNewEntry").Emit(idx)
	return nil
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) InsertNew(idx int64) (interface{}, error) {

	length, _ := self.Length()
	if idx >= length || idx < 0 {
		return nil, fmt.Errorf("Index out of bounds: %v", idx)
	}

	res, err := self.AppendNew()
	if err != nil {
		return nil, utils.StackError(err, "Unable to insert value into vector")
	}
	err = self.Move(length, idx)
	if err != nil {
		return nil, utils.StackError(err, "Unable to insert value into vector")
	}

	self.GetEvent("onNewEntry").Emit(idx)
	return res, nil
}

//remove a entry from the vector
func (self *vector) Remove(idx int64) error {

	length, _ := self.Length()
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	dt := self.entryDataType()
	if dt.IsComplex() || dt.IsObject() {
		//we have a object stored, hence delete must remove it completely!
		val, err := self.Get(idx)
		if err != nil {
			return utils.StackError(err, "Unable to delete entry")
		}
		data, ok := val.(Data)
		if ok {
			data.DecreaseRefcount()
		}
	}
	
	//inform that we are going to remove
	self.GetEvent("onDeleteEntry").Emit(idx)

	//deleting means moving each entry after idx one down and shortening the length by 1
	l, _ := self.Length()
	for i := idx; i < (l - 1); i++ {
		data, err := self.entries.Read(i + 1)
		if err != nil {
			return utils.StackError(err, "Unable to move vector entries after deleting entry")
		}
		err = self.entries.Write(i, data)
		if err != nil {
			return utils.StackError(err, "Unable to move vector entries after deleting entry")
		}
	}

	//now shorten the length
	err := self.length.Write(l - 1)
	if err != nil {
		return utils.StackError(err, "Unable to Delete entry: vector cnnot be shortent")
	}

	//and delete the old key
	return self.entries.Remove(l - 1)
}

func (self *vector) Swap(idx1 int64, idx2 int64) error {

	//get the data to move
	data1, err := self.entries.Read(idx1)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	data2, err := self.entries.Read(idx2)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	err = self.entries.Write(idx1, data2)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}
	err = self.entries.Write(idx2, data1)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	self.GetEvent("onChange").Emit()
	return nil
}

func (self *vector) Move(oldIdx int64, newIdx int64) error {

	//everything between newIdx and oldIdx must be moved by 1
	//[0 1 2 3 4 5 6]
	// e.g. old: 2, new: 5 [0 1 3 4 5 2 6] (3->2, 4->3, 5->4, 2->5)
	// e.g. old: 5, new: 2 [0 1 5 2 3 4 6] (2->3, 3->4, 4->5, 5->2)

	if oldIdx == newIdx {
		return nil
	}

	//get the data to move
	data, err := self.entries.Read(oldIdx)
	if err != nil {
		return utils.StackError(err, "Unable to move vector entries")
	}

	//move the in-between idx 1 down
	if oldIdx < newIdx {
		for i := oldIdx + 1; i <= newIdx; i++ {
			res, err := self.entries.Read(i)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = self.entries.Write(i-1, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
		}
	} else {

		for i := oldIdx; i > newIdx; i-- {
			res, err := self.entries.Read(i - 1)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = self.entries.Write(i, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
		}
	}

	//write the data into the correct location
	err = self.entries.Write(newIdx, data)
	if err == nil {
		self.GetEvent("onChange").Emit()
	}
	return err
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

//override to handle children refcount additional to our own
func (self *vector) IncreaseRefcount() error {

	//increase entrie refcount
	dt := self.entryDataType()
	if dt.IsObject() || dt.IsComplex() {

		length, _ := self.Length()
		for i := int64(0); i < length; i++ {
			res, err := self.entries.Read(i)
			if err == nil {
				id, e := IdentifierFromEncoded(res.(string))
				if e == nil {
					res, ok := self.rntm.objects[id]
					if ok {
						err := res.IncreaseRefcount()
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	//now increase our own and children refcount
	return self.object.IncreaseRefcount()
}

//override to handle children refcount additional to our own
func (self *vector) DecreaseRefcount() error {
	//decrease child refcount
	dt := self.entryDataType()
	if dt.IsObject() || dt.IsComplex() {

		length, _ := self.Length()
		for i := int64(0); i < length; i++ {
			res, err := self.entries.Read(i)
			if err == nil {
				id, e := IdentifierFromEncoded(res.(string))
				if e == nil {
					res, ok := self.rntm.objects[id]
					if ok {
						err := res.DecreaseRefcount()
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	//now decrease our own
	return self.object.DecreaseRefcount()
}

func (self *vector) entryDataType() DataType {

	prop := self.GetProperty("type").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}

func (self *vector) buildNew() (interface{}, error) {

	var err error
	var result interface{}

	dt := self.entryDataType()
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "")
		if err != nil {
			return -1, utils.StackError(err, "Unable to append new object to vector: construction failed")
		}
		obj.IncreaseRefcount()
		result = obj

	} else {
		result = dt.GetDefaultValue()
	}

	return result, err
}

func (self *vector) print() {

	fmt.Printf("[")
	l, _ := self.Length()
	for i := int64(0); i < l; i++ {
		val, _ := self.Get(i)
		fmt.Printf("%v ", val)
	}
	fmt.Println("]")
}

func (self *vector) GetSubobjects(bhvr bool, prop bool) []Object {
	
	//get default objects
	res := self.DataImpl.GetSubobjects(bhvr, prop)
	
	dt := self.entryDataType()
	if dt.IsObject() || dt.IsComplex() {
		//iterate over all entries and add them
		length, _ := self.Length()
		for i := int64(0); i < length; i++ {
			read, err := self.entries.Read(i)
			if err == nil {
				id, e := IdentifierFromEncoded(read.(string))
				if e == nil {
					obj, ok := self.rntm.objects[id]
					if ok {
						res = append(res, obj)
					}
				}
			}
		}
	}
	
	return res
}

func (self *vector) GetSubobjectByName(name string, bhvr bool, prop bool) (Object, error) {
	
	//default search
	obj, err := self.DataImpl.GetSubobjectByName(name, bhvr, prop)
	if err == nil {
		return obj, nil
	}
	
	//let's see if it is a index
	i, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("No such idx available")
	}
	
	res, err := self.Get(i)
	if err != nil {
		return nil, utils.StackError(err, "No index %v available in %v", name, obj.Id().Name)
	}
	
	obj, ok := res.(Object)
	if !ok {
		return nil, fmt.Errorf("Index is %v not a object", name)
	}
	
	return obj, nil
}

func (self *vector) GetValueByName(name string) interface{} {
	
	
	//let's see if it is a index
	i, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil
	}
	
	res, err := self.Get(i)
	if err != nil {
		return nil
	}
	
	return res
}