// Vector
package dml

import (
	datastore "CollaborationNode/datastores"
	"CollaborationNode/utils"
	"fmt"
)

//vector type: stores requested data type by index (0-based)
type vector struct {
	*data

	entries *datastore.MapVersioned
	length  *datastore.ValueVersioned
}

func NewVector(name string, parent identifier, rntm *Runtime) Object {

	base := NewDataBaseClass(name, "Vector", parent, rntm)

	//get the db entries
	set, _ := base.GetDatabaseSet(datastore.MapType)
	mapSet := set.(*datastore.MapVersionedSet)
	entries, _ := mapSet.GetOrCreateMap([]byte("__vector_entries"))
	set, _ = base.GetDatabaseSet(datastore.ValueType)
	valueSet := set.(*datastore.ValueVersionedSet)
	length, _ := valueSet.GetOrCreateValue([]byte("__vector_order"))

	//build the vector
	vec := &vector{
		base,
		entries,
		length,
	}

	//add properties
	vec.AddProperty("type", MustNewDataType("type"), MustNewDataType("int"), true)

	//add methods
	vec.AddMethod("Length", MustNewMethod(vec.Length))
	vec.AddMethod("Get", MustNewMethod(vec.Get))
	vec.AddMethod("Set", MustNewMethod(vec.Set))
	vec.AddMethod("Append", MustNewMethod(vec.Append))
	vec.AddMethod("AppendNew", MustNewMethod(vec.AppendNew))
	vec.AddMethod("Insert", MustNewMethod(vec.Append))
	vec.AddMethod("Delete", MustNewMethod(vec.Append))
	vec.AddMethod("Swap", MustNewMethod(vec.Append))
	vec.AddMethod("Move", MustNewMethod(vec.Append))

	//events of a vector
	vec.AddEvent("onNewEntry", NewEvent(vec.GetJSObject(), rntm.jsvm, MustNewDataType("int")))
	vec.AddEvent("onChangedEntry", NewEvent(vec.GetJSObject(), rntm.jsvm, MustNewDataType("int")))
	vec.AddEvent("onDeletedEntry", NewEvent(vec.GetJSObject(), rntm.jsvm, MustNewDataType("int")))

	return vec
}

func (self *vector) Length() (int64, error) {

	var result int64
	err := self.length.ReadType(&result)
	return result, err
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
		var res string
		err = self.entries.ReadType(idx, &res)
		if err == nil {
			id, e := IdentifierFromEncoded(res)
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

		var res string
		err = self.entries.ReadType(idx, &res)
		if err == nil {
			result, err = NewDataType(res)
		}

	} else {
		//plain types remain
		result, err = self.entries.Read(idx)
	}

	if err != nil {
		return nil, utils.StackError(err, "Unable to write vector at idx %v", idx)
	}

	return result, nil
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
		obj, _ := value.(Object)
		err = self.entries.Write(idx, obj.Id().encode())

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

	//now increase length
	newIdx, _ := self.Length()
	self.length.Write(newIdx + 1)
	//and set value
	err = self.Set(newIdx, value)

	return newIdx, err
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) AppendNew() (interface{}, error) {

	//create a new entry
	length, _ := self.Length()
	result, err := self.buildNew()
	if err != nil {
		return nil, utils.StackError(err, "Unable to create new object to append to vector")
	}

	//write new entry
	err = self.entries.Write(length, result)
	if err != nil {
		return -1, err
	}
	//and increase length
	self.length.Write(length + 1)

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

	return res, nil
}

//remove a entry from the vector
func (self *vector) Delete(idx int64) error {

	length, _ := self.Length()
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//deleting means moving each entry after idx one down and shortening the length by 1
	l, _ := self.Length()
	for i := idx; i < (l - 1); i++ {
		var data []byte
		err := self.entries.ReadType(i+1, &data)
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
	var data1 []byte
	err := self.entries.ReadType(idx1, data1)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	var data2 []byte
	err = self.entries.ReadType(idx2, data2)
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
	var data []byte
	err := self.entries.ReadType(oldIdx, &data)
	if err != nil {
		return utils.StackError(err, "Unable to move vector entries")
	}

	//move the in-between idx 1 down
	if oldIdx < newIdx {
		for i := oldIdx + 1; i <= newIdx; i++ {
			var res []byte
			err := self.entries.ReadType(i, &res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = self.entries.Write(i-1, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
		}
	} else {
		for i := newIdx; i < oldIdx; i++ {

			var res []byte
			err := self.entries.ReadType(i, &res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = self.entries.Write(i+1, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
		}
	}

	//write the data into the correct location
	return self.entries.Write(newIdx, data)
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *vector) entryDataType() DataType {

	prop := self.GetProperty("type").(*typeProperty)
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
		result = obj

	} else if dt.IsType() || dt.IsObject() || dt.IsString() {
		result = ""

	} else if dt.IsBool() {
		result = false
	} else if dt.IsInt() {
		result = 0
	} else if dt.IsFloat() {
		result = 0.0
	}

	return result, err
}
