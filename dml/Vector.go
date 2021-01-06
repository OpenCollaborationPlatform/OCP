// Vector
package dml

import (
	"fmt"
	"strconv"

	"github.com/ickby/CollaborationNode/utils"
)

var lengthKey = []byte("__length")
var entryKey = []byte("__entries")

//vector type: stores requested data type by index (0-based)
type vector struct {
	*DataImpl
}

func NewVector(rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(rntm)
	if err != nil {
		return nil, err
	}

	//build the vector
	vec := &vector{
		base,
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
	vec.AddEvent(NewEvent("onNewEntry", vec))
	vec.AddEvent(NewEvent("onDeleteEntry", vec))

	return vec, nil
}

func (self *vector) InitializeDB(id Identifier) error {

	err := self.object.InitializeDB(id)
	if err != nil {
		return err
	}

	//initial values
	dbLength, err := self.GetDBValueVersioned(id, lengthKey)
	if err != nil {
		return err
	}
	if ok, _ := dbLength.WasWrittenOnce(); !ok {
		dbLength.Write(int64(0))
	}

	return nil
}

func (self *vector) Length(id Identifier) (int64, error) {

	dbLength, err := self.GetDBValueVersioned(id, lengthKey)
	if err != nil {
		return -1, err
	}

	result, err := dbLength.Read()
	if err != nil {
		return -1, err
	}
	if result == nil {
		return 0, nil
	}
	return result.(int64), nil
}

func (self *vector) Get(id Identifier, idx int64) (interface{}, error) {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return nil, fmt.Errorf("Index out of bounds: %v", idx)
	}

	//db access for entries
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return nil, err
	}

	//check if the type of the value is correct
	dt := self.entryDataType(id)
	var result interface{}

	if dt.IsComplex() {
		res, err := dbEntries.Read(idx)
		if err == nil {
			ident, ok := res.(*Identifier)
			if !ok {
				err = fmt.Errorf("Stored data not a object, as it should be")
			} else {
				res, e := self.rntm.getObjectSet(*ident)
				if e != nil {
					err = utils.StackError(e, "Unable to access stored object")
				}
				result = res
			}
		}

	} else if dt.IsType() {

		res, err := dbEntries.Read(idx)
		if err == nil {
			result, err = NewDataType(res.(string))
		}

	} else {
		//plain types remain
		result, err = dbEntries.Read(idx)
	}

	if err != nil {
		return nil, utils.StackError(err, "Unable to read vector at idx %v", idx)
	}

	return result, nil
}

func (self *vector) GetAll(id Identifier) (interface{}, error) {

	length, _ := self.Length(id)
	list := make([]interface{}, length)

	for idx := int64(0); idx < length; idx++ {

		result, err := self.Get(id, idx)
		if err != nil {
			return nil, err
		}
		list[idx] = result
	}

	return list, nil
}

func (self *vector) Set(id Identifier, idx int64, value interface{}) error {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//check if the type of the value is correct
	dt := self.entryDataType(id)
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Unable to set vector entry")
	}

	if dt.IsComplex() {
		return fmt.Errorf("Compley datatypes cannot be set")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	err = self.set(id, dt, idx, value)
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(id)
	return nil
}

//internal set, without any type checking
func (self *vector) set(id Identifier, dt DataType, idx int64, value interface{}) error {

	//db access for entries
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}

	if dt.IsType() {

		val, _ := value.(DataType)
		err = dbEntries.Write(idx, val.AsString())

	} else {
		//plain types remain
		err = dbEntries.Write(idx, value)
	}

	return err
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) Append(id Identifier, value interface{}) (int64, error) {

	//check if the type of the value is correct (must be done befor increasing length)
	dt := self.entryDataType(id)
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return -1, utils.StackError(err, "Unable to append vector entry")
	}

	if dt.IsComplex() {
		return 0, fmt.Errorf("Complex datatypes cannot be appended, use AppendNew")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return -1, err
	}

	newLength, err := self.changeLength(id, 1)
	if err != nil {
		return -1, err
	}
	newIdx := newLength - 1

	//and set value
	err = self.set(id, dt, newIdx, value)

	self.GetEvent("onNewEntry").Emit(id, newIdx)
	self.GetEvent("onChanged").Emit(id)
	return newIdx, err
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) AppendNew(id Identifier) (interface{}, error) {

	//event handling
	err := self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return nil, err
	}

	//create a new entry
	length, _ := self.Length(id)
	result, err := self.buildNew(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create new object to append to vector")
	}

	//write new entry
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return nil, err
	}
	if set, ok := result.(dmlSet); ok {
		err = dbEntries.Write(length, set.id)
	} else {
		err = dbEntries.Write(length, result)
	}
	if err != nil {
		return nil, utils.StackError(err, "Unable to write new object to vector")
	}
	//and increase length
	_, err = self.changeLength(id, 1)
	if err != nil {
		return nil, err
	}

	if set, ok := result.(dmlSet); ok {
		//build the object path and set it
		path, err := self.GetObjectPath(id)
		if err != nil {
			return nil, err
		}
		set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, length))

		//call "created"
		if data, ok := set.obj.(Data); ok {
			data.Created(set.id)
		}
	}

	self.GetEvent("onNewEntry").Emit(id, length)
	self.GetEvent("onChanged").Emit(id)
	return result, nil
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) Insert(id Identifier, idx int64, value interface{}) error {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	//event handling
	err := self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	appidx, err := self.Append(id, value)
	if err != nil {
		return utils.StackError(err, "Unable to insert value into vector")
	}
	err = self.move(id, appidx, idx)
	if err != nil {
		return utils.StackError(err, "Unable to insert value into vector")
	}

	self.GetEvent("onNewEntry").Emit(id, idx)
	self.GetEvent("onChanged").Emit(id)
	return nil
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) InsertNew(id Identifier, idx int64) (interface{}, error) {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return nil, fmt.Errorf("Index out of bounds: %v", idx)
	}

	//event handling
	err := self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return nil, err
	}

	res, err := self.AppendNew(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to insert value into vector")
	}
	err = self.move(id, length, idx)
	if err != nil {
		return nil, utils.StackError(err, "Unable to insert value into vector")
	}

	self.GetEvent("onNewEntry").Emit(id, idx)
	self.GetEvent("onChanged").Emit(id)
	return res, nil
}

//remove a entry from the vector
func (self *vector) Remove(id Identifier, idx int64) error {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//event handling
	err := self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	//inform that we are going to remove
	err = self.GetEvent("onDeleteEntry").Emit(id, idx)
	if err != nil {
		return err
	}

	//deleting means moving each entry after idx one down and shortening the length by 1
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}

	for i := idx; i < (length - 1); i++ {
		data, err := dbEntries.Read(i + 1)
		if err != nil {
			return utils.StackError(err, "Unable to move vector entries after deleting entry")
		}
		err = dbEntries.Write(i, data)
		if err != nil {
			return utils.StackError(err, "Unable to move vector entries after deleting entry")
		}
	}

	//now shorten the length
	_, err = self.changeLength(id, -1)
	if err != nil {
		return utils.StackError(err, "Unable to Delete entry: vector cnnot be shortent")
	}

	//and delete the old key
	err = dbEntries.Remove(length - 1)
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(id)
	return nil
}

func (self *vector) Swap(id Identifier, idx1 int64, idx2 int64) error {

	if idx1 == idx2 {
		return nil
	}

	length, err := self.Length(id)
	if err != nil {
		return err
	}
	if idx1 >= length || idx2 >= length {
		return fmt.Errorf("Both indices need to be within vector range")
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	//get the data to move
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}
	data1, err := dbEntries.Read(idx1)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	data2, err := dbEntries.Read(idx2)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	err = dbEntries.Write(idx1, data2)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}
	err = dbEntries.Write(idx2, data1)
	if err != nil {
		return utils.StackError(err, "Unable to swap vector entries")
	}

	self.GetEvent("onChanged").Emit(id)
	return nil
}

func (self *vector) Move(id Identifier, oldIdx int64, newIdx int64) error {

	if oldIdx == newIdx {
		return nil
	}

	length, err := self.Length(id)
	if err != nil {
		return err
	}
	if oldIdx >= length || newIdx >= length {
		return fmt.Errorf("Both indices need to be within vector range")
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	err = self.move(id, oldIdx, newIdx)
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(id)
	return nil
}

func (self *vector) move(id Identifier, oldIdx int64, newIdx int64) error {

	//everything between newIdx and oldIdx must be moved by 1
	//[0 1 2 3 4 5 6]
	// e.g. old: 2, new: 5 [0 1 3 4 5 2 6] (3->2, 4->3, 5->4, 2->5)
	// e.g. old: 5, new: 2 [0 1 5 2 3 4 6] (2->3, 3->4, 4->5, 5->2)

	//get the data to move
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}
	data, err := dbEntries.Read(oldIdx)
	if err != nil {
		return utils.StackError(err, "Unable to move vector entries")
	}

	//move the in-between idx 1 down
	if oldIdx < newIdx {
		for i := oldIdx + 1; i <= newIdx; i++ {
			res, err := dbEntries.Read(i)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = dbEntries.Write(i-1, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
		}
	} else {

		for i := oldIdx; i > newIdx; i-- {
			res, err := dbEntries.Read(i - 1)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = dbEntries.Write(i, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
		}
	}

	//write the data into the correct location
	return dbEntries.Write(newIdx, data)
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *vector) changeLength(id Identifier, amount int64) (int64, error) {

	dbLength, err := self.GetDBValueVersioned(id, lengthKey)
	if err != nil {
		return -1, err
	}
	result, err := dbLength.Read()
	if err != nil {
		return -1, err
	}
	newLength := result.(int64) + amount

	err = dbLength.Write(newLength)
	if err != nil {
		return -1, err
	}

	return newLength, nil
}

func (self *vector) entryDataType(id Identifier) DataType {

	prop := self.GetProperty("type").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}

func (self *vector) buildNew(id Identifier) (interface{}, error) {

	var err error
	var result interface{}

	dt := self.entryDataType(id)
	if dt.IsComplex() {
		set, err := self.rntm.constructObjectSet(dt, id)
		if err != nil {
			return dmlSet{}, utils.StackError(err, "Unable to append new object to vector: construction failed")
		}
		result = set

	} else {
		result = dt.GetDefaultValue()
	}

	return result, err
}

func (self *vector) print(id Identifier) {

	fmt.Printf("[")
	l, _ := self.Length(id)
	for i := int64(0); i < l; i++ {
		val, _ := self.Get(id, i)
		fmt.Printf("%v ", val)
	}
	fmt.Println("]")
}

func (self *vector) GetSubobjects(id Identifier, bhvr bool) ([]dmlSet, error) {

	//get default objects
	res, err := self.DataImpl.GetSubobjects(id, bhvr)
	if err != nil {
		return nil, err
	}

	dt := self.entryDataType(id)
	if dt.IsComplex() {

		dbEntries, err := self.GetDBMapVersioned(id, entryKey)
		if err != nil {
			return nil, err
		}

		//iterate over all entries and add them
		length, _ := self.Length(id)
		for i := int64(0); i < length; i++ {
			read, err := dbEntries.Read(i)
			if err == nil {
				id, ok := read.(*Identifier)
				if ok {
					set, err := self.rntm.getObjectSet(*id)
					if err != nil {
						return nil, err
					}
					res = append(res, set)
				}
			}
		}
	}

	return res, nil
}

func (self *vector) GetSubobjectByName(id Identifier, name string, bhvr bool) (dmlSet, error) {

	//default search
	set, err := self.DataImpl.GetSubobjectByName(id, name, bhvr)
	if err == nil {
		return set, nil
	}

	//let's see if it is a index
	i, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return dmlSet{}, fmt.Errorf("No such idx available")
	}

	res, err := self.Get(id, i)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "No index %v available in %v", name, id.Name)
	}

	result, ok := res.(dmlSet)
	if !ok {
		return dmlSet{}, fmt.Errorf("Index is %v not a object", name)
	}

	return result, nil
}

func (self *vector) GetValueByName(id Identifier, name string) (interface{}, error) {

	//let's see if it is a index
	i, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, err
	}

	res, err := self.Get(id, i)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (self *vector) SetObjectPath(id Identifier, path string) error {

	//ourself and children
	err := self.DataImpl.SetObjectPath(id, path)
	if err != nil {
		return err
	}

	//now we need to set all map objects, if available
	dt := self.entryDataType(id)
	if dt.IsComplex() {

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
			fullpath := fmt.Sprintf("%v.%v", path, key)

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
