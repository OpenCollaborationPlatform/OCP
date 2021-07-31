// Vector
package dml

import (
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"
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
	vec.AddMethod("Length", MustNewIdMethod(vec.Length, true))
	vec.AddMethod("Get", MustNewIdMethod(vec.Get, true))
	vec.AddMethod("GetAll", MustNewIdMethod(vec.GetAll, true))
	vec.AddMethod("Set", MustNewIdMethod(vec.Set, false))
	vec.AddMethod("Append", MustNewIdMethod(vec.Append, false))
	vec.AddMethod("AppendNew", MustNewIdMethod(vec.AppendNew, false))
	vec.AddMethod("Insert", MustNewIdMethod(vec.Insert, false))
	vec.AddMethod("Remove", MustNewIdMethod(vec.Remove, false))
	vec.AddMethod("Swap", MustNewIdMethod(vec.Swap, false))
	vec.AddMethod("Move", MustNewIdMethod(vec.Move, false))

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
		return nil, newUserError(Error_Key_Not_Available, "Index out of bounds: %v", idx)
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
				err = newInternalError(Error_Fatal, "Stored data not a object, as it should be")
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
		return newUserError(Error_Key_Not_Available, "Index out of bounds: %v", idx)
	}

	//check if the type of the value is correct
	dt := self.entryDataType(id)
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Unable to set vector entry")
	}

	if dt.IsComplex() {
		return newUserError(Error_Operation_Invalid, "Compley datatypes cannot be set")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id, idx)
	if err != nil {
		return err
	}

	err = self.set(id, dt, idx, value)
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(id, idx)
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

	return utils.StackError(err, "Unable to access DB")
}

func (self *vector) append(id Identifier, value interface{}) (int64, error) {
	//check if the type of the value is correct (must be done befor increasing length)
	dt := self.entryDataType(id)
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return -1, utils.StackError(err, "Value is wrong type")
	}

	if dt.IsComplex() {
		return 0, newUserError(Error_Operation_Invalid, "Complex datatypes cannot be appended, use AppendNew")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	newLength, err := self.changeLength(id, 1)
	if err != nil {
		return -1, err
	}
	newIdx := newLength - 1

	//and set value
	err = self.set(id, dt, newIdx, value)

	return newIdx, err
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) Append(id Identifier, value interface{}) (int64, error) {

	//event handling
	length, _ := self.Length(id)
	err := self.GetEvent("onBeforeChange").Emit(id, length)
	if err != nil {
		return -1, err
	}

	newIdx, err := self.append(id, value)
	if err != nil {
		return -1, err
	}

	self.GetEvent("onNewEntry").Emit(id, newIdx)
	self.GetEvent("onChanged").Emit(id, newIdx)
	return newIdx, err
}

func (self *vector) appendNew(id Identifier) (interface{}, error) {

	length, _ := self.Length(id)

	//create a new entry
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
		return nil, utils.StackError(err, "Unable to access DB")
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
			self.GetEvent("onNewSubobject").Emit(id, set.id)
		}
	}

	return result, nil
}

//creates a new entry with a all new type, returns the the new one
func (self *vector) AppendNew(id Identifier) (interface{}, error) {

	//event handling
	length, _ := self.Length(id)
	err := self.GetEvent("onBeforeChange").Emit(id, length)
	if err != nil {
		return nil, err
	}

	result, err := self.appendNew(id)
	if err != nil {
		return nil, err
	}

	self.GetEvent("onNewEntry").Emit(id, length)
	self.GetEvent("onChanged").Emit(id, length)
	return result, nil
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) Insert(id Identifier, idx int64, value interface{}) error {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return newUserError(Error_Key_Not_Available, "Index out of bounds: %v", idx)
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	appidx, err := self.append(id, value)
	if err != nil {
		return utils.StackError(err, "Unable to insert value into vector")
	}
	//move take care of events
	err = self.move(id, appidx, idx)
	if err != nil {
		return utils.StackError(err, "Unable to insert value into vector")
	}

	self.GetEvent("onNewEntry").Emit(id, idx)
	return nil
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) InsertNew(id Identifier, idx int64) (interface{}, error) {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return nil, newUserError(Error_Key_Not_Available, "Index out of bounds: %v", idx)
	}

	res, err := self.appendNew(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to insert value into vector")
	}
	//move takes care of eventy
	err = self.move(id, length, idx)
	if err != nil {
		return nil, utils.StackError(err, "Unable to insert value into vector")
	}

	self.GetEvent("onNewEntry").Emit(id, idx)
	return res, nil
}

//remove a entry from the vector
func (self *vector) Remove(id Identifier, idx int64) error {

	length, _ := self.Length(id)
	if idx >= length || idx < 0 {
		return newUserError(Error_Key_Not_Available, "Index out of bounds: %v", idx)
	}

	//inform that we are going to remove
	err := self.GetEvent("onDeleteEntry").Emit(id, idx)
	if err != nil {
		return err
	}

	//deleting means moving each entry after idx one down and shortening the length by 1
	dbEntries, err := self.GetDBMapVersioned(id, entryKey)
	if err != nil {
		return err
	}

	dt := self.entryDataType(id)
	path, err := self.GetObjectPath(id)
	if err != nil {
		return err
	}

	for i := idx; i < (length - 1); i++ {

		data, err := dbEntries.Read(i + 1)
		if err != nil {
			return utils.StackError(err, "Unable to move vector entries after deleting entry")
		}

		//event handling
		err = self.GetEvent("onBeforeChange").Emit(id, i)
		if err != nil {
			return err
		}
		err = dbEntries.Write(i, data)
		if err != nil {
			return utils.StackError(err, "Unable to move vector entries after deleting entry")
		}

		//change the path for objects
		if dt.IsComplex() {
			if id, ok := data.(*Identifier); ok {
				set, err := self.rntm.getObjectSet(*id)
				if err != nil {
					return utils.StackError(err, "Unable to access object for path update during remove")
				}
				err = set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, i))
				if err != nil {
					return utils.StackError(err, "Unable to update path during remove")
				}
			}
		}

		self.GetEvent("onChanged").Emit(id, i)
	}

	err = self.GetEvent("onBeforeChange").Emit(id, length-1)
	if err != nil {
		return err
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

	self.GetEvent("onChanged").Emit(id, length-1)
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
		return newUserError(Error_Key_Not_Available, "Both indices need to be within vector range")
	}

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id, idx1)
	if err != nil {
		return err
	}
	err = self.GetEvent("onBeforeChange").Emit(id, idx2)
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

	//change the path for objects
	if self.entryDataType(id).IsComplex() {
		path, err := self.GetObjectPath(id)
		if err != nil {
			return utils.StackError(err, "Unable to access path for swaping entries")
		}
		if sid, ok := data1.(*Identifier); ok {
			set, err := self.rntm.getObjectSet(*sid)
			if err != nil {
				return utils.StackError(err, "Unable to access object for path update during remove")
			}
			err = set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, idx2))
			if err != nil {
				return utils.StackError(err, "Unable to update path during remove")
			}
		}
		if sid, ok := data2.(*Identifier); ok {
			set, err := self.rntm.getObjectSet(*sid)
			if err != nil {
				return utils.StackError(err, "Unable to access object for path update during remove")
			}
			err = set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, idx1))
			if err != nil {
				return utils.StackError(err, "Unable to update path during remove")
			}
		}
	}

	self.GetEvent("onChanged").Emit(id, idx1)
	self.GetEvent("onChanged").Emit(id, idx2)
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
		return newUserError(Error_Key_Not_Available, "Both indices need to be within vector range")
	}

	err = self.move(id, oldIdx, newIdx)
	if err != nil {
		return err
	}

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

	err = self.GetEvent("onBeforeChange").Emit(id, newIdx)
	if err != nil {
		return err
	}
	data, err := dbEntries.Read(oldIdx)
	if err != nil {
		return utils.StackError(err, "Unable to move vector entries")
	}

	dt := self.entryDataType(id)
	path, err := self.GetObjectPath(id)
	if err != nil {
		return utils.StackError(err, "Unable to access path for swaping entries")
	}

	//move the in-between idx 1 down
	if oldIdx < newIdx {
		//e.g. move 1->3
		for i := oldIdx + 1; i <= newIdx; i++ {

			//event handling
			err = self.GetEvent("onBeforeChange").Emit(id, i-1)
			if err != nil {
				return err
			}

			res, err := dbEntries.Read(i)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = dbEntries.Write(i-1, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}

			//update path
			if dt.IsComplex() {
				if sid, ok := res.(*Identifier); ok {
					set, err := self.rntm.getObjectSet(*sid)
					if err != nil {
						return utils.StackError(err, "Unable to access object for path update during remove")
					}
					err = set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, i-1))
					if err != nil {
						return utils.StackError(err, "Unable to update path during remove")
					}
				}
			}

			self.GetEvent("onChanged").Emit(id, i-1)
		}
	} else {

		//e.g. move 3->1
		//[01234] -> [03124]
		for i := oldIdx; i > newIdx; i-- {

			//event handling
			err = self.GetEvent("onBeforeChange").Emit(id, i)
			if err != nil {
				return err
			}

			res, err := dbEntries.Read(i - 1)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}
			err = dbEntries.Write(i, res)
			if err != nil {
				return utils.StackError(err, "Unable to move vector entries")
			}

			//update path
			if dt.IsComplex() {
				if sid, ok := res.(*Identifier); ok {
					set, err := self.rntm.getObjectSet(*sid)
					if err != nil {
						return utils.StackError(err, "Unable to access object for path update during remove")
					}
					err = set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, i))
					if err != nil {
						return utils.StackError(err, "Unable to update path during remove")
					}
				}
			}

			self.GetEvent("onChanged").Emit(id, i)
		}
	}

	//write the data into the correct location
	err = dbEntries.Write(newIdx, data)
	if err != nil {
		return utils.StackError(err, "Failed to write entry")
	}
	//update path
	if dt.IsComplex() {
		if sid, ok := data.(*Identifier); ok {
			set, err := self.rntm.getObjectSet(*sid)
			if err != nil {
				return utils.StackError(err, "Unable to access object for path update during remove")
			}
			err = set.obj.SetObjectPath(set.id, fmt.Sprintf("%v.%v", path, newIdx))
			if err != nil {
				return utils.StackError(err, "Unable to update path during remove")
			}
		}
	}
	self.GetEvent("onChanged").Emit(id, newIdx)
	return nil
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

	prop, _ := self.GetProperty("type").GetValue(Identifier{}) //const, ignore error
	return prop.(DataType)
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

func (self *vector) GetSubobjects(id Identifier) ([]dmlSet, error) {

	//get default objects
	res, err := self.DataImpl.GetSubobjects(id)
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

//Key handling for generic access to Data
func (self *vector) GetByKey(id Identifier, key Key) (interface{}, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return self.DataImpl.GetByKey(id, key)
	}

	i, err := key.AsDataType(MustNewDataType("int"))
	if err != nil {
		return nil, err
	}
	return self.Get(id, i.(int64))
}

func (self *vector) HasKey(id Identifier, key Key) (bool, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return true, nil
	}

	i, err := key.AsDataType(MustNewDataType("int"))
	if err != nil {
		return false, err
	}
	length, err := self.Length(id)
	if err != nil {
		return false, utils.StackError(err, "Unable to access vector length")
	}
	if i.(int64) < length {
		return true, nil
	}
	return false, nil
}

func (self *vector) GetKeys(id Identifier) ([]Key, error) {

	keys, err := self.DataImpl.GetKeys(id)
	if err != nil {
		return nil, err
	}

	length, err := self.Length(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access vector length")
	}

	for i := int64(0); i < length; i++ {
		keys = append(keys, MustNewKey(i))
	}
	return keys, nil
}

func (self *vector) keyRemoved(Identifier, Key) error {

	//TODO: check if key is object and remove it accordingly
	return nil
}

func (self *vector) keyToDS(id Identifier, key Key) ([]datastore.Key, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return self.DataImpl.keyToDS(id, key)
	}

	i, err := key.AsDataType(MustNewDataType("int"))
	if err != nil {
		return nil, err
	}

	return []datastore.Key{datastore.NewKey(datastore.MapType, true, id.Hash(), entryKey, i)}, nil
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
