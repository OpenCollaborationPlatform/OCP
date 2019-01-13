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
	vec.AddProperty("type", MustNewDataType("type"), "int", true)

	//add methods
	vec.AddMethod("Length", MustNewMethod(vec.Length))
	vec.AddMethod("Get", MustNewMethod(vec.Get))
	vec.AddMethod("Set", MustNewMethod(vec.Set))
	vec.AddMethod("Append", MustNewMethod(vec.Append))
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

func (self *vector) Length() int64 {

	var result int64
	self.length.ReadType(&result)
	return result
}

func (self *vector) Get(idx int64) interface{} {

	if idx >= self.Length() || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//get the right type
	dt := MustNewDataType(self.GetProperty("type").GetValue().(string))
	if dt.IsPOD() {
		_, err := self.entries.Read(idx)
		if err != nil {
			return utils.StackError(err, "Unable to read idx %v in vector", idx)
		}
	}

	return 1
}

func (self *vector) Set(idx int64, value interface{}) error {

	if idx >= self.Length() || idx < 0 {
		return fmt.Errorf("Index out of bounds: %v", idx)
	}

	//check if the type of the value is correct
	dt := MustNewDataType(self.GetProperty("type").GetValue().(string))
	if dt.IsPOD() && dt.MustBeTypeOf(value) == nil {
		err := self.entries.Write(idx, value)
		if err != nil {
			return utils.StackError(err, "Unable to write vector at idx %v", idx)
		}
		return nil
	}

	//check if it is an object

	return self.entries.Write(idx, value)
}

//creates a new entry with a all new type, returns the index of the new one
func (self *vector) Append() int64 {

	return 1
}

//creates a new entry with a all new type at given position, returns the created object
func (self *vector) Insert(idx int64) interface{} {

	return 1
}

//remove a entry from the vector
func (self *vector) Delete(idx int64) error {

	return nil
}

func (self *vector) Swap(idx1 int64, idx2 int64) error {

	return nil
}

func (self *vector) Move(oldIdx int64, newIdx int64) error {

	return nil
}
