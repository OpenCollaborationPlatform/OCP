// Vector
package dml

import (
	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"
)

var variantKey = []byte("__variant_value")

//variant type: stores any kind of data, dependend on type property
type variant struct {
	*DataImpl
}

func NewVariant(rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(rntm)
	if err != nil {
		return nil, err
	}

	//build the variant
	vari := &variant{
		base,
	}

	//add properties (with setup callback)
	vari.AddProperty("type", MustNewDataType("type"), MustNewDataType("int"), false)

	//add methods
	vari.AddMethod("SetValue", MustNewIdMethod(vari.SetValue, false))
	vari.AddMethod("GetValue", MustNewIdMethod(vari.GetValue, true))

	vari.AddEvent(NewEvent("onTypeChanged", vari))
	vari.AddEvent(NewEvent("onValueChanged", vari))

	return vari, nil
}

func (self *variant) SetValue(id Identifier, value interface{}) error {

	//check if the type of the value is correct
	dt := self.getDataType(id)
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Value is invalid type")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return utils.StackError(err, "Event onBeforeChange failed")
	}

	var dbValue datastore.ValueVersioned
	dbValue, err = self.GetDBValueVersioned(id, variantKey)

	if err == nil {
		if dt.IsComplex() {

			return newUserError(Error_Operation_Invalid, "Unable to set value for complex datatypes")

		} else if dt.IsType() {
			val, _ := value.(DataType)
			err = dbValue.Write(val.AsString())

		} else {
			//plain types remain
			err = dbValue.Write(value)
		}
	}

	if err != nil {
		return utils.StackError(err, "Unable to write to DB")
	}

	self.GetEvent("onValueChanged").Emit(id)
	self.GetEvent("onChanged").Emit(id)
	return nil
}

func (self *variant) GetValue(id Identifier) (interface{}, error) {

	dt := self.getDataType(id)

	dbValue, err := self.GetDBValueVersioned(id, variantKey)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access DB")
	}

	//if no value was set before we just return the default values!
	if !dbValue.IsValid() {
		return dt.GetDefaultValue(), nil
	}

	var result interface{}

	if dt.IsComplex() {
		var err error
		result, err = self.getStoredObject(id)
		if err != nil {
			return nil, utils.StackError(err, "Invalid object stored")
		}

	} else if dt.IsType() {

		res, err := dbValue.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read type from database")
		}
		result, _ = NewDataType(res.(string))

	} else {
		//plain types remain
		var err error
		result, err = dbValue.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read variant value from database")
		}
	}

	return result, nil
}

func (self *variant) GetSubobjects(id Identifier) ([]dmlSet, error) {

	objs, err := self.DataImpl.GetSubobjects(id)
	if err != nil {
		return nil, err
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return nil, utils.StackError(err, "Invalid object stored")
		}
		return append(objs, obj), nil
	}

	return objs, nil
}

//Key handling for generic access to Data
func (self *variant) GetByKey(id Identifier, key Key) (interface{}, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return self.DataImpl.GetByKey(id, key)
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return dmlSet{}, utils.StackError(err, "Invalid object stored")
		}

		if obj.id.Name == key.AsString() {
			return obj, nil
		}
	}

	return nil, newUserError(Error_Key_Not_Available, "Key not available in variant")
}

func (self *variant) HasKey(id Identifier, key Key) (bool, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return true, nil
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return false, utils.StackError(err, "Invalid object stored")
		}

		return obj.id.Name == key.AsString(), nil
	}

	return false, nil
}

func (self *variant) GetKeys(id Identifier) ([]Key, error) {

	keys, err := self.DataImpl.GetKeys(id)
	if err != nil {
		return nil, err
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return nil, utils.StackError(err, "Invalid object stored")
		}

		keys = append(keys, MustNewKey(obj.id.Name))
	}

	return keys, nil
}

func (self *variant) keyRemoved(Identifier, Key) error {

	//TODO: check if key is object and remove it accordingly
	return nil
}

func (self *variant) keyToDS(id Identifier, key Key) ([]datastore.Key, error) {

	if has, _ := self.DataImpl.HasKey(id, key); has {
		return self.DataImpl.keyToDS(id, key)
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return nil, utils.StackError(err, "Invalid object stored")
		}
		if obj.id.Name != key.AsString() {
			return nil, newUserError(Error_Key_Not_Available, "Key not available in variant")
		}

		return []datastore.Key{datastore.NewKey(datastore.ValueType, true, id.Hash(), variantKey)}, nil
	}

	return nil, newUserError(Error_Key_Not_Available, "Key does not exist in variant")
}

func (self *variant) PropertyChanged(id Identifier, name string) error {

	if name == "type" {

		//build the default values! And set the value. Don't use SetValue as this
		//assumes old and new value have same datatype
		dbValue, e := self.GetDBValueVersioned(id, variantKey)
		if e != nil {
			return utils.StackError(e, "Unable to access DB")
		}

		dt := self.getDataType(id)
		var err error
		if dt.IsComplex() {
			set, err := self.rntm.constructObjectSet(dt, id)
			if err != nil {
				return utils.StackError(err, "Construction of object failed")
			}
			err = dbValue.Write(set.id)

			//build the path
			path, err := self.GetObjectPath(id)
			if err != nil {
				return err
			}
			path += ".value"
			set.obj.SetObjectPath(set.id, path)
			if data, ok := set.obj.(Data); ok {
				data.Created(id)
			}

			set.obj.(Data).Created(set.id)

		} else if dt.IsType() {

			val, _ := dt.GetDefaultValue().(DataType)
			err = dbValue.Write(val.AsString())

		} else {
			result := dt.GetDefaultValue()
			err = dbValue.Write(result)
		}

		if err != nil {
			return err
		}

		self.GetEvent("onTypeChanged").Emit(id)
	}

	return self.DataImpl.PropertyChanged(id, name)
}

func (self *variant) Created(id Identifier) error {

	//on initialisation we want to set default values. That is the same as happens
	//with a changed type proeprty, hence we reuse this function
	err := self.PropertyChanged(id, "type")
	if err != nil {
		return err
	}
	return self.DataImpl.Created(id)
}

func (self *variant) SetObjectPath(id Identifier, path string) error {

	//ourself and children
	err := self.DataImpl.SetObjectPath(id, path)
	if err != nil {
		return err
	}

	//now we need to set the data if it is a object
	dt := self.getDataType(id)
	if dt.IsComplex() {

		//get the object
		result, err := self.getStoredObject(id)
		if err != nil {
			return utils.StackError(err, "Invalid object stored in variant")
		}

		//build the path
		path, err := self.GetObjectPath(id)
		if err != nil {
			return err
		}
		path += ".value"
		result.obj.SetObjectPath(result.id, path)
	}

	return nil
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *variant) getStoredObject(id Identifier) (dmlSet, error) {

	dbValue, err := self.GetDBValueVersioned(id, variantKey)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access DB")
	}

	res, err := dbValue.Read()
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to read object id from database")
	}

	storeID, ok := res.(*Identifier)
	if !ok {
		return dmlSet{}, newInternalError(Error_Fatal, "Stored identifier invalid")
	}

	set, err := self.rntm.getObjectSet(*storeID)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access object stored in variant")
	}

	return set, nil
}

func (self *variant) getDataType(id Identifier) DataType {

	value := self.GetProperty("type").GetValue(id)
	return value.(DataType)
}
