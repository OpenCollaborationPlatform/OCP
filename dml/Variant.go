// Vector
package dml

import (
	"fmt"

	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
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
	//	vari.GetProperty("type").GetEvent("onChanged").RegisterObjectGoCallback(vari.changedCallback)

	//add methods
	vari.AddMethod("SetValue", MustNewMethod(vari.SetValue, false))
	vari.AddMethod("GetValue", MustNewMethod(vari.GetValue, true))

	vari.AddEvent(NewEvent("onTypeChanged", vari))
	vari.AddEvent(NewEvent("onValueChanged", vari))

	return vari, nil
}

func (self *variant) SetValue(id Identifier, value interface{}) error {

	//check if the type of the value is correct
	dt := self.getDataType(id)
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Unable to set variant data")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	//event handling
	err = self.GetEvent("onBeforeChange").Emit(id)
	if err != nil {
		return err
	}

	var dbValue datastore.ValueVersioned
	dbValue, err = self.GetDBValueVersioned(id, variantKey)

	if err == nil {
		if dt.IsComplex() {

			return fmt.Errorf("Unable to set value for complex datatypes")

		} else if dt.IsType() {
			val, _ := value.(DataType)
			err = dbValue.Write(val.AsString())

		} else {
			//plain types remain
			err = dbValue.Write(value)
		}
	}

	if err != nil {
		return utils.StackError(err, "Unable to write variant")
	}

	self.GetEvent("onValueChanged").Emit(id)
	self.GetEvent("onChanged").Emit(id)
	return nil
}

func (self *variant) GetValue(id Identifier) (interface{}, error) {

	dt := self.getDataType(id)

	dbValue, err := self.GetDBValueVersioned(id, variantKey)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access DB for reading variant")
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
			return nil, utils.StackError(err, "Invalid object stored in variant")
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
func (self *variant) GetSubobjects(id Identifier, bhvr bool) ([]dmlSet, error) {

	objs, err := self.DataImpl.GetSubobjects(id, bhvr)
	if err != nil {
		return nil, err
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return nil, err
		}
		return append(objs, obj), nil
	}

	return objs, nil
}

func (self *variant) GetSubobjectByName(id Identifier, name string, bhvr bool) (dmlSet, error) {

	obj, err := self.DataImpl.GetSubobjectByName(id, name, bhvr)
	if err == nil {
		return obj, err
	}

	dt := self.getDataType(id)
	if dt.IsComplex() {
		obj, err := self.getStoredObject(id)
		if err != nil {
			return dmlSet{}, err
		}

		if obj.id.Name == name {
			return obj, nil
		}
	}

	return dmlSet{}, fmt.Errorf("Unable to find object with given name")
}

func (self *variant) InitializeDB(id Identifier) error {

	err := self.object.InitializeDB(id)
	if err != nil {
		return err
	}

	//on initialisation we want to set default values. That is the same as happens
	//with a changed type proeprty, hence we reuse this function
	return self.changedCallback(id)
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *variant) getStoredObject(id Identifier) (dmlSet, error) {

	dbValue, err := self.GetDBValueVersioned(id, variantKey)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access DB for reading variant")
	}

	res, err := dbValue.Read()
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to read object id from database")
	}

	storeID, ok := res.(*Identifier)
	if !ok {
		return dmlSet{}, fmt.Errorf("Stored identifier invalid")
	}

	set, err := self.rntm.getObjectSet(*storeID)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access object stored in variant")
	}

	return set, nil
}

func (self *variant) changedCallback(id Identifier, args ...interface{}) error {

	//build the default values! And set the value. Don't use SetValue as this
	//assumes old and new value have same datatype

	dbValue, e := self.GetDBValueVersioned(id, variantKey)
	if e != nil {
		return utils.StackError(e, "Unable to access DB for reading variant")
	}

	dt := self.getDataType(id)
	var err error
	if dt.IsComplex() {
		set, err := self.rntm.constructObjectSet(dt, id)
		if err != nil {
			return utils.StackError(err, "Unable to setup variant object: construction failed")
		}
		err = dbValue.Write(set.id)
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
	return nil
}

func (self *variant) getDataType(id Identifier) DataType {

	prop := self.GetProperty("type").(*typeProperty)
	dt := prop.GetDataType(id)
	return dt
}
