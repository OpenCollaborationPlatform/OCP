// Vector
package dml

import (
	"fmt"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
)

//variant type: stores any kind of data, dependend on type property
type variant struct {
	*DataImpl

	value *datastore.ValueVersioned
}

func NewVariant(id Identifier, parent Identifier, rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(id, parent, rntm)
	if err != nil {
		return nil, err
	}

	//get the db entries
	set, _ := base.GetDatabaseSet(datastore.ValueType)
	valueSet := set.(*datastore.ValueVersionedSet)
	value, _ := valueSet.GetOrCreateValue([]byte("__variant_value"))

	//build the variant
	vari := &variant{
		base,
		value,
	}

	//add properties (with setup callback)
	vari.AddProperty("type", MustNewDataType("type"), MustNewDataType("int"), false)
	vari.GetProperty("type").GetEvent("onChanged").RegisterCallback(vari.changedCallback)

	//add methods
	vari.AddMethod("SetValue", MustNewMethod(vari.SetValue, false))
	vari.AddMethod("GetValue", MustNewMethod(vari.GetValue, true))

	vari.AddEvent("onTypeChanged", NewEvent(vari.GetJSObject(), rntm))
	vari.AddEvent("onValueChanged", NewEvent(vari.GetJSObject(), rntm))

	return vari, nil
}

//implement DynamicData interface
func (self *variant) Load() error {

	//if the value was never set we don't need to load anything...
	has, err := self.value.HoldsValue()
	if err != nil {
		return utils.StackError(err, "Unable to access database to load variant value")
	}
	if !has {
		return nil
	}
	//we only need to load when we store objects
	dt := self.getDataType()
	if dt.IsComplex() {

		res, err := self.value.Read()
		if err == nil {
			id, err := IdentifierFromEncoded(res.(string))
			if err != nil {
				return utils.StackError(err, "Unable to load variant: Stored identifier is invalid")
			}
			obj, err := LoadObject(self.rntm, dt, id, self.Id())
			if err != nil {
				return utils.StackError(err, "Unable to load object for variant: construction failed")
			}
			self.rntm.objects[id] = obj.(Data)

		} else {
			return utils.StackError(err, "Unable to load variant: entry cannot be read")
		}
	}
	return nil
}

func (self *variant) SetValue(value interface{}) error {

	//check if the type of the value is correct
	dt := self.getDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Unable to set variant data")
	}

	if dt.IsComplex() {

		return fmt.Errorf("Unable to set value for complex datatypes")

	} else if dt.IsType() {

		val, _ := value.(DataType)
		err = self.value.Write(val.AsString())

	} else {
		//plain types remain
		err = self.value.Write(value)
	}

	if err != nil {
		return utils.StackError(err, "Unable to write variant")
	}

	self.GetEvent("onValueChanged").Emit()
	return nil
}

func (self *variant) GetValue() (interface{}, error) {

	dt := self.getDataType()

	//if no value waas set before we just return the default values!
	if holds, _ := self.value.HoldsValue(); !holds {
		return dt.GetDefaultValue(), nil
	}

	var result interface{}

	if dt.IsComplex() {
		res, err := self.value.Read()
		if err == nil {
			id, err := IdentifierFromEncoded(res.(string))
			if err != nil {
				return nil, utils.StackError(err, "Unable to parse object id from databaase value")
			} else {
				res, ok := self.rntm.objects[id]
				if !ok {
					return nil, fmt.Errorf("Variant entry is invalid object")
				}
				result = res
			}

		} else {
			return nil, utils.StackError(err, "Unable to read object id from deatabase")
		}

	} else if dt.IsType() {

		res, err := self.value.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read type from database")
		}
		result, err = NewDataType(res.(string))

	} else {
		//plain types remain
		var err error
		result, err = self.value.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read variant value from database")
		}
	}

	return result, nil
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *variant) changedCallback(args ...interface{}) error {

	self.GetEvent("onTypeChanged").Emit()

	//build the default values! And set the value. Don't use SetValue as this
	//assumes old and new value have same datatype
	dt := self.getDataType()
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "", self.Id())
		if err != nil {
			return utils.StackError(err, "Unable to setup variant object: construction failed")
		}
		return self.value.Write(obj.Id().Encode())

	} else if dt.IsType() {

		val, _ := dt.GetDefaultValue().(DataType)
		return self.value.Write(val.AsString())

	} else {
		result := dt.GetDefaultValue()
		return self.value.Write(result)
	}

	return fmt.Errorf("Something went wrong")
}

func (self *variant) getDataType() DataType {

	prop := self.GetProperty("type").(*typeProperty)
	dt := prop.GetDataType()
	return dt
}
