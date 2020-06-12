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
	vari.GetProperty("type").GetEvent("onBeforeChange").RegisterCallback(vari.beforeChangeCallback)
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

	//we only need to load when we store objects
	dt := self.getDataType()
	if dt.IsComplex() {

		//if the value was never set. Hence we need to create the object newly
		if !self.value.IsValid() {
			obj, err := ConstructObject(self.rntm, dt, "", self.Id())
			if err != nil {
				return utils.StackError(err, "Unable to setup variant object: construction of object failed")
			}
			err = self.value.Write(obj.Id().Encode())
			if err != nil {
				return err
			}

		} else {

			res, err := self.value.Read()
			if err == nil {
				id, err := IdentifierFromEncoded(res.(string))
				if err != nil {
					return utils.StackError(err, "Unable to load variant: Stored identifier is invalid")
				}
				//maybe the object exists already (happens on first startup: it is created already)
				_, has := self.rntm.objects[id]
				if has {
					return nil
				}

				//not existing yet, load it!
				obj, err := LoadObject(self.rntm, dt, id, self.Id())
				if err != nil {
					return utils.StackError(err, "Unable to load object for variant: construction failed")
				}
				self.rntm.objects[id] = obj.(Data)
			} else {
				return utils.StackError(err, "Unable to load variant: entry cannot be read")
			}
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

	//event handling
	err = self.GetEvent("onBeforeChange").Emit()
	if err != nil {
		return err
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
	self.GetEvent("onChanged").Emit()
	return nil
}

func (self *variant) GetValue() (interface{}, error) {

	dt := self.getDataType()

	//if no value was set before we just return the default values!
	if !self.value.IsValid() {
		return dt.GetDefaultValue(), nil
	}

	var result interface{}

	if dt.IsComplex() {
		var err error
		result, err = self.getObject()
		if err != nil {
			return nil, utils.StackError(err, "Invalid object stored in variant")
		}

	} else if dt.IsType() {

		res, err := self.value.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read type from database")
		}
		result, _ = NewDataType(res.(string))

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
func (self *variant) GetSubobjects(bhvr bool) []Object {

	objs := self.DataImpl.GetSubobjects(bhvr)

	dt := self.getDataType()
	if dt.IsComplex() {
		obj, err := self.getObject()
		if err != nil {
			return objs
		}
		return append(objs, obj)
	}

	return objs
}

func (self *variant) GetSubobjectByName(name string, bhvr bool) (Object, error) {

	obj, err := self.DataImpl.GetSubobjectByName(name, bhvr)
	if obj == nil {
		return obj, err
	}

	dt := self.getDataType()
	if dt.IsComplex() {
		obj, err := self.getObject()
		if err != nil {
			return nil, err
		}

		if obj.Id().Name == name {
			return obj, nil
		}
	}

	return nil, fmt.Errorf("Unable to find object with given name")
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************

func (self *variant) getObject() (Object, error) {

	res, err := self.value.Read()
	if err == nil {
		id, err := IdentifierFromEncoded(res.(string))
		if err != nil {
			return nil, utils.StackError(err, "Unable to parse object id from databaase value")
		} else {
			res, ok := self.rntm.objects[id]
			if !ok {
				return nil, fmt.Errorf("Variant entry is non existing object")
			}
			return res, nil
		}
	}

	return nil, utils.StackError(err, "Unable to read object id from deatabase")
}

func (self *variant) beforeChangeCallback(args ...interface{}) error {

	//check if we currently hold a object and handle it accordingly
	dt := self.getDataType()
	if dt.IsComplex() {

		ndt := args[0].(DataType)
		if dt.IsEqual(ndt) {
			//returning an error here means the value change fails, and onChanged is
			//never called
			return fmt.Errorf("Value is equal current value: not changed")
		}

		obj, err := self.getObject()
		if err != nil {
			return utils.StackError(err, "Cannot change object type")
		}

		//remove object!
		return self.rntm.removeObject(obj)
	}

	return nil
}

func (self *variant) changedCallback(args ...interface{}) error {

	//build the default values! And set the value. Don't use SetValue as this
	//assumes old and new value have same datatype
	dt := self.getDataType()
	var err error
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "", self.Id())
		if err != nil {
			return utils.StackError(err, "Unable to setup variant object: construction failed")
		}
		err = self.value.Write(obj.Id().Encode())

	} else if dt.IsType() {

		val, _ := dt.GetDefaultValue().(DataType)
		err = self.value.Write(val.AsString())

	} else {
		result := dt.GetDefaultValue()
		err = self.value.Write(result)
	}

	if err != nil {
		return err
	}

	self.GetEvent("onTypeChanged").Emit()
	return nil
}

func (self *variant) getDataType() DataType {

	prop := self.GetProperty("type").(*typeProperty)
	dt := prop.GetDataType()
	return dt
}
