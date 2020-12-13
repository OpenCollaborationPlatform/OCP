package dml

import (
	"fmt"
	"log"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/dop251/goja"
)

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	EventHandler

	Type() DataType
	IsConst() bool

	SetValue(id Identifier, value interface{}) error
	GetValue(id Identifier) interface{}

	//required for startup, sets the initial value. If the property already
	//has a value set (in database) this call does nothing.
	SetDefaultValue(value interface{}) error
}

func NewProperty(name string, dtype DataType, default_value interface{}, rntm *Runtime, parent *goja.Object, constprop bool) (Property, error) {

	err := dtype.MustBeTypeOf(default_value)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create property, default value does not match data type")
	}

	var prop Property

	if !constprop {

		if dtype.IsPOD() {

			//setup default value if needed
			prop = &dataProperty{NewEventHandler(), name, dtype, nil, rntm}
			err := prop.SetDefaultValue(default_value)
			if err != nil {
				return nil, utils.StackError(err, "Unable to use provided value as default for property")
			}

		} else if dtype.IsType() {
			//setup default value if needed
			prop = &typeProperty{NewEventHandler(), name, DataType{}}
			err := prop.SetDefaultValue(default_value)
			if err != nil {
				return nil, utils.StackError(err, "Unable to use provided value as default for property")
			}

		} else {
			return nil, fmt.Errorf("Unknown type")
		}
	} else {
		if dtype.IsPOD() {
			prop = &constProperty{NewEventHandler(), dtype, nil}
			err := prop.SetDefaultValue(default_value)
			if err != nil {
				return nil, utils.StackError(err, "Unable to use provided value as default for property")
			}

		} else if dtype.IsType() {
			prop = &constTypeProperty{NewEventHandler(), MustNewDataType("int")}
			err := prop.SetDefaultValue(default_value)
			if err != nil {
				return nil, utils.StackError(err, "Unable to use provided value as default for property")
			}
		} else {
			return nil, fmt.Errorf("Unknown type")
		}
	}

	//add all required events
	prop.AddEvent("onBeforeChange", NewEvent(parent, rntm))
	prop.AddEvent("onChanged", NewEvent(parent, rntm))

	return prop, nil
}

//Data property
//**************

type dataProperty struct {
	eventHandler
	name         string
	propertyType DataType
	default_val  interface{}
	rntm         *Runtime
}

func (self dataProperty) Type() DataType {
	return self.propertyType
}

func (self dataProperty) IsConst() bool {
	return false
}

func (self *dataProperty) SetValue(id Identifier, val interface{}) error {

	//check if the type is correct
	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return err
	}

	//store it
	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}
	if ok, _ := dbValue.Exists(); !ok {
		return fmt.Errorf("Invalid database entry")
	}

	err = self.GetEvent("onBeforeChange").Emit(val)
	if err != nil {
		return err
	}

	if obj, isobj := val.(Object); isobj {
		err = dbValue.Write(obj.Id().Encode())

	} else {
		err = dbValue.Write(val)
	}

	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(val) //no error handling, as value was already changed successfully
	return nil
}

func (self *dataProperty) SetDefaultValue(val interface{}) error {

	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Unable to set property defult value")
	}

	self.default_val = val
	return nil
}

func (self *dataProperty) GetValue(id Identifier) interface{} {

	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}

	if !dbValue.IsValid() {
		return self.default_val
	}

	val, err := dbValue.Read()
	if err != nil {
		log.Printf("Error reading value: %s", err)
		return nil
	}

	return val
}

type typeProperty struct {
	eventHandler
	name        string
	default_val DataType
}

func (self typeProperty) Type() DataType {
	return MustNewDataType("type")
}

func (self typeProperty) IsConst() bool {
	return false
}

//we store the basic information, plain type string or parser result for object
func (self *typeProperty) SetValue(id Identifier, val interface{}) error {

	//check if the type is correct
	err := MustNewDataType("type").MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Cannot set type property: invalid argument")
	}

	err = self.GetEvent("onBeforeChange").Emit(val)
	if err != nil {
		return err
	}

	data := val.(DataType)

	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}
	err = dbValue.Write(data.AsString())
	if err != nil {
		return err
	}

	self.GetEvent("onChanged").Emit(val) //no error handling, as value was already changed successfully
	return nil
}

//we only return basic information, mailny for JS accessibility
func (self *typeProperty) GetValue(id Identifier) interface{} {

	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}

	if !dbValue.IsValid() {
		return self.default_val
	}

	data, err := dbValue.Read()
	if err != nil {
		log.Printf("Error reading value: %s", err)
		return nil
	}
	return DataType{data.(string)}
}

func (self *typeProperty) GetDataType() DataType {

	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}

	if !dbValue.IsValid() {
		return self.default_val
	}

	data, err := dbValue.Read()
	if err != nil {
		log.Printf("Cannot access datastore: %v", err)
	}
	return DataType{data.(string)}
}

func (self *typeProperty) SetDefaultValue(val interface{}) error {
	err := MustNewDataType("type").MustBeTypeOf(val)
	if err != nil {
		utils.StackError(err, "default value for type property set with wrong type")
	}

	self.default_val = val.(DataType)
	return nil
}

//Const property
//**************

type constProperty struct {
	eventHandler
	propertyType DataType
	value        interface{}
}

func (self constProperty) Type() DataType {
	return self.propertyType
}

func (self constProperty) IsConst() bool {
	return true
}

func (self *constProperty) SetValue(val interface{}) error {

	return fmt.Errorf("Const property cannot set value")
}

func (self *constProperty) SetDefaultValue(val interface{}) error {

	//check if the type is correct
	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Const property default value set with wrong type")
	}

	self.value = val
	return nil
}

func (self *constProperty) GetValue() interface{} {

	return self.value
}

type constTypeProperty struct {
	eventHandler
	data DataType
}

func (self constTypeProperty) Type() DataType {
	return MustNewDataType("type")
}

func (self constTypeProperty) IsConst() bool {
	return true
}

func (self *constTypeProperty) SetValue(val interface{}) error {
	return fmt.Errorf("Const property cannot set value")
}

//we store the basic information, plain type string or parser result for object
func (self *constTypeProperty) SetDefaultValue(val interface{}) error {

	//check if the type is correct
	err := MustNewDataType("type").MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Cannot set type property default value: invalid argument")
	}

	self.data = val.(DataType)

	return nil
}

//we only return basic information, mailny for JS accessibility
func (self *constTypeProperty) GetValue() interface{} {
	return self.data
}

func (self *constTypeProperty) GetDataType() DataType {
	return self.data
}

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, dtype DataType, defaultVal interface{}, constprop bool) error
	GetProperty(name string) Property
	GetProperties() []string

	SetupJSProperties(rntm *Runtime, obj *goja.Object) error
}

func NewPropertyHandler() propertyHandler {
	return propertyHandler{make(map[string]Property, 0)}
}

//Implements PropertyHandler interface except the AddProperty function: this must be
//done by the respective implementation, as it must define which kind of property
//shall be used
type propertyHandler struct {
	properties map[string]Property
}

func (self *propertyHandler) HasProperty(name string) bool {

	_, ok := self.properties[name]
	return ok
}

func (self *propertyHandler) GetProperty(name string) Property {

	return self.properties[name]
}

func (self *propertyHandler) GetProperties() []string {

	result := make([]string, len(self.properties))
	cnt := 0
	for name, _ := range self.properties {
		result[cnt] = name
		cnt++
	}
	return result
}

func (self *propertyHandler) SetupJSProperties(rntm *Runtime, obj *goja.Object) error {

	keys := obj.Keys()

	for name, _ := range self.properties {

		//check if proeprty is already set up
		cont := false
		for _, key := range keys {
			if key == name {
				cont = true
				break
			}
		}
		if cont {
			continue
		}

		var propname string = name
		getter := rntm.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {

			//return ob object is different than POD
			val := self.GetProperty(propname).GetValue()
			obj, ok := val.(Object)
			if ok {
				return obj.GetJSObject()
			}

			return rntm.jsvm.ToValue(val)
		})

		setter := rntm.jsvm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {

			if len(call.Arguments) != 1 {
				//panic becomes exception in JS
				panic(rntm.jsvm.ToValue(fmt.Sprintf("Property setting requires exactly one argument")))
			}

			p := self.GetProperty(propname)
			if p.IsConst() {
				//panic becomes exception in JS
				panic(rntm.jsvm.ToValue(fmt.Sprintf("Property %s is constant", propname)))
			}

			//convert goja args to go ones
			args := extractValues(call.Arguments, rntm)
			err := p.SetValue(args[0])
			if err != nil {
				//panic becomes exception in JS
				panic(rntm.jsvm.ToValue(err.Error()))
			}
			return
		})
		err := obj.DefineAccessorProperty(name, getter, setter, goja.FLAG_FALSE, goja.FLAG_TRUE)

		if err != nil {
			return err
		}
	}
	return nil
}
