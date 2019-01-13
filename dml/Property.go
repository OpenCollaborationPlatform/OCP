package dml

import (
	datastore "CollaborationNode/datastores"
	"CollaborationNode/utils"
	"fmt"
	"log"

	"github.com/dop251/goja"
)

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	EventHandler

	Type() DataType
	IsConst() bool

	SetValue(value interface{}) error
	GetValue() interface{}
}

func NewProperty(name string, dtype DataType, default_value interface{}, set *datastore.ValueVersionedSet, vm *goja.Runtime, constprop bool) (Property, error) {

	err := dtype.MustBeTypeOf(default_value)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create property, default value does not match data type")
	}

	var prop Property

	if !constprop {
		if dtype.IsPOD() {
			value, _ := set.GetOrCreateValue([]byte(name))

			//setup default value if needed
			res, err := value.HoldsValue()
			if err != nil {
				return nil, utils.StackError(err, "Cannot create property, datastore not accessible")
			}
			if !res {
				value.Write(default_value)
			}

			prop = &dataProperty{NewEventHandler(), dtype, *value}

		} else if dtype.IsType() {

			//type property needs to store parsings ast object which cannot be serialized, hence const only
			return nil, fmt.Errorf("Type property can only be const")

		} else {
			return nil, fmt.Errorf("Unknown type")
		}
	} else {
		if dtype.IsPOD() {
			prop = &constProperty{NewEventHandler(), dtype, default_value}

		} else if dtype.IsType() {
			prop = &typeProperty{NewEventHandler(), MustNewDataType("int")}
			prop.SetValue(default_value)

		} else {
			return nil, fmt.Errorf("Unknown type")
		}
	}

	//add all required events
	prop.AddEvent("onChanged", NewEvent(vm.NewObject(), vm, dtype))

	return prop, nil
}

//Data property
//**************

type dataProperty struct {
	eventHandler
	propertyType DataType
	db           datastore.ValueVersioned
}

func (self dataProperty) Type() DataType {
	return self.propertyType
}

func (self dataProperty) IsConst() bool {
	return false
}

func (self *dataProperty) SetValue(val interface{}) error {

	//check if the type is correct
	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return err
	}

	//store it
	if !self.db.IsValid() {
		return fmt.Errorf("Invalid database entry")
	}
	err = self.db.Write(val)
	if err != nil {
		return err
	}
	return self.GetEvent("onChanged").Emit(val)
}

func (self *dataProperty) GetValue() interface{} {

	if !self.db.IsValid() {
		return nil
	}

	val, err := self.db.Read()
	if err != nil {
		log.Printf("Error reading value: %s", err)
		return nil
	}
	return val
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

	//check if the type is correct
	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return err
	}

	self.value = val
	return self.GetEvent("onChanged").Emit(val)
}

func (self *constProperty) GetValue() interface{} {

	return self.value
}

type typeProperty struct {
	eventHandler
	data DataType
}

func (self typeProperty) Type() DataType {
	return MustNewDataType("type")
}

func (self typeProperty) IsConst() bool {
	return true
}

//we store the basic information, plain type string or parser result for object
func (self *typeProperty) SetValue(val interface{}) error {

	//check if the type is correct
	err := MustNewDataType("type").MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Cannot set type property: invalid argument")
	}

	self.data = val.(DataType)

	return nil
}

//we only return basic information, mailny for JS accessibility
func (self *typeProperty) GetValue() interface{} {
	return self.data.AsString()
}

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, dtype DataType, defaultVal interface{}, constprop bool) error
	GetProperty(name string) Property

	SetupJSProperties(vm *goja.Runtime, obj *goja.Object) error
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

func (self *propertyHandler) SetupJSProperties(vm *goja.Runtime, obj *goja.Object) error {

	for name, _ := range self.properties {

		var propname string = name
		getter := vm.ToValue(func(call goja.FunctionCall) goja.Value {
			return vm.ToValue(self.GetProperty(propname).GetValue())
		})

		setter := vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {
			p := self.GetProperty(propname)
			if p.IsConst() {
				//panic becomes exception in JS
				panic(vm.ToValue(fmt.Sprintf("Property %s is constant", propname)))
			}
			err := p.SetValue(call.Argument(0).Export())
			if err != nil {
				//panic becomes exception in JS
				panic(vm.ToValue(err.Error()))
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
