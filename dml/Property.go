package dml

import (
	"fmt"

	"github.com/dop251/goja"
)

type DataType int

const (
	String DataType = 1
	Int    DataType = 2
	Float  DataType = 3
	Bool   DataType = 4
	File   DataType = 5
	Raw    DataType = 6
)

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	EventHandler

	Type() DataType

	SetValue(value interface{}) error
	GetValue() interface{}
}

func MakeProperty(dtype DataType) (Property, error) {

	var prop Property

	switch dtype {
	case Int:
		prop = &intProperty{}
	case Float:
		prop = &floatProperty{}
	case String:
		prop = &stringProperty{}
	case Bool:
		prop = &boolProperty{}
	default:
		return nil, fmt.Errorf("Unknown type")
	}

	//add all required events
	prop.AddEvent("onChange", makeEvent(dtype))

	return prop, nil
}

//Integer data property
//*********************
type intProperty struct {
	eventHandler
	value int64
	js    goja.Value
}

func (self intProperty) Type() DataType {
	return Int
}

func (self *intProperty) SetValue(val interface{}) error {

	intVal, ok := val.(int64)
	if !ok {
		return fmt.Errorf("Wrong type: must be int64")
	}

	self.value = intVal
	return self.GetEvent("onChanged").Emit(self.value)
}

func (self *intProperty) GetValue() interface{} {
	return self.value
}

//Float data property
//*******************
type floatProperty struct {
	eventHandler
	value float64
}

func (self floatProperty) Type() DataType {
	return Float
}

func (self *floatProperty) SetValue(val interface{}) error {

	floatVal, ok := val.(float64)
	if !ok {
		return fmt.Errorf("Wrong type: must be float64")
	}

	self.value = floatVal
	return self.GetEvent("onChanged").Emit(self.value)
}

func (self *floatProperty) GetValue() interface{} {
	return self.value
}

//String data property
//*********************
type stringProperty struct {
	eventHandler
	value string
}

func (self stringProperty) Type() DataType {
	return String
}

func (self *stringProperty) SetValue(val interface{}) error {

	stringVal, ok := val.(string)
	if !ok {
		return fmt.Errorf("Wrong type: must be string")
	}

	self.value = stringVal
	return self.GetEvent("onChanged").Emit(self.value)
}

func (self *stringProperty) GetValue() interface{} {
	return self.value
}

//Bool data property
//*********************
type boolProperty struct {
	eventHandler
	value bool
}

func (self boolProperty) Type() DataType {
	return Bool
}

func (self *boolProperty) SetValue(val interface{}) error {

	boolVal, ok := val.(bool)
	if !ok {
		return fmt.Errorf("Wrong type: must be bool")
	}

	self.value = boolVal
	return self.GetEvent("onChanged").Emit(self.value)
}

func (self *boolProperty) GetValue() interface{} {
	return self.value
}

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, evt Property) error
	GetProperty(name string) *Property

	SetupJSProperties(obj *goja.Object) error
}

//unifies handling of multiple properties
type propertyHandler struct {
	properties map[string]Property
}

func (self *propertyHandler) HasProperty(name string) bool {

	_, ok := self.properties[name]
	return ok
}

func (self *propertyHandler) AddProperty(name string, prop Property) error {

	if self.HasProperty(name) {
		return fmt.Errorf("Property already exists")
	}
	self.properties[name] = prop
	return nil
}

func (self *propertyHandler) GetProperty(name string) Property {

	return self.properties[name]
}

func (self *propertyHandler) SetupJSProperties(vm *goja.Runtime, obj *goja.Object) error {

	for name, prop := range self.properties {

		getter := vm.ToValue(func(call goja.FunctionCall) goja.Value {
			return vm.ToValue(prop.GetValue())
		})
		setter := vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {
			prop.SetValue(call.Argument(0).Export())
			return
		})

		err := obj.DefineAccessorProperty(name, getter, setter, goja.FLAG_TRUE, goja.FLAG_TRUE)
		if err != nil {
			return err
		}
	}
	return nil
}
