package dml

import (
	"CollaborationNode/datastores"
	"fmt"
	"log"

	"github.com/dop251/goja"
)

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	EventHandler

	Type() DataType

	SetValue(value interface{}) error
	GetValue() interface{}
}

//Data property
//**************

func NewProperty(name string, dtype DataType, entry datastore.KeyValueEntry, vm *goja.Runtime) (Property, error) {

	var prop Property

	switch dtype {
	case Int, Float, String, Bool:
		db := entry.GetOrCreateKey([]byte(name))
		prop = &dataProperty{NewEventHandler(), dtype, db}
	default:
		return nil, fmt.Errorf("Unknown type")
	}

	//add all required events
	prop.AddEvent("onChanged", NewEvent(vm, dtype))

	return prop, nil
}

type dataProperty struct {
	eventHandler
	propertyType DataType
	db           datastore.KeyValuePair
}

func (self dataProperty) Type() DataType {
	return self.propertyType
}

func (self *dataProperty) SetValue(val interface{}) error {

	//check if the type is correct
	err := mustBeType(self.propertyType, val)
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

type Data interface {
	Object
}

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, dtype DataType) error
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
			err := self.GetProperty(propname).SetValue(call.Argument(0).Export())
			if err != nil {
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
