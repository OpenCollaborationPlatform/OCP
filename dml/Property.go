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
	MethodHandler

	Type() DataType

	SetValue(value interface{}) error
	GetValue() interface{}
}

func NewProperty(name string, dtype DataType, store datastore.Store, vm *goja.Runtime) (Property, error) {

	var prop Property

	switch dtype {
	case Int, Float, String, Bool:
		db := store.GetOrCreateEntry(name)
		prop = &DataProperty{NewEventHandler(), NewMethodHandler(), dtype, db}
	default:
		return nil, fmt.Errorf("Unknown type")
	}

	//add all required events
	prop.AddEvent("onChanged", NewEvent(vm, dtype))

	return prop, nil
}

//Data properties
//*********************
type DataProperty struct {
	eventHandler
	methodHandler
	propertyType DataType
	db           datastore.Entry
}

func (self DataProperty) Type() DataType {
	return self.propertyType
}

func (self *DataProperty) SetValue(val interface{}) error {

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

func (self *DataProperty) GetValue() interface{} {

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

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, evt Property) error
	GetProperty(name string) Property

	SetupJSProperties(vm *goja.Runtime, obj *goja.Object) error
}

func NewPropertyHandler() propertyHandler {
	return propertyHandler{make(map[string]Property, 0)}
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

		jsprop := vm.NewObject()
		obj.Set(name, jsprop)
		var propname string = name
		getter := vm.ToValue(func(call goja.FunctionCall) goja.Value {
			return vm.ToValue(self.GetProperty(propname).GetValue())
		})
		setter := vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {
			err := self.GetProperty(propname).SetValue(call.Argument(0).Export())
			if err != nil {
				panic(vm.ToValue(err))
			}
			return
		})

		err := jsprop.DefineAccessorProperty("value", getter, setter, goja.FLAG_TRUE, goja.FLAG_TRUE)
		if err != nil {
			return err
		}

		prop.SetupJSMethods(vm, jsprop)

	}
	return nil
}
