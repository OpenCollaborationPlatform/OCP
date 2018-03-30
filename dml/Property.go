package dml

import (
	"CollaborationNode/datastores"
	"fmt"
	"log"

	"github.com/dop251/goja"
)

type PropertyType int

const (
	String PropertyType = 1
	Int    PropertyType = 2
	Float  PropertyType = 3
	Bool   PropertyType = 4
	File   PropertyType = 5
	Raw    PropertyType = 6
)

func typeToString(t PropertyType) string {

	switch t {
	case String:
		return "string"
	case Int:
		return "int"
	case Float:
		return "float"
	case Bool:
		return "bool"
	case File:
		return "file"
	case Raw:
		return "raw"
	}
	return ""
}

func stringToType(t string) PropertyType {

	switch t {
	case "string":
		return String
	case "int":
		return Int
	case "float":
		return Float
	case "bool":
		return Bool
	case "file":
		return File
	case "raw":
		return Raw
	}
	return Int
}

func mustBeType(pt PropertyType, val interface{}) error {
	//check if the type is correct
	switch val.(type) {
	case int, int32, int64:
		if pt != Int {
			return fmt.Errorf(`wrong type, got 'int' and expected '%s'`, typeToString(pt))
		}
	case float32, float64:
		if pt != Float {
			return fmt.Errorf(`wrong type, got 'float' and expected '%s'`, typeToString(pt))
		}
	case string:
		if pt != String {
			return fmt.Errorf(`wrong type, got 'string' and expected '%s'`, typeToString(pt))
		}
	case bool:
		if pt != Bool {
			return fmt.Errorf(`wrong type, got 'bool' and expected '%s'`, typeToString(pt))
		}
	default:
		return fmt.Errorf("Unknown type: %T", val)
	}
	return nil
}

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	EventHandler

	Type() PropertyType

	SetValue(value interface{}) error
	GetValue() interface{}
}

func NewProperty(name string, dtype PropertyType, store datastore.Store) (Property, error) {

	var prop Property

	switch dtype {
	case Int, Float, String, Bool:
		db := store.GetOrCreateEntry(name)
		prop = &DataProperty{NewEventHandler(), dtype, db}
	default:
		return nil, fmt.Errorf("Unknown type")
	}

	//add all required events
	prop.AddEvent("onChanged", NewEvent(dtype))

	return prop, nil
}

//Data properties
//*********************
type DataProperty struct {
	eventHandler
	propertyType PropertyType
	db           datastore.Entry
}

func (self DataProperty) Type() PropertyType {
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

	for name, _ := range self.properties {

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

		err := obj.DefineAccessorProperty(name, getter, setter, goja.FLAG_TRUE, goja.FLAG_TRUE)
		if err != nil {
			return err
		}
	}
	return nil
}
