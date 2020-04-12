package dml

import (
	"fmt"
	datastore "github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
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

func NewProperty(name string, dtype DataType, default_value interface{}, set *datastore.ValueVersionedSet, rntm *Runtime, constprop bool) (Property, error) {

	err := dtype.MustBeTypeOf(default_value)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create property, default value does not match data type")
	}

	var prop Property

	if !constprop {

		if dtype.IsPOD() || dtype.IsObject() {

			//setup default value if needed
			value, _ := set.GetOrCreateValue([]byte(name))
			res, err := value.HoldsValue()
			if err != nil {
				return nil, utils.StackError(err, "Cannot create property, datastore not accessible")
			}
			if !res {
				value.Write(default_value)
			}
			prop = &dataProperty{NewEventHandler(), dtype, *value, rntm}

		} else if dtype.IsType() {
			//setup default value if needed
			value, _ := set.GetOrCreateValue([]byte(name))
			res, err := value.HoldsValue()
			if err != nil {
				return nil, utils.StackError(err, "Cannot create property, datastore not accessible")
			}
			if !res {
				dt := default_value.(DataType)
				value.Write(dt.AsString())
			}
			prop = &typeProperty{NewEventHandler(), *value}

		} else {
			return nil, fmt.Errorf("Unknown type")
		}
	} else {
		if dtype.IsPOD() {
			prop = &constProperty{NewEventHandler(), dtype, default_value}

		} else if dtype.IsType() {
			prop = &constTypeProperty{NewEventHandler(), MustNewDataType("int")}
			prop.SetValue(default_value)

		} else {
			return nil, fmt.Errorf("Unknown type")
		}
	}

	//add all required events
	prop.AddEvent("onBeforeChange", NewEvent(rntm.jsvm.NewObject(), rntm))
	prop.AddEvent("onChanged", NewEvent(rntm.jsvm.NewObject(), rntm))

	return prop, nil
}

//Data property
//**************

type dataProperty struct {
	eventHandler
	propertyType DataType
	db           datastore.ValueVersioned
	rntm 		*Runtime
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
	
	//check if it is a object and handle it accordingly
	obj, isobj := val.(Object)
	if isobj {
		//increase ref count, as we store it!
		err := obj.IncreaseRefcount()
		if err != nil {
			return utils.StackError(err, "Unable to increase refcount")
		}
	}
	
	//check if the old, currently stored type is a object, as than we need to 
	//decrease refcount
	if self.propertyType.IsObject() || self.propertyType.IsComplex() {
		val, err := self.db.Read()
		if err != nil {
			return utils.StackError(err, "Unable to read current value")
		}
		str, ok := val.(string)
		if ok {	
			id, err := IdentifierFromEncoded(str)
			if err == nil {
				obj, ok := self.rntm.objects[id]
				if (ok) {
					obj.DecreaseRefcount()
				}
			}
		}
	}

	//store it
	if !self.db.IsValid() {
		return fmt.Errorf("Invalid database entry")
	}

	self.GetEvent("onBeforeChange").Emit(val)
	if isobj {
		err = self.db.Write(obj.Id().Encode())
	
	} else {
		err = self.db.Write(val)
	}
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
	
	//check if the type is a object
	if self.propertyType.IsObject() || self.propertyType.IsComplex() {
		str, ok := val.(string)
		if ok {
			id, err := IdentifierFromEncoded(str)
			if err == nil {
				obj, ok := self.rntm.objects[id]
				if !ok {
					log.Printf("Error reading object value: invalid identifier")
					return nil
				}
				return obj
			}
		
		} else  {
			return nil
		}
	}
	
	return val
}

type typeProperty struct {
	eventHandler
	db datastore.ValueVersioned
}

func (self typeProperty) Type() DataType {
	return MustNewDataType("type")
}

func (self typeProperty) IsConst() bool {
	return false
}

//we store the basic information, plain type string or parser result for object
func (self *typeProperty) SetValue(val interface{}) error {

	//check if the type is correct
	err := MustNewDataType("type").MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Cannot set type property: invalid argument")
	}

	self.GetEvent("onBeforeChange").Emit(val)

	data := val.(DataType)
	self.db.Write(data.AsString())

	return self.GetEvent("onChanged").Emit(val)
}

//we only return basic information, mailny for JS accessibility
func (self *typeProperty) GetValue() interface{} {

	data, err := self.db.Read()
	if err != nil {
		log.Printf("Error reading value: %s", err)
		return nil
	}
	return DataType{data.(string)}
}

func (self *typeProperty) GetDataType() DataType {

	data, err := self.db.Read()
	if err != nil {
		log.Printf("Cannot access datastore: %v", err)
	}
	return DataType{data.(string)}
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

//we store the basic information, plain type string or parser result for object
func (self *constTypeProperty) SetValue(val interface{}) error {

	//check if the type is correct
	err := MustNewDataType("type").MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Cannot set type property: invalid argument")
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
			args := extractArgs(call.Arguments, rntm)
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
