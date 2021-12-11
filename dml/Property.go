package dml

import (
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/dop251/goja"
)

//Defines a interface that is called by a Property on changes
type PropertyChangeNotifyer interface {
	BeforePropertyChange(Identifier, string) error
	PropertyChanged(Identifier, string) error
}

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	//	EventHandler
	Type() DataType
	IsConst() bool
	IsReadOnly() bool

	SetValue(id Identifier, value interface{}) error
	GetValue(id Identifier) (interface{}, error)

	//required for startup, sets the initial value
	SetDefaultValue(value interface{}) error
	GetDefaultValue() interface{}
	InitializeDB(id Identifier) error
}

func NewProperty(name string, dtype DataType, default_value interface{}, constprop bool) (Property, error) {

	err := dtype.MustBeTypeOf(default_value)
	if err != nil {
		fmt.Printf("\n type: %v, default: %v\n", dtype, default_value)
		return nil, utils.StackError(err, "Cannot create property, default value does not match data type")
	}

	var prop Property

	if !constprop {
		prop = &dataProperty{name, dtype, nil, nil, nil}
	} else {
		prop = &constProperty{dtype, nil}
	}

	err = prop.SetDefaultValue(default_value)
	if err != nil {
		return nil, utils.StackError(err, "Unable to use provided value as default for property")
	}

	return prop, nil
}

func NewFuncProperty(name string, getter func(Identifier) (interface{}, error), constprop bool) (Property, error) {

	return &funcProperty{constprop, getter}, nil
}

//Data property
//**************

type dataProperty struct {
	name         string
	propertyType DataType
	default_val  interface{}
	rntm         *Runtime
	callback     PropertyChangeNotifyer
}

func (self dataProperty) Type() DataType {
	return self.propertyType
}

func (self dataProperty) IsConst() bool {
	return false
}

func (self dataProperty) IsReadOnly() bool {
	return false
}

func (self *dataProperty) SetValue(id Identifier, val interface{}) error {

	//check if the type is correct
	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return err
	}

	val = UnifyDataType(val)

	err = self.callback.BeforePropertyChange(id, self.name)
	if err != nil {
		return err
	}

	//store it
	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}
	if ok, _ := dbValue.Exists(); !ok {
		return newInternalError(Error_Fatal, "Invalid database entry")
	}

	err = dbValue.Write(val)

	if err != nil {
		return utils.StackError(err, "Unable to write to DB")
	}
	return self.callback.PropertyChanged(id, self.name)
}

func (self *dataProperty) InitializeDB(id Identifier) error {

	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return err
	}
	if ok, _ := dbValue.Exists(); !ok {
		return newInternalError(Error_Fatal, "Invalid database entry")
	}
	if err := dbValue.Write(self.default_val); err != nil {
		return utils.StackError(err, "Unable to write default value into datastore")
	}
	return nil
}

func (self *dataProperty) SetDefaultValue(val interface{}) error {

	err := self.propertyType.MustBeTypeOf(val)
	if err != nil {
		return utils.StackError(err, "Unable to set property defult value")
	}

	val = UnifyDataType(val)

	self.default_val = val
	return nil
}

func (self *dataProperty) GetDefaultValue() interface{} {
	return self.default_val
}

func (self *dataProperty) GetValue(id Identifier) (interface{}, error) {

	dbValue, err := valueVersionedFromStore(self.rntm.datastore, id, []byte(self.name))
	if err != nil {
		return nil, utils.StackError(err, "Unable to access DB for property value")
	}

	if !dbValue.IsValid() {
		return nil, newInternalError(Error_Setup_Invalid, "property has no valid value")
	}

	val, err := dbValue.Read()
	if err != nil {
		return nil, utils.StackError(err, "Error reading database vaue for property access")
	}

	val = UnifyDataType(val)
	return val, nil
}

func (self *dataProperty) getDSKey(id Identifier) datastore.Key {
	return datastore.NewKey(datastore.ValueType, true, id.Hash(), []byte(self.name))
}

//Const property
//**************

type constProperty struct {
	//	eventHandler
	propertyType DataType
	value        interface{}
}

func (self constProperty) Type() DataType {
	return self.propertyType
}

func (self constProperty) IsConst() bool {
	return true
}

func (self constProperty) IsReadOnly() bool {
	return true
}

func (self *constProperty) SetValue(id Identifier, val interface{}) error {

	return newUserError(Error_Operation_Invalid, "Const property cannot set value")
}

func (self *constProperty) GetValue(id Identifier) (interface{}, error) {

	return self.value, nil
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

func (self *constProperty) GetDefaultValue() interface{} {
	return self.value
}

func (self *constProperty) InitializeDB(id Identifier) error {
	return nil
}

//Const property
//**************

type funcProperty struct {
	isConst bool
	getter  func(Identifier) (interface{}, error)
}

func (self funcProperty) Type() DataType {
	return MustNewDataType("none")
}

func (self funcProperty) IsConst() bool {
	return self.isConst
}

func (self funcProperty) IsReadOnly() bool {
	return true
}

func (self *funcProperty) SetValue(id Identifier, val interface{}) error {

	return newUserError(Error_Operation_Invalid, "ReadOnly property cannot set value")
}

func (self *funcProperty) GetValue(id Identifier) (interface{}, error) {

	return self.getter(id)
}

func (self *funcProperty) SetDefaultValue(val interface{}) error {

	//we do not need a default value
	return nil
}

func (self *funcProperty) GetDefaultValue() interface{} {
	return nil
}

func (self *funcProperty) InitializeDB(id Identifier) error {
	return nil
}

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, dtype DataType, defaultVal interface{}, constprop bool) error
	AddFuncProperty(name string, getter func(Identifier) (interface{}, error), constprop bool) error
	GetProperty(name string) Property
	GetProperties() []string

	SetupProperties(rntm *Runtime, jsobj *goja.Object, cb PropertyChangeNotifyer) error
	InitializePropertyDB(Identifier) error
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

func (self *propertyHandler) AddProperty(name string, dtype DataType, default_val interface{}, constprop bool) error {

	if self.HasProperty(name) {
		return newInternalError(Error_Setup_Invalid, fmt.Sprintf("Property %s already exists", name))
	}

	//we add properties
	prop, err := NewProperty(name, dtype, default_val, constprop)
	if err != nil {
		return err
	}

	//everthing went without error, now we can set this property
	self.properties[name] = prop
	return nil
}

func (self *propertyHandler) AddFuncProperty(name string, getter func(Identifier) (interface{}, error), constprop bool) error {

	if self.HasProperty(name) {
		return newInternalError(Error_Setup_Invalid, fmt.Sprintf("Property %s already exists", name))
	}

	prop, err := NewFuncProperty(name, getter, constprop)
	if err != nil {
		return err
	}

	//everthing went without error, now we can set this property
	self.properties[name] = prop
	return nil
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

func (self *propertyHandler) SetupProperties(rntm *Runtime, proto *goja.Object, cb PropertyChangeNotifyer) error {

	keys := proto.Keys()

	for name, prop := range self.properties {

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

		//set the property callback
		if dp, ok := prop.(*dataProperty); ok {
			dp.callback = cb
			dp.rntm = rntm
		}

		//expose to JavaScript
		var propname string = name
		getter := rntm.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {

			//get the identifier of the object that accesses the property
			id := call.This.ToObject(rntm.jsvm).Get("identifier").Export()
			identifier, ok := id.(Identifier)
			if !ok {
				panic(rntm.jsvm.ToValue(newInternalError(Error_Fatal, "Called object does not have identifier setup correctly: %v", id)))
			}

			//return ob object is different than POD
			val, err := self.GetProperty(propname).GetValue(identifier)
			if err != nil {
				return rntm.jsvm.ToValue(err)
			}

			//handle returning identifier or dmlset (and lists of those)
			return toGojaValue(val, rntm)
		})

		setter := rntm.jsvm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {

			if len(call.Arguments) != 1 {
				//panic becomes exception in JS
				panic(rntm.jsvm.ToValue(newUserError(Error_Arguments_Wrong, "Property setting requires exactly one argument")))
			}

			//get the identifier of the object that accesses the property
			id := call.This.ToObject(rntm.jsvm).Get("identifier").Export()
			identifier, ok := id.(Identifier)
			if !ok {
				panic(rntm.jsvm.ToValue(newInternalError(Error_Fatal, "Called object does not have identifier setup correctly")))
			}

			p := self.GetProperty(propname)
			if p.IsConst() {
				//panic becomes exception in JS
				panic(rntm.jsvm.ToValue(newUserError(Error_Operation_Invalid, fmt.Sprintf("Property %s is constant", propname))))
			}

			//convert goja args to go ones
			args := extractValues(call.Arguments, rntm)
			err := p.SetValue(identifier, args[0])
			if err != nil {
				//panic becomes exception in JS
				panic(rntm.jsvm.ToValue(err))
			}
			return
		})
		err := proto.DefineAccessorProperty(name, getter, setter, goja.FLAG_FALSE, goja.FLAG_TRUE)

		if err != nil {
			return err
		}
	}
	return nil
}

func (self *propertyHandler) InitializePropertyDB(id Identifier) error {

	//we write the property default values into the db
	for _, prop := range self.properties {
		prop.InitializeDB(id)
	}

	return nil
}
