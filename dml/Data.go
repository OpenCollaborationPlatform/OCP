// Data descibes a Data object with its respective properties to store things in a database
package dml

import (
	"fmt"
	"log"

	"github.com/dop251/goja"

	"CollaborationNode/datastores"
)

//Data property
//**************

func NewDataProperty(name string, dtype DataType, store datastore.Store, vm *goja.Runtime) (Property, error) {

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

type Data interface {
	Object

	GetDataStore() datastore.Store
}

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions. It has no other special
//functionality. It is intended as dml grouping object as well as base object for
//other types
type data struct {
	*object

	store datastore.Store
	id    string
}

func (self *data) Id() string {
	return self.id
}

func (self *data) GetDataStore() datastore.Store {
	return self.store
}

//the missing add property method to fully define the Object interface
func (self *data) AddProperty(name string, dtype DataType) error {

	if self.HasProperty(name) {
		return fmt.Errorf("Property %s already exists", name)
	}

	//we add dataproperties!
	prop, err := NewDataProperty(name, dtype, self.GetDataStore(), self.GetJSRuntime())
	if err != nil {
		return err
	}

	self.propertyHandler.properties[name] = prop
	return nil
}

func NewData(ds *datastore.Datastore, name string, vm *goja.Runtime) Object {

	obj := NewObject(name, vm)
	dat := data{obj, ds.GetOrCreateStore(datastore.KeyValue, name), name}

	dat.AddProperty("id", String)

	return &dat
}
