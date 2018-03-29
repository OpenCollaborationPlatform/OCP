package dml

import (
	"CollaborationNode/datastores"
	"fmt"
)

type Object interface {
	PropertyHandler
	EventHandler

	GetDataStore() datastore.Store
	AddChild(name string, obj Object) error
}

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions. It has no other special
//functionality. It is intended as dml grouping object as well as base object for
//other types
type Group struct {
	propertyHandler
	eventHandler

	store     datastore.Store
	functions map[string]interface{}
	children  map[string]Object
}

func (self *Group) AddChild(name string, obj Object) error {

	_, ok := self.children[name]
	if ok {
		return fmt.Errorf("Child already exist")
	}
	self.children[name] = obj
	return nil
}

func (self *Group) GetDataStore() datastore.Store {
	return self.store
}

func NewGroup(ds *datastore.Datastore, name string) Object {
	grp := Group{
		NewPropertyHandler(),
		NewEventHandler(),
		ds.GetOrCreateStore(datastore.KeyValue, name),
		make(map[string]interface{}, 0),
		make(map[string]Object, 0),
	}

	id, err := NewProperty("id", String, grp.GetDataStore())
	if err != nil {
		return nil
	}
	grp.AddProperty("id", id)

	return &grp
}
