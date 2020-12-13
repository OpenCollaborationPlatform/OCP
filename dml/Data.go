// Data descibes a Data object with its respective properties to store things in a database
package dml

import (
	"fmt"

	"github.com/ickby/CollaborationNode/utils"
)

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions, as well as holding Behaviours.
//It has no other special functionality. It is intended as dml grouping object as well as
//base object for all other data types
//Data does allow for children. Note that children are static values, they cannot change at
//runtime. Hence they are different to dynamic objects like lists etc. Children are
//used purely for the static DML hirarchy.
type Data interface {
	Object
	BehaviourHandler

	//Access a value by name. Could be overriden by some objects, e.g. maps and vectors
	GetValueByName(id Identifier, name string) interface{}

	//Data hirarchy allows childs
	AddChild(id Identifier, child Identifier) error
	GetChildren(id Identifier) ([]Identifier, error)
	GetChildByName(id Identifier, name string) (Identifier, error)

	//Subobject handling is more than only childrens
	//Hirarchy + dynamic objects, optional behaviour
	GetSubobjects(id Identifier, bhvr bool) ([]Identifier, error)
	GetSubobjectByName(id Identifier, name string, bhvr bool) (Identifier, error)

	Created(id Identifier) //emits onCreated event for this and all subobjects (not behaviours)
}

type DataImpl struct {
	*object
	behaviourHandler
}

func NewData(rntm *Runtime) (Object, error) {
	return NewDataBaseClass(rntm)
}

func NewDataBaseClass(rntm *Runtime) (*DataImpl, error) {

	obj, err := NewObject(rntm)
	if err != nil {
		return nil, err
	}

	dat := DataImpl{
		obj,
		NewBehaviourHandler(rntm),
	}

	dat.AddEvent("onCreated", NewEvent(dat.GetJSObject(), rntm))
	dat.AddEvent("onRemove", NewEvent(dat.GetJSObject(), rntm))

	return &dat, nil
}

func (self *DataImpl) AddChild(id Identifier, child Identifier) error {

	list, err := listFromStore(self.rntm.datastore, id, []byte("__children"))
	if err != nil {
		return err
	}
	_, err = list.Add(child)
	if err != nil {
		return utils.StackError(err, "Unable to add child to Data object")
	}
	return nil
}

func (self *DataImpl) GetChildren(id Identifier) ([]Identifier, error) {

	list, err := listFromStore(self.rntm.datastore, id, []byte("__children"))
	if err != nil {
		return nil, err
	}
	entries, err := list.GetEntries()
	if err != nil {
		return nil, utils.StackError(err, "Unable to query object children")
	}

	result := make([]Identifier, len(entries))
	for i, child := range entries {
		id, err := child.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read child entry")
		}
		result[i] = id.(Identifier)
	}
	return result, nil
}

func (self *DataImpl) GetChildByName(id Identifier, name string) (Identifier, error) {

	childs, err := self.GetChildren(id)
	if err != nil {
		return Identifier{}, err
	}
	for _, child := range childs {

		//all children are objects, hence we can use our own Name property to
		//access them, and get the correct name by the child ID
		childname := self.GetProperty("name").GetValue(child)
		if childname == name {
			return child, nil
		}
	}
	return Identifier{}, fmt.Errorf("No such object available")
}

func (self *DataImpl) GetSubobjects(id Identifier, bhvr bool) ([]Identifier, error) {

	result := make([]Identifier, 0)

	//add hirarchy
	children, err := self.GetChildren(id)
	if err != nil {
		return nil, err
	}
	result = append(result, children...)

	//add behaviour
	if bhvr {
		bhvrs, err := self.Behaviours(id)
		if err != nil {
			return nil, utils.StackError(err, "Unable to query behaviour manager")
		}
		for _, name := range bhvrs {
			bhvr, err := self.GetBehaviour(id, name)
			if err != nil {
				return nil, utils.StackError(err, "Unable to query behaviour manager")
			}
			result = append(result, bhvr)
		}
	}

	return result, nil
}

func (self *DataImpl) GetSubobjectByName(id Identifier, name string, bhvr bool) (Identifier, error) {

	//search hirarchy
	child, err := self.GetChildByName(id, name)
	if err == nil {
		return child, nil
	}

	//search behaviour
	if bhvr {
		if val, _ := self.HasBehaviour(id, name); val {
			return self.GetBehaviour(id, name)
		}
	}

	return Identifier{}, fmt.Errorf("No such name known")
}

func (self *DataImpl) GetValueByName(id Identifier, name string) (interface{}, error) {
	return nil, fmt.Errorf("No value with name %s available", name)
}

func (self *DataImpl) Created(id Identifier) {
	/*
		self.GetEvent("onCreated").Emit()

		for _, obj := range self.GetSubobjects(id, false) {

			data, ok := obj.(Data)
			if ok {
				data.Created()
			}
		}*/
}
