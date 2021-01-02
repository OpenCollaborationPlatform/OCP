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
	GetValueByName(id Identifier, name string) (interface{}, error)

	//Data hirarchy allows childs. Here we add the structure and logic by
	//adding static objects. Database access by identifiers is handled seperatly
	AddChildObject(Data)
	GetChildObjects() []Data

	//Data hirarchy allows childs
	AddChildIdentifier(Identifier, Identifier) error
	GetChildIdentifiers(Identifier) ([]Identifier, error)
	GetChildIdentifierByName(Identifier, string) (Identifier, error)

	//little convinience function for children hirarchy combining objects and IDs
	GetChildren(Identifier) ([]dmlSet, error)
	GetChildByName(Identifier, string) (dmlSet, error)

	//Subobject handling is more than only childrens
	//Hirarchy + dynamic objects, optional behaviours
	GetSubobjects(id Identifier, bhvr bool) ([]dmlSet, error)
	GetSubobjectByName(id Identifier, name string, bhvr bool) (dmlSet, error)

	Created(id Identifier) error //emits onCreated event for this and all subobjects (not behaviours)
}

type DataImpl struct {
	*object
	behaviourHandler

	children []Data
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
		make([]Data, 0),
	}

	dat.AddEvent(NewEvent("onCreated", dat.GetJSPrototype(), rntm))
	dat.AddEvent(NewEvent("onRemove", dat.GetJSPrototype(), rntm))

	return &dat, nil
}

func (self *DataImpl) AddChildObject(child Data) {
	self.children = append(self.children, child)
}

func (self *DataImpl) GetChildObjects() []Data {
	return self.children
}

func (self *DataImpl) AddChildIdentifier(id Identifier, child Identifier) error {

	list, err := self.GetDBList(id, []byte("__children"))
	if err != nil {
		return err
	}
	_, err = list.Add(child)
	if err != nil {
		return utils.StackError(err, "Unable to add child to Data object")
	}
	return nil
}

func (self *DataImpl) GetChildIdentifiers(id Identifier) ([]Identifier, error) {

	list, err := self.GetDBList(id, []byte("__children"))
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
		result[i] = *id.(*Identifier)
	}
	return result, nil
}

func (self *DataImpl) GetChildIdentifierByName(id Identifier, name string) (Identifier, error) {

	childs, err := self.GetChildIdentifiers(id)
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to access child identifiers in DB")
	}
	for _, child := range childs {

		if child.Name == name {
			return child, nil
		}
	}
	return Identifier{}, fmt.Errorf("No such object available")
}

func (self *DataImpl) GetChildren(id Identifier) ([]dmlSet, error) {

	childIDs, err := self.GetChildIdentifiers(id)
	if err != nil {
		return nil, err
	}

	result := make([]dmlSet, len(childIDs))
	for i, childID := range childIDs {
		childDT, err := self.GetDataType(childID)
		if err != nil {
			return nil, utils.StackError(err, "Unable to query child datatype")
		}
		result[i] = dmlSet{obj: self.rntm.objects[childDT].(Data), id: childID}
	}
	return result, nil
}

func (self *DataImpl) GetChildByName(id Identifier, name string) (dmlSet, error) {

	childID, err := self.GetChildIdentifierByName(id, name)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to find child in object")
	}

	childDT, err := self.GetDataType(childID)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to query child datatype")
	}

	return dmlSet{obj: self.rntm.objects[childDT].(Data), id: childID}, nil
}

func (self *DataImpl) GetSubobjects(id Identifier, bhvr bool) ([]dmlSet, error) {

	result := make([]dmlSet, 0)

	//add hirarchy
	children, err := self.GetChildren(id)
	if err != nil {
		return nil, err
	}
	result = append(result, children...)

	//add behaviour
	if bhvr {
		bhvrs := self.Behaviours()
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

func (self *DataImpl) GetSubobjectByName(id Identifier, name string, bhvr bool) (dmlSet, error) {

	//search hirarchy
	child, err := self.GetChildByName(id, name)
	if err == nil {
		return child, nil
	}

	//search behaviour
	if bhvr {
		if self.HasBehaviour(name) {
			return self.GetBehaviour(id, name)
		}
	}

	return dmlSet{}, fmt.Errorf("No such name known")
}

func (self *DataImpl) GetValueByName(id Identifier, name string) (interface{}, error) {
	return nil, fmt.Errorf("No value with name %s available", name)
}

func (self *DataImpl) Created(id Identifier) error {

	err := self.GetEvent("onCreated").Emit(id)
	if err != nil {
		return err
	}

	subs, err := self.GetSubobjects(id, false)
	if err != nil {
		return err
	}
	for _, dbSet := range subs {

		data, ok := dbSet.obj.(Data)
		if ok {
			err := data.Created(dbSet.id)
			if err != nil {
				return err
			}

		} else {
			return fmt.Errorf("Data subobject not accessed correctly")
		}
	}
	return nil
}
