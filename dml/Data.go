// Data descibes a Data object with its respective properties to store things in a database
package dml

import (
	"fmt"
)

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions, as well as holding Behaviours.
//It has no other special functionality. It is intended as dml grouping object as well as
//base object for all other data types
type Data interface {
	Object
	BehaviourHandler

	//Data hirarchy allows childs
	AddChild(obj Data)
	GetChildren() []Data
	GetChildByName(name string) (Data, error)
}

type DataImpl struct {
	*object
	behaviourHandler

	children []Identifier
}

func NewData(name string, parent Identifier, rntm *Runtime) Object {
	return NewDataBaseClass(name, "Data", parent, rntm)
}

func NewDataBaseClass(name string, typename string, parent Identifier, rntm *Runtime) *DataImpl {

	dat := DataImpl{
		NewObject(parent, name, typename, rntm),
		NewBehaviourHandler(),
		make([]Identifier, 0),
	}

	return &dat
}

func (self *DataImpl) AddChild(obj Data) {

	self.children = append(self.children, obj.Id())
}

func (self *DataImpl) GetChildren() []Data {

	result := make([]Data, len(self.children))
	for i, child := range self.children {
		result[i] = self.rntm.objects[child]
	}
	return result
}

func (self *DataImpl) GetChildByName(name string) (Data, error) {

	for _, child := range self.children {
		if child.Name == name {
			return self.rntm.objects[child], nil
		}
	}
	return nil, fmt.Errorf("No such object available")
}

//override to handle children refcount additional to our own
func (self *DataImpl) IncreaseRefcount() error {
	//increase child refcount
	for _, child := range self.children {
		res, ok := self.rntm.objects[child]
		if ok {
			err := res.IncreaseRefcount()
			if err != nil {
				return err
			}
		}
	}
	//now increase our own
	return self.object.IncreaseRefcount()
}

//override to handle children refcount additional to our own
func (self *DataImpl) DecreaseRefcount() error {
	//decrease child refcount
	for _, child := range self.children {
		res, ok := self.rntm.objects[child]
		if ok {
			err := res.DecreaseRefcount()
			if err != nil {
				return err
			}
		}
	}
	//now decrease our own
	return self.object.DecreaseRefcount()
}

/*******************************************************************************/

//A data object that is able to create or remove new data objects
type DynamicData interface {
	Data

	Load() error //when loading on startup the object must recreat all data objects belonging to it
}
