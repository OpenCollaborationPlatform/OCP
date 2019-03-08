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

type data struct {
	*object
	behaviourHandler

	children []identifier
}

func NewData(name string, parent identifier, rntm *Runtime) Object {
	return NewDataBaseClass(name, "Data", parent, rntm)
}

func NewDataBaseClass(name string, typename string, parent identifier, rntm *Runtime) *data {

	dat := data{
		NewObject(parent, name, typename, rntm),
		NewBehaviourHandler(),
		make([]identifier, 0),
	}

	return &dat
}

func (self *data) AddChild(obj Data) {

	self.children = append(self.children, obj.Id())
}

func (self *data) GetChildren() []Data {

	result := make([]Data, len(self.children))
	for i, child := range self.children {
		result[i] = self.rntm.objects[child]
	}
	return result
}

func (self *data) GetChildByName(name string) (Data, error) {

	for _, child := range self.children {
		if child.Name == name {
			return self.rntm.objects[child], nil
		}
	}
	return nil, fmt.Errorf("No such object available")
}

//override to handle children refcount additional to our own
func (self *data) IncreaseRefcount() error {
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
func (self *data) DecreaseRefcount() error {
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
