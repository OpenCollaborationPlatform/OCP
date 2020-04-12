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
	
	//Access a value by name. Coul be overriden by some objects, e.g. maps and vectors
	GetValueByName(name string) interface{}

	//Data hirarchy allows childs
	AddChild(obj Data)
	GetChildren() []Data
	GetChildByName(name string) (Data, error)								
	
	//Subobject handling is more than only childres
	//Hirarchy + dynamic objects, optional behaviour + object properties
	GetSubobjects(bhvr bool, prop bool) []Object
	GetSubobjectByName(name string, bhvr bool, prop bool) (Object, error)
}

type DataImpl struct {
	*object
	behaviourHandler

	children []Identifier
}

func NewData(id Identifier, parent Identifier, rntm *Runtime) (Object, error) {
	return NewDataBaseClass(id, parent, rntm)
}

func NewDataBaseClass(id Identifier, parent Identifier, rntm *Runtime) (*DataImpl, error) {

	obj, err := NewObject(id, parent, rntm)
	if err != nil {
		return nil, err
	}
	
	dat := DataImpl{
		obj,
		NewBehaviourHandler(),
		make([]Identifier, 0),
	}

	return &dat, nil
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

func (self *DataImpl) GetSubobjects(bhvr bool, prop bool) []Object {
	
	result := make([]Object, 0)
	
	//add hirarchy
	children := self.GetChildren()
	for _, child := range children {
		result = append(result, child)
	}
	
	
	//add behaviour
	if bhvr {
		bhvrs := self.Behaviours()
		for _, name := range bhvrs {
			result = append(result, self.GetBehaviour(name))
		}
	}
	
	//search object properties
	if prop {
		props := self.GetProperties()
		for _, name := range props {
			prop := self.GetProperty(name)
			if prop.Type().IsObject() {
				val := prop.GetValue()
				if val != nil {
					obj := val.(Object)
					result = append(result, obj)
				}
			}
		}
	}
	
	return result
}


func (self *DataImpl) GetSubobjectByName(name string, bhvr bool, prop bool) (Object, error) {
	
	//search hirarchy
	child, err := self.GetChildByName(name)
	if err == nil {
		return child, nil
	}
	
	//search behaviour
	if bhvr {
		if self.HasBehaviour(name) { 
			return self.GetBehaviour(name), nil
		}
	}
	
	//search object properties
	if prop {
		props := self.GetProperties()
		for _, name := range props {
			prop := self.GetProperty(name)
			if prop.Type().IsObject() {
				val := prop.GetValue()
				if val != nil {
					obj := val.(Object)
					if obj.Id().Name == name {
						return obj, nil
					}
				}
			}
		}
	}
	
	return nil, fmt.Errorf("No such name known")
}

func (self *DataImpl) GetValueByName(name string) interface{} {
	return nil
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
