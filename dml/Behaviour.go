// Transaction.go
package dml

import (
	"fmt"
	"github.com/ickby/CollaborationNode/utils"
)

/*
 * A Behaviour is a set of methods and events that define a certain way an object reacts on
 * certain circumstances or in certain situations. The language interpreter handles a object
 * according to the behaviours it has adopted.  The behaviour does
 *  - Provide properties to access special data decribing the behaviour state
 *  - Provide Events that fire during execution of the behaviour
 *  - Provides Methods to override and hence customice the behaviour
 */
type Behaviour interface {
	Object

	//SetupDefaults()
	Copy() Behaviour
}

func NewBehaviour(id Identifier, parent Identifier, rntm *Runtime) (*behaviour, error) {

	obj, err := NewObject(id, parent, rntm)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create base object for behaviour %s", id.String())
	}
	result := behaviour{obj}

	//add default behaviour properties
	err = result.AddProperty(`recursive`, MustNewDataType("bool"), false, true)
	if err != nil {
		return nil, utils.StackError(err, "Unable to add object to %s", id.String())
	}

	return &result, nil
}

//Behaviour implementation with ability to have default methods
type behaviour struct {
	*object

	//defaults methodHandler
}

/*
//all methods that are not provided by the user are setup from the behaviour, so that
//to it appears that the user can override the behaviour
//TODO: check for correct arguments types
func (self *behaviour) SetupDefaults() {

	//methods
	defmeths := self.defaults.Methods()
	for _, defmeth := range defmeths {
		if !self.HasMethod(defmeth) {
			self.AddMethod(defmeth, self.defaults.GetMethod(defmeth))
		}
	}
}*/

//creates identical copy of this behaviour witch is functional independend
func (self *behaviour) Copy() {

}

/*
 * Handles different behaviours added to a object
 */
type BehaviourHandler interface {

	//handle the behaviours
	HasBehaviour(string) bool
	GetBehaviour(string) Behaviour
	AddBehaviour(string, Behaviour) error
	Behaviours() []string
}

func NewBehaviourHandler() behaviourHandler {
	return behaviourHandler{make(map[string]Behaviour, 0)}
}

type behaviourHandler struct {
	behaviours map[string]Behaviour
}

func (self *behaviourHandler) HasBehaviour(name string) bool {
	_, ok := self.behaviours[name]
	return ok
}

func (self *behaviourHandler) AddBehaviour(name string, behaviour Behaviour) error {

	if self.HasBehaviour(name) {
		return fmt.Errorf("Behaviour already set: %v", name)
	}
	self.behaviours[name] = behaviour
	return nil
}

func (self *behaviourHandler) GetBehaviour(name string) Behaviour {
	return self.behaviours[name]
}

func (self *behaviourHandler) Behaviours() []string {

	meths := make([]string, 0)
	for name, _ := range self.behaviours {
		meths = append(meths, name)
	}

	return meths
}
