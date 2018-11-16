// Transaction.go
package dml

import (
	"CollaborationNode/datastores"

	"github.com/dop251/goja"
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

	SetupDefaults()
	Copy() Behaviour
}

func NewBehaviour(parent Object, name string, oType string, vm *goja.Runtime, store *datastore.Datastore) (*behaviour, error) {

	result := behaviour{
		NewObject(parent, name, oType, vm, store),
		NewMethodHandler(),
	}

	//add default behaviour properties
	err := result.AddProperty(`recursive`, Bool, true)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

//Behaviour implementation with ability to have default methods
type behaviour struct {
	*object

	defaults methodHandler
}

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
}

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
	SetBehaviour(string, Behaviour) error
	Behaviours() []string

	SetupBehaviours() error
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

func (self *behaviourHandler) AddBehaviour(name string, behaviour Behaviour) {

	if self.HasBehaviour(name) {
		return
	}
	self.behaviours[name] = behaviour
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
