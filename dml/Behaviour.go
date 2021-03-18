// Transaction.go
package dml

import (
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP /utils"
)

/*
 * A Behaviour is a set of methods and events that define a certain way an object reacts on
 * certain circumstances or in certain situations. The language interpreter handles a object
 * according to the behaviours it has adopted.  The behaviour does
 *  - Provide properties to access special data decribing the behaviour state
 *  - Provide Events that fire during execution of the behaviour
 *  - Provides Methods to override and hence customice the behaviour
 *
 * Behaviour infrastructure consists of:
 * - Behaviours: Objects (not Data) that can be added to Data Objects and describe the
 * 	 Object for the given behaviour type
 * - Behaviour Handler: Helper interface to manage behaviours within an Object, similar
 *   to MethodHandler or EventHandler
 * - BehaviourManager: A general managing class for a given behaviour. This is a singleton
 *   and accessible as toplevel global object. It exposes global behaviour methods
 */

/* The general behaviour manager, exposing Methods */
type BehaviourManager interface {
	MethodHandler
	JSObject
}

/* The behaviour object */
type Behaviour interface {
	Object

	Setup() error                  //Called after parent is fully setup
	SetupRecursive(obj Data) error //Called for each recursive data Object (if recursive is true)
}

func NewBehaviour(runtime *Runtime) (*behaviour, error) {

	obj, err := NewObject(runtime)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create base object for behaviour")
	}
	result := behaviour{obj}

	//add default behaviour properties
	err = result.AddProperty(`recursive`, MustNewDataType("bool"), false, true)
	if err != nil {
		return nil, utils.StackError(err, "Unable to recursive property to behaviour")
	}

	return &result, nil
}

type behaviour struct {
	*object
}

/*
 * Handler for different behaviours within an object
 */
type BehaviourHandler interface {

	//management functions for behaviours:
	//here general behaviour objects are handled, as a object has a defined set of behaviours.
	//This does not provide any database access, only logic
	HasBehaviour(string) bool
	GetBehaviourObject(string) Behaviour
	AddBehaviourObject(string, Behaviour) error
	Behaviours() []string

	//This function is used to retrieve a behaviour database access Identifier
	GetBehaviourIdentifier(Identifier, string) (Identifier, error)
	SetBehaviourIdentifier(Identifier, string, Identifier) error
	HasBehaviourIdentifier(Identifier, string) (bool, error)

	//convinience function for combined logic and db access
	GetBehaviour(Identifier, string) (dmlSet, error)

	//Setup all behaviours, possible including all childs
	//to be called after data hirarchy is setup (parent and children)
	//SetupBehaviours(Data, bool) error
}

func NewBehaviourHandler(runtime *Runtime) behaviourHandler {
	return behaviourHandler{make(map[string]Behaviour, 0), runtime}
}

type behaviourHandler struct {
	behvrs  map[string]Behaviour
	runtime *Runtime
}

func (self *behaviourHandler) HasBehaviour(name string) bool {

	_, has := self.behvrs[name]
	return has
}

func (self *behaviourHandler) AddBehaviourObject(name string, behaviour Behaviour) error {

	if self.HasBehaviour(name) {
		return fmt.Errorf("Behaviour already set, cannot override")
	}
	self.behvrs[name] = behaviour
	return nil
}

func (self *behaviourHandler) GetBehaviourObject(name string) Behaviour {

	val, has := self.behvrs[name]
	if !has {
		return nil
	}

	return val
}

func (self *behaviourHandler) Behaviours() []string {

	result := make([]string, 0)
	for key, _ := range self.behvrs {
		result = append(result, key)
	}

	return result
}

func (self *behaviourHandler) GetBehaviourIdentifier(id Identifier, name string) (Identifier, error) {

	map_, err := mapFromStore(self.runtime.datastore, id, []byte("__behaviours"))
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to access behaviours in database for %s", id)
	}
	value, err := map_.Read(name)
	if err != nil {
		return Identifier{}, err
	}
	return *value.(*Identifier), nil
}

func (self *behaviourHandler) SetBehaviourIdentifier(id Identifier, name string, behaviour Identifier) error {

	if val, _ := self.HasBehaviourIdentifier(id, name); val {
		return fmt.Errorf("Behaviour already set: %v", name)
	}

	map_, err := mapFromStore(self.runtime.datastore, id, []byte("__behaviours"))
	if err != nil {
		return err
	}
	return map_.Write(name, behaviour)
}

func (self *behaviourHandler) HasBehaviourIdentifier(id Identifier, name string) (bool, error) {

	map_, err := mapFromStore(self.runtime.datastore, id, []byte("__behaviours"))
	if err != nil {
		return false, err
	}
	return map_.HasKey(name), nil
}

func (self *behaviourHandler) GetBehaviour(id Identifier, name string) (dmlSet, error) {

	bhvrID, err := self.GetBehaviourIdentifier(id, name)
	if err != nil {
		return dmlSet{}, err
	}

	return dmlSet{obj: self.GetBehaviourObject(name), id: bhvrID}, nil
}

/*
func (self *behaviourHandler) SetupBehaviours(obj Data, childs bool) error {

	//We need to setup all behaviours we have been added, and all recursive
	//ones in any of our parents

	//own behaviours
	done := make([]string, 0)
	for name, bhvr := range self.behaviours {
		bhvr.Setup()
		done = append(done, name)
	}

	//let's start recursive behaviour setup (iterate upwards)
	if obj.Parent() != nil {
		parent, ok := obj.Parent().(Data)
		if ok {
			err := self.setupRecursive(parent, obj, done)
			if err != nil {
				return err
			}
		}
	}

	//setup the children too if required  (iterate downwards)
	if childs {
		for _, child := range obj.GetSubobjects(false) {
			data, ok := child.(Data)
			if !ok {
				continue
			}
			data.SetupBehaviours(data, true)
		}
	}

	return nil
}

func (self *behaviourHandler) setupRecursive(setup Data, with Data, done []string) error {

	//we check if there are any behaviours available, that we did not yet Setup
	for _, bhvr := range setup.Behaviours() {

		in := false
		for _, d := range done {
			if d == bhvr {
				in = true
				break
			}
		}

		if !in {
			done = append(done, bhvr)
			err := setup.GetBehaviour(bhvr).SetupRecursive(with)
			if err != nil {
				return err
			}
		}
	}

	//go on check the parent
	if setup.GetParent() != nil {
		parent, ok := setup.GetParent().(Data)
		if ok {
			return self.setupRecursive(parent, with, done)
		}
	}

	return nil
}
*/
