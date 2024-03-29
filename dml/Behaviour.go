// Transaction.go
package dml

import (
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"
)

var behaviourKey []byte = []byte("__behaviours")

/*
 * A Behaviour is a set of methods and events that define a certain way an object reacts on
 * certain circumstances or in certain situations. The language interpreter handles a object
 * according to the behaviours it has adopted.  The behaviour does
 *  - Provide properties to access special data decribing the behaviour state
 *  - Provide Events that fire during execution of the behaviour
 *  - Provides Methods to override and hence customice the behaviour
 *
 * Behaviour infrastructure consists of:
 * - Behaviours: Objects (not Data) that can be added to Data Objects and describe how the
 * 	 parent Object is handled for the given behaviour type
 * - Behaviour Handler: Helper interface to manage behaviours within an Object, similar
 *   to MethodHandler or EventHandler
 * - System: A general managing class for a given behaviour (To be overriden).
 *   This is a singleton and accessible as toplevel global object. It exposes global behaviour
 *   methods
 * - SystemHandler: Helper interface to manage multiple behaviour managers. This is used
 *   by the runtime to store and access all System. SystemHandler is to System
 *   what BehaviourHandler is to Behaviour
 *
 *  TODO: Ensure behaviours are not "historical", e.g. do not to change during versioning or transactioning.
 *		  Maybe only allow const properties?
 */

/* +extract prio:3
.. dml:behaviour:: Behaviour
	:abstract:

	Base class for all behaviours, adding common properties and events. It cannot be
	used directly, only behaviours derived from it. It does add the possibility to add
	custom properties,  events and functions. Children are not allowed.

	.. dml:property:: name
		:const:
		:type: string

		A property defining the name of the behaviour. The name can be used to access ut in
		the hirarchy, either in JavaScript code or as WAMP uri. It is mandatory to set the name
		of each behaviour.

	.. dml:property:: parent
		:const:
		:type: Data

		The parent object of the behaviour, the one which it extends.

	.. dml:property:: recursive
		:const:
		:type: bool

		Defines if the behaviour is applied recursively for all children and subobjects
		of the behaviours parent. For example, if a behaviour is added to a Map Object,
		it may watch for changes in that object. If recursive is true, it will also look
		for all changes in any children or value objects of that Map.

		:default: false

	.. dml:event:: onBeforePropertyChange

		Emitted bevore a property of the object changes. At time of emit the
		property still has its old value.

		:argument string Property: Name of the property thats about to be changed

	.. dml:event:: onPropertyChanged

		Emitted when a property was changed. The value of the property is already the
		new one when emitted.

		:argument string Property: Name of the property thats was changed

*/
type Behaviour interface {
	Object

	GetBehaviourType() string                                        //returns the type of behaviour, e.g. "Transaction". Needed to allow multiple different structs implement single behaviour
	HandleEvent(Identifier, Identifier, string, []interface{}) error //Entry for any kind of behhaviour handling. Here the event that can be handled according to the relevant Manager are provided
	HandleKeyword(Identifier, string, interface{}) error             //Handle Keywords on WAMP Api call to the object
}

func NewBaseBehaviour(runtime *Runtime) (*behaviour, error) {

	obj, err := NewObject(runtime)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create base object for behaviour")
	}
	result := behaviour{obj}

	//add default behaviour properties
	err = result.AddProperty(`recursive`, MustNewDataType("bool"), false, Constant)
	if err != nil {
		return nil, utils.StackError(err, "Unable to recursive property to behaviour")
	}

	return &result, nil
}

//Base implementation
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
	AddBehaviourObject(Behaviour) error
	Behaviours() []string

	//This function is used to retrieve a behaviour database access Identifier
	GetBehaviourIdentifier(Identifier, string) (Identifier, error)
	SetBehaviourIdentifier(Identifier, string, Identifier) error
	HasBehaviourIdentifier(Identifier, string) (bool, error)

	//convinience function for combined logic and db access
	GetBehaviour(Identifier, string) (dmlSet, error)

	//Forwards event to all behaviours given in list, and returns the ones not available
	//Identifier, source object, eventname, arguments, behaviours to forward, recursive (true) or original object(false).
	HandleBehaviourEvent(Identifier, Identifier, string, []interface{}, []string, bool) ([]string, error)

	//Forwards WAMP keywords to all behaviours given in list, and returns the ones not available
	//Identifier, keyword arguments, behaviours to forward, recursive (true) or original object(false).
	HandleBehaviourKeywords(Identifier, map[string]interface{}, []string, bool) ([]string, error)

	//key handlings
	getDSKeyForBehaviour(Identifier, string) datastore.Key
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

func (self *behaviourHandler) AddBehaviourObject(behaviour Behaviour) error {

	name := behaviour.GetBehaviourType()

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

	map_, err := mapFromStore(self.runtime.datastore, id, behaviourKey)
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

	map_, err := mapFromStore(self.runtime.datastore, id, behaviourKey)
	if err != nil {
		return err
	}
	return map_.Write(name, behaviour)
}

func (self *behaviourHandler) HasBehaviourIdentifier(id Identifier, name string) (bool, error) {

	map_, err := mapFromStore(self.runtime.datastore, id, behaviourKey)
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

func (self *behaviourHandler) HandleBehaviourEvent(id Identifier, source Identifier, event string, args []interface{}, behaviours []string, isrecursive bool) ([]string, error) {

	result := make([]string, 0)
	for _, behaviour := range behaviours {
		if self.HasBehaviour(behaviour) {
			bhvrSet, err := self.GetBehaviour(id, behaviour)
			if err != nil {
				return nil, utils.StackError(err, "Unable to retreive behaviour for ID, even though it said to have the behaviour", "behaviour", behaviour, "id", id.String())
			}
			bhvrObj := bhvrSet.obj.(Behaviour)

			val, err := bhvrObj.GetProperty("recursive").GetValue(bhvrSet.id)
			if err != nil {
				return nil, utils.StackError(err, "Unable to read property value", "Property", "recursive")
			}
			if isrecursive && !val.(bool) {
				// we do not add the bahaviour to the result: We have the relevant behaviour, it is just
				//not recursive. this means processing ends here
				continue
			}

			//handle the event, and do no not propagate further
			err = bhvrObj.HandleEvent(bhvrSet.id, source, event, args)
			if err != nil {
				return result, err
			}

		} else {
			result = append(result, behaviour)
		}
	}
	return result, nil
}

func (self *behaviourHandler) HandleBehaviourKeywords(id Identifier, kwargs map[string]interface{}, behaviours []string, isrecursive bool) ([]string, error) {

	result := make([]string, 0)
	for _, behaviour := range behaviours {
		if self.HasBehaviour(behaviour) {
			bhvrSet, err := self.GetBehaviour(id, behaviour)
			if err != nil {
				return nil, utils.StackError(err, "Unable to retreive behaviour for ID, even though it said to have the behaviour", "behaviour", behaviour, "id", id.String())
			}
			bhvrObj := bhvrSet.obj.(Behaviour)

			val, err := bhvrObj.GetProperty("recursive").GetValue(bhvrSet.id)
			if err != nil {
				return nil, utils.StackError(err, "Unable to read property value", "Property", "recursive")
			}
			if isrecursive && !val.(bool) {
				// we do not add the bahaviour to the result: We have the relevant behaviour, it is just
				//not recursive. this means processing ends here
				continue
			}

			//handle the keyword, and do no not propagate further
			for name, arg := range kwargs {
				err = bhvrObj.HandleKeyword(bhvrSet.id, name, arg)
				if err != nil {
					return result, err
				}
			}

		} else {
			result = append(result, behaviour)
		}
	}
	return result, nil
}

func (self *behaviourHandler) getDSKeyForBehaviour(id Identifier, name string) datastore.Key {

	if !self.HasBehaviour(name) {
		return datastore.Key{}
	}

	return datastore.NewKey(datastore.MapType, false, id.Hash(), behaviourKey, name)
}
