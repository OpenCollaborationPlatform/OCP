package dml

import (
	"github.com/dop251/goja"
)

var userKey []byte = []byte("__userContinuityStateKey_")

type continuitySystem struct {
	methodHandler
	toIncrement map[Identifier]*continuity
}

func NewContinuitySystem(rntm *Runtime) (System, error) {

	sys := &continuitySystem{NewMethodHandler(), make(map[Identifier]*continuity, 0)}
	return sys, nil
}

func (self *continuitySystem) ExposedToJS() bool {
	return false
}

func (self *continuitySystem) GetJSObject() *goja.Object {
	return nil
}

func (self *continuitySystem) CanHandleEvent(event string) bool {

	switch event {
	case "onBeforePropertyChange", "onBeforeChange", "onPropertyChanged", "onChanged":
		return true
	}

	return false
}
func (self *continuitySystem) CanHandleKeyword(keyword string) bool {
	switch keyword {
	case "based_on_state":
		return true
	}

	return false
}

func (self *continuitySystem) BeforeOperation() error {
	return nil
}

func (self *continuitySystem) AfterOperation() error {

	defer func() {
		//clear all increments
		self.toIncrement = make(map[Identifier]*continuity, 0)
	}()

	//process all increments we stored
	for id, con := range self.toIncrement {

		value, _ := con.GetProperty("state").GetValue(id)
		state, ok := value.(int64)
		if !ok {
			return newInternalError(Error_Setup_Invalid, "Wrong datatype stored for continuity state")
		}
		if err := con.GetProperty("state").SetValue(id, state+1); err != nil {
			return err
		}
		con.GetEvent("onStateUpdate").Emit(id, state+1)
	}

	return nil
}

/* +extract prio:4

.. dml:behaviour:: Continuity
	:derived: Behaviour

	The continuity behaviour helps you to ensure continious updates to a object. The idea is
	to make updates on a object only possible, if they are based on the latest available data.

	To ensure this, the caller, the one who wants to make a change, need to show that he based
	his update on the currently existing object. For this, the behaviour adds the :dml:prop:`state`
	property to the object which represents the current state as a integer. When a set of changes
	is finished, the state gets incremented (by the user or automatically). Any caller needs to
	ensure, that his change is based on the currently available data by providing his last
	state as integer. If this provided known state matches the current :dml:prop:`state` value, the
	update is allowed, and if it does not match, the update fails.

	..note:: The state increment happens after all user code is executed. Calling "Increment" on a
			 continuity behaviour marks that object for state increment but does not do it directly.
			 It is only done just before finishing the WAMP api call. The same holds for automatic
			 increments.


	.. dml:property:: automatic
		:const:
		:type: bool
		:default: false

		If set to true a state increment is done automatically for each user call that changes
		the extended object. For example, if a user sets a property on the parent object via
		the WAMP api, the state of the continuity behaviour is incremented when automatic==true.
		If annother property of the parent object is changed during the same WAMP api call, the
		state is not incremented anymore. The next increment can only happen in the next WAMP api
		call or by using the manual methods of the behaviour.

	..  dml:property:: state
		:readonly:
		:type: int
		:default: 0

		A integer describing the current state of this object. Gets incremented if a new state is
		reached. This happens either by user action by calling the increment method, or automatically
		on a change if configured like this.
*/

//Object Transaction adds a whole object to the current transaction. This happens on every change within the object,
//property or key, and if recursive == True also for every change of any subobject.
type continuity struct {
	*behaviour
	system *continuitySystem
}

func NewContinuityBehaviour(rntm *Runtime) (Object, error) {

	base, _ := NewBaseBehaviour(rntm)
	sys := rntm.systems.GetSystem("Continuity").(*continuitySystem)
	cbhvr := &continuity{base, sys}

	cbhvr.AddProperty("automatic", MustNewDataType("bool"), false, Constant)
	cbhvr.AddProperty("state", MustNewDataType("int"), 0, ReadOnly)

	//add default events
	cbhvr.AddEvent(NewEvent(`onStateUpdate`, cbhvr)) //called when the state got incremented

	//add the user usable methods
	cbhvr.AddMethod("Increment", MustNewIdMethod(cbhvr.increment, false))              // increment the object state
	cbhvr.AddMethod("SetKnownState", MustNewIdMethod(cbhvr.setLatestState, false))     //returns true if the user knows the latest state
	cbhvr.AddMethod("HasLatestState", MustNewIdMethod(cbhvr.userHasLatestState, true)) //returns true if the user knows the latest state

	return cbhvr, nil
}

func (self *continuity) GetBehaviourType() string {
	return "Continuity"
}

func (self *continuity) HandleEvent(id Identifier, source Identifier, event string, args []interface{}) error {

	//whenever a property or the object itself changed, we add ourself to the current transaction
	switch event {
	case "onBeforePropertyChange", "onBeforeChange":
		//check if the user change is based on the lates change
		if has, _ := self.userHasLatestState(id); !has {
			return newUserError(Error_Operation_Invalid, "Change is not based on latest state")
		}

	case "onPropertyChanged", "onChanged":
		//after change we check if we should automatically increment the state
		auto, _ := self.GetProperty("automatic").GetValue(id)
		if auto.(bool) {
			self.increment(id)
		}
	}
	return nil
}

func (self *continuity) HandleKeyword(id Identifier, keyword string, arg interface{}) error {

	if keyword == "based_on_state" {

		val := UnifyDataType(arg)
		knownstate, ok := val.(int64)
		if !ok {
			newUserError(Error_Arguments_Wrong, "Keyword argument for continuity behaviour needs to be integer")
		}

		prop, _ := self.GetProperty("state").GetValue(id)
		state := prop.(int64)

		if knownstate != state {
			newUserError(Error_Operation_Invalid, "The change is not based on the latest state", "latest", state, "based_on", knownstate)
		}
		// make sure we store this info
		return self.setLatestState(id, knownstate)
	}
	return nil
}

func (self *continuity) increment(id Identifier) error {

	//check if we can increment
	has, err := self.userHasLatestState(id)
	if err != nil {
		return err
	}
	if !has {
		return newUserError(Error_Operation_Invalid, "Can only increment state based on current one")
	}

	//we only mark ourself for increment, and not do it directly
	self.system.toIncrement[id] = self
	return nil
}

func (self *continuity) userHasLatestState(id Identifier) (bool, error) {

	map_, err := self.GetDBMap(id, userKey)
	if err != nil {
		return false, err
	}

	if !map_.HasKey(self.rntm.currentUser) {
		return false, nil
	}

	value, err := map_.Read(self.rntm.currentUser)
	if err != nil {
		return false, err
	}

	knownstate, ok := value.(int64)
	if !ok {
		return false, newInternalError(Error_Setup_Invalid, "User state data stored in wrong format")
	}

	state, err := self.GetProperty("state").GetValue(id)
	if err != nil {
		return false, err
	}

	return state == knownstate, nil
}

func (self *continuity) setLatestState(id Identifier, state int64) error {

	map_, err := self.GetDBMap(id, userKey)
	if err != nil {
		return err
	}

	return map_.Write(self.rntm.currentUser, state)
}
