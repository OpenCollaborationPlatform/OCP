// Transaction.go
package dml

/*
 * A Behaviour is a set of methods and events that define a certain way an object reacts on
 * certain circumstances or in certain situations. The language interpreter handles a object in
 * according to the behaviours it has adopted.  The behaviour does
 *  - Provide properties to access special data decribing the behaviour state
 *  - Provide Events that fire during execution of the behaviour
 *  - Provides Methods to override and hence customice the behaviour
 */
type Behaviour interface {
	Object

	SetupDefaults()
}

//Behaviour implementation with ability to have default methods
type behaviour struct {
	object

	Defaults methodHandler
}

//all methods that are not provided by the user are setup
func (self *behaviour) SetupDefaults() {

	meths := self.Defaults.Methods()
	for _, meth := range meths {
		if !self.HasMethod(meth) {
			self.AddMethod(meth, self.Defaults.GetMethod(meth))
		}
	}
}

/*
 * Handles different behaviours added to a object
 */
type BehaviourHandler interface {
	HasBehaviour(string) bool
}
