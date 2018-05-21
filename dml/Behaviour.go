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
	EventHandler
	MethodHandler
	PropertyHandler
	JSObject
}

/*
 * Handles different behaviours added to a object
 */
type BehaviourHandler interface {
	HasBehaviour(string) bool
}
