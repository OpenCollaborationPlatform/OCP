// Data descibes a Data object with its respective properties to store things in a database
package dml

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions, as well as holding Behaviours.
//It has no other special functionality. It is intended as dml grouping object as well as
//base object for all other data types
type Data interface {
	Object
	BehaviourHandler
}

type data struct {
	*object
	behaviourHandler
}

func NewData(name string, parent identifier, rntm *Runtime) Object {

	dat := data{
		NewObject(parent, name, "Data", rntm),
		NewBehaviourHandler(),
	}

	return &dat
}
