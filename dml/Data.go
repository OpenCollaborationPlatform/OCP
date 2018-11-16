// Data descibes a Data object with its respective properties to store things in a database
package dml

import (
	"github.com/dop251/goja"

	"CollaborationNode/datastores"
)

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

func NewData(ds *datastore.Datastore, name string, parent Object, vm *goja.Runtime) Object {

	dat := data{
		NewObject(parent, name, "Data", vm, ds),
		NewBehaviourHandler(),
	}

	//default properties
	dat.AddProperty("id", String, true)

	return &dat
}
