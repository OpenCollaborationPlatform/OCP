// Data descibes a Data object with its respective properties to store things in a database
package dml

import (
	"github.com/dop251/goja"

	"CollaborationNode/datastores"
)

//the most basic implementation of an dml Object. All it does is to allow adding
//fundamental types, like properties, events and functions. It has no other special
//functionality. It is intended as dml grouping object as well as base object for
//other types
type data struct {
	*object
}

func NewData(ds *datastore.Datastore, name string, vm *goja.Runtime) Object {

	obj := NewObject(name, vm, ds.GetOrCreateStore(datastore.KeyValue, name))
	dat := data{obj}

	dat.AddProperty("id", String)

	return &dat
}
