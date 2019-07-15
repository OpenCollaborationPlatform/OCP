package datastructure

import (
	"CollaborationNode/datastores"
	"CollaborationNode/dml"
	"sync"
)

//Async datastructure whcih encapsulates synchronous DML runtime and a datastore.
//It works operation based: All operations are carried out ordered
type Datastructure struct {
	dml   *dml.Runtime
	store *datastore.Datastore
	mutex sync.Mutex
}

//generates a new operation which will be in line after all currently processed
//operations
func (self *Datastructure) NextOperation() Operation {

	return nil
}
