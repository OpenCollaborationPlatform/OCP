package datastructure

import (
	"CollaborationNode/connection"
	"CollaborationNode/datastores"
	"CollaborationNode/dml"
	"CollaborationNode/p2p"
	"sync"
)

//Async datastructure whcih encapsulates synchronous DML runtime and a datastore.
//It works operation based: All operations are carried out ordered
type Datastructure struct {

	//dml state handling
	dml      *dml.Runtime
	dmlState state
	stateIdx uint8

	//connection handling
	swarm  p2p.Swarm
	server connection.Server
	client connection.Client
}

//generates a new operation which will be in line after all currently processed
//operations
func (self *Datastructure) NextOperation() Operation {

	return nil
}
