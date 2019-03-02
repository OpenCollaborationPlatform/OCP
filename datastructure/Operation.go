package datastructure

import "CollaborationNode/dml"

type Operation interface {
	ApplyTo(*dml.Runtime) interface{}
	ActingUser() dml.User
}

type operation struct {
	user   dml.User
	opType string
}

func newOperation() Operation {

}

func operationFromData(data []byte) {

}

func (self *Operation) toData() []byte {

}

func (self *Operation) apply(rntm *dml.Runtime) interface{} {

}
