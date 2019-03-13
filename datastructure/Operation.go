package datastructure

import (
	"CollaborationNode/dml"
	"fmt"
)

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

func operationFromData(data []byte) Operation {

	//for now return default empty op
	return operation{"user1", "empty"}
}

func (self *Operation) toData() []byte {
	return nil
}

func (self *Operation) apply(rntm *dml.Runtime) interface{} {
	fmt.Println("Apply empty op to runtime")
	return nil
}
