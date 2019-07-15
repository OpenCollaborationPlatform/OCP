package datastructure

import (
	"CollaborationNode/dml"
	"fmt"
)

type Operation interface {
	ApplyTo(*dml.Runtime) error
	ActingUser() dml.User
}

type operation struct {
	user   dml.User
	opType string
}

func newOperation() Operation {

	return nil
}

func operationFromData(data []byte) Operation {

	//for now return default empty op
	return &operation{"user1", "empty"}
}

func (self *operation) ActingUser() dml.User {
	return self.user
}

func (self *operation) toData() []byte {
	return nil
}

func (self *operation) ApplyTo(rntm *dml.Runtime) error {
	fmt.Println("Apply empty op to runtime")
	return nil
}
