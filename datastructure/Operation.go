package datastructure

import (
	"bytes"
	"encoding/gob"

	"github.com/ickby/CollaborationNode/dml"
)

func init() {
	gob.Register(functionOperation{})
}

type Operation interface {
	ApplyTo(*dml.Runtime) interface{}
	ToData() []byte
}

type functionOperation struct {
	User      dml.User
	Path      string
	Function  string
	Arguments []interface{}
}

func newFunctionOperation(user dml.User, path string, fnc string, args []interface{}) Operation {

	return functionOperation{user, path, fnc, args}
}

func operationFromData(data []byte) Operation {

	var op Operation
	buf := bytes.NewReader(data)
	d := gob.NewDecoder(buf)
	d.Decode(&op)
	return op
}

func (self functionOperation) ToData() []byte {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	e.Encode(self)
	return b.Bytes()
}

func (self functionOperation) ApplyTo(rntm *dml.Runtime) interface{} {

	val, err := rntm.CallMethod(self.User, self.Path, self.Function, self.Arguments)
	if err != nil {
		return err
	}
	return val
}
