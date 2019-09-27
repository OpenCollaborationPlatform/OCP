package datastructure

import (
	"bytes"
	"encoding/gob"

	"github.com/ickby/CollaborationNode/dml"
)

func init() {
	gob.Register(functionOperation{})
	gob.Register(jsOperation{})
	gob.Register(propertyOperation{})
}

type Operation interface {
	ApplyTo(*dml.Runtime) interface{}
	ToData() []byte
}

func operationFromData(data []byte) (Operation, error) {

	var op Operation
	buf := bytes.NewBuffer(data)
	d := gob.NewDecoder(buf)
	err := d.Decode(&op)
	if err != nil {
		return nil, err
	}
	return op, nil
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

func (self functionOperation) ToData() []byte {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	e.Encode(&op)
	return b.Bytes()
}

func (self functionOperation) ApplyTo(rntm *dml.Runtime) interface{} {

	val, err := rntm.CallMethod(self.User, self.Path, self.Function, self.Arguments...)
	if err != nil {
		return err
	}

	//check if it is a Object, if so we only return the encoded identifier!
	obj, ok := val.(dml.Object)
	if ok {
		return obj.Id().Encode()
	}
	
	return val
}

type jsOperation struct {
	User dml.User
	Code string
}

func newJsOperation(user dml.User, code string) Operation {

	return jsOperation{user, code}
}

func (self jsOperation) ToData() []byte {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	e.Encode(&op)
	return b.Bytes()
}

func (self jsOperation) ApplyTo(rntm *dml.Runtime) interface{} {

	val, err := rntm.RunJavaScript(self.User, self.Code)
	if err != nil {
		return err
	}

	//check if it is a Object, if so we only return the encoded identifier!
	obj, ok := val.(dml.Object)
	if ok {
		return obj.Id().Encode()
	}
	
	return val
}

type propertyOperation struct {
	User     dml.User
	Path     string
	Property string
	Value    interface{}
}

func newPropertyOperation(user dml.User, path string, prop string, val interface{}) Operation {

	return propertyOperation{user, path, prop, val}
}

func (self propertyOperation) ToData() []byte {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	e.Encode(&op)
	return b.Bytes()
}

func (self propertyOperation) ApplyTo(rntm *dml.Runtime) interface{} {

	err := rntm.WriteProperty(self.User, self.Path, self.Property, self.Value)
	if err != nil {
		return err
	}

	return self.Value
}
