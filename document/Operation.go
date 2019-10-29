package document

import (
	"bytes"
	"encoding/gob"

	"github.com/ickby/CollaborationNode/dml"
	"github.com/gammazero/nexus/wamp"
	"github.com/ickby/CollaborationNode/p2p"
)

func init() {
	gob.Register(functionOperation{})
	gob.Register(jsOperation{})
	gob.Register(propertyOperation{})
}

type Operation interface {
	ApplyTo(*dml.Runtime) interface{}
	ToData() ([]byte, error)
	GetSession() (p2p.PeerID, wamp.ID)
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

	Node    p2p.PeerID
	Session wamp.ID
}

func newFunctionOperation(user dml.User, path string, fnc string, args []interface{}, node p2p.PeerID, session wamp.ID) Operation {

	return functionOperation{user, path, fnc, args, node, session}
}

func (self functionOperation) ToData() ([]byte, error) {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	err := e.Encode(&op)
	return b.Bytes(), err
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

func (self functionOperation) GetSession() (p2p.PeerID, wamp.ID) {
	return self.Node, self.Session
}

type jsOperation struct {
	User dml.User
	Code string

	Node    p2p.PeerID
	Session wamp.ID
}

func newJsOperation(user dml.User, code string, node p2p.PeerID, session wamp.ID) Operation {

	return jsOperation{user, code, node, session}
}

func (self jsOperation) ToData() ([]byte, error) {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	err := e.Encode(&op)
	return b.Bytes(), err
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

func (self jsOperation) GetSession() (p2p.PeerID, wamp.ID) {
	return self.Node, self.Session
}

type propertyOperation struct {
	User     dml.User
	Path     string
	Property string
	Value    interface{}

	Node    p2p.PeerID
	Session wamp.ID
}

func newPropertyOperation(user dml.User, path string, prop string, val interface{}, node p2p.PeerID, session wamp.ID) Operation {

	return propertyOperation{user, path, prop, val, node, session}
}

func (self propertyOperation) ToData() ([]byte, error) {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	err := e.Encode(&op)
	return b.Bytes(), err
}

func (self propertyOperation) ApplyTo(rntm *dml.Runtime) interface{} {

	err := rntm.WriteProperty(self.User, self.Path, self.Property, self.Value)
	if err != nil {
		return err
	}

	return self.Value
}

func (self propertyOperation) GetSession() (p2p.PeerID, wamp.ID) {
	return self.Node, self.Session
}
