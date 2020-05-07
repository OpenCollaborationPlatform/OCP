package document

import (
	"bytes"
	"encoding/gob"

	"github.com/gammazero/nexus/v3/wamp"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
)

func init() {
	gob.Register(callOperation{})
	gob.Register(jsOperation{})
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

type callOperation struct {
	User      dml.User
	Path      string
	Arguments []interface{}

	Node    p2p.PeerID
	Session wamp.ID
}

func newCallOperation(user dml.User, path string, args []interface{}, node p2p.PeerID, session wamp.ID) Operation {

	return callOperation{user, path, args, node, session}
}

func (self callOperation) ToData() ([]byte, error) {

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var op Operation = self
	err := e.Encode(&op)
	return b.Bytes(), err
}

func (self callOperation) ApplyTo(rntm *dml.Runtime) interface{} {

	val, err := rntm.Call(self.User, self.Path, self.Arguments...)
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

func (self callOperation) GetSession() (p2p.PeerID, wamp.ID) {
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
