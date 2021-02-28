package document

import (
	"reflect"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/gammazero/nexus/v3/wamp"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ugorji/go/codec"
)

//Using msgpack for encoding to ensure, that all arguments that are handble by wamp are handled by the operation.
//This is poblematic with gob, as it needs to have many types registered, which is impossible to know for all
//the datatypes applications throw at us
var mh *codec.MsgpackHandle

func init() {
	mh = new(codec.MsgpackHandle)
	mh.WriteExt = true
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))
}

func operationFromData(data []byte) (Operation, error) {

	var v Operation
	err := codec.NewDecoderBytes(data, mh).Decode(&v)
	return v, wrapInternalError(err, Error_Invalid_Data)
}

type Operation struct {
	User      dml.User
	Path      string
	Arguments []interface{}

	Node    p2p.PeerID
	Session wamp.ID
}

func newCallOperation(user dml.User, path string, args []interface{}, node p2p.PeerID, session wamp.ID) Operation {
	return Operation{user, path, args, node, session}
}

func newJsOperation(user dml.User, code string, node p2p.PeerID, session wamp.ID) Operation {
	return Operation{user, "__js__", []interface{}{code}, node, session}
}

func (self Operation) ToData() ([]byte, error) {

	var b []byte
	err := codec.NewEncoderBytes(&b, mh).Encode(self)
	return b, wrapInternalError(err, Error_Invalid_Data)
}

func (self Operation) ApplyTo(rntm *dml.Runtime, ds *datastore.Datastore) interface{} {

	//convert all encoded arguments
	args := make([]interface{}, len(self.Arguments))
	copy(args, self.Arguments)
	for i, arg := range self.Arguments {
		if utils.Decoder.InterfaceIsEncoded(arg) {
			val, err := utils.Decoder.DecodeInterface(arg)
			if err != nil {
				return utils.StackError(err, "Unable to decode argument")
			}
			args[i] = val
		}
	}

	var val interface{}
	var err error

	if self.Path == "__js__" {
		if len(args) != 1 {
			err = newInternalError(Error_Arguments, "JS operation needs code as argument")

		} else if code, ok := args[0].(string); ok {
			val, err = rntm.RunJavaScript(ds, self.User, code)

		} else {
			err = newInternalError(Error_Arguments, "JS operation needs code as argument")
		}

	} else {
		val, err = rntm.Call(ds, self.User, self.Path, args...)
	}
	if err != nil {
		return err
	}

	//check if it is a Encotable, if so we only return the encoded identifier!
	if enc, ok := val.(utils.Encotable); ok {
		val = enc.Encode()
	}

	return val
}

func (self Operation) GetSession() (p2p.PeerID, wamp.ID) {
	return self.Node, self.Session
}
