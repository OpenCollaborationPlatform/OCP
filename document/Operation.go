package document

import (
	"reflect"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/dml"
	"github.com/OpenCollaborationPlatform/OCP/p2p"
	"github.com/gammazero/nexus/v3/wamp"
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
	User        dml.User
	Path        string
	Arguments   []interface{}
	KWArguments map[string]interface{}

	Node    p2p.PeerID
	Session wamp.ID
}

func newCallOperation(user dml.User, path string, args []interface{}, kwargs map[string]interface{}, node p2p.PeerID, session wamp.ID) Operation {
	return Operation{user, path, args, kwargs, node, session}
}

func newJsOperation(user dml.User, code string, node p2p.PeerID, session wamp.ID) Operation {
	return Operation{user, "__js__", []interface{}{code}, nil, node, session}
}

func (self Operation) ToData() ([]byte, error) {

	var b []byte
	err := codec.NewEncoderBytes(&b, mh).Encode(self)
	return b, wrapInternalError(err, Error_Invalid_Data)
}

func (self Operation) ApplyTo(rntm *dml.Runtime, ds *datastore.Datastore) (interface{}, []dml.EmmitedEvent) {

	//convert all encoded arguments
	args := make([]interface{}, len(self.Arguments))
	copy(args, self.Arguments)
	for i, arg := range self.Arguments {
		if utils.Decoder.InterfaceIsEncoded(arg) {
			val, err := utils.Decoder.DecodeInterface(arg)
			if err != nil {
				return utils.StackError(err, "Unable to decode argument"), nil
			}
			args[i] = val
		}
	}

	var val interface{}
	var evts []dml.EmmitedEvent
	var err error

	if self.Path == "__js__" {
		if len(args) != 1 {
			err = newInternalError(Error_Arguments, "JS operation needs code as argument")

		} else if code, ok := args[0].(string); ok {
			val, evts, err = rntm.RunJavaScript(ds, self.User, code)

		} else {
			err = newInternalError(Error_Arguments, "JS operation needs code as argument")
		}

	} else {
		val, evts, err = rntm.Call(ds, self.User, self.Path, args, kwargs())
	}
	if err != nil {
		return err, nil
	}

	//check if it is a Encotable, if so we only return the encoded identifier!
	if enc, ok := val.(utils.Encotable); ok {
		val = enc.Encode()
	}

	return val, evts
}

func (self Operation) GetSession() (p2p.PeerID, wamp.ID) {
	return self.Node, self.Session
}
