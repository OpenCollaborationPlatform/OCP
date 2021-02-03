package datastore

import (
	"encoding/gob"
)

func init() {
	gob.Register(new([]interface{}))
	gob.Register(new(map[string]interface{}))
}

/*
import (
	"reflect"

	"github.com/ugorji/go/codec"
)

//encoding done by msgpack
var mh *codec.MsgpackHandle

func init() {
	mh = new(codec.MsgpackHandle)
	mh.WriteExt = true
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))
}

// MsgpackRegisterExtension registers a custom type for special serialization.
func RegisterEncodingType(t reflect.Type, ext byte, encode func(reflect.Value) ([]byte, error), decode func(reflect.Value, []byte) error) {
	mh.s
}

//helper functions
func getBytes(data interface{}) ([]byte, error) {

	var b []byte
	return b, codec.NewEncoderBytes(&b, mh).Encode(data)
}

func getInterface(bts []byte) (interface{}, error) {

	var res interface{}
	if err := codec.NewDecoderBytes(bts, mh).Decode(&res); err != nil {
		return nil, wrapDSError(err, Error_Invalid_Data)
	}

	return res, nil
}
*/
