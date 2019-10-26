package dml

import (
	"encoding/gob"
	"fmt"
	"github.com/dop251/goja"
	"github.com/ickby/CollaborationNode/utils"
)

func init() {
	gob.Register(new(User))
}

//should be implemented by everythign that is exposed to JS
type JSObject interface {
	GetJSObject() *goja.Object
	GetJSRuntime() *goja.Runtime
}

//returns the DML object if it is one. obj is nil if not. Error is only set if it is a
//dml object but proplems occured extracting it
func objectFromJSValue(jsval goja.Value, rntm *Runtime) (Object, error) {

	jsobj, ok := jsval.(*goja.Object)
	if !ok {
		return nil, nil
	}

	fncobj := jsobj.Get("Identifier")
	if fncobj != nil {
		fnc := fncobj.Export()
		wrapper, ok := fnc.(func(goja.FunctionCall) goja.Value)
		if ok {
			encoded := wrapper(goja.FunctionCall{})
			id, err := IdentifierFromEncoded(encoded.Export().(string))
			if err != nil {
				return nil, utils.StackError(err, "Unable to convert returned object from javascript")
			}
			obj, ok := rntm.objects[id]
			if !ok {
				return nil, utils.StackError(err, "Cannot find object returned from javascript")
			}
			return obj, nil

		} else {
			return nil, fmt.Errorf("Javascript returned object, but it has errous Identifier method")
		}
	}

	return nil, nil

}

//user type to store data about a user
type User string

func (self User) Data() []byte {
	return []byte(self)
}

func UserFromData(data []byte) (User, error) {
	return User(data), nil
}
