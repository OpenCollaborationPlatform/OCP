package dml

import (
	"encoding/gob"
	"fmt"
	"reflect"
	"github.com/dop251/goja"
	"github.com/ickby/CollaborationNode/utils"
)

var (
	reflectTypeArray  = reflect.TypeOf([]interface{}{})
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

//goja value handling
func extractValue(value goja.Value, rntm *Runtime) interface{}  {
	
	//fmt.Printf("Extract arg: %v (%T) %v (%T)\n", value, value, value.ExportType(), value.ExportType())

	switch value.ExportType() {
		
		case reflectTypeArray:
		
			//List type. as enything can be in the list we simply call extractValue recurisvly
			obj := value.(*goja.Object)
			keys := obj.Keys()
			//fmt.Printf("Keys: %v\n", keys)
			result := make([]interface{}, len(keys))
			for i, key := range keys {
				//fmt.Printf("Add key:  %v\n", i)
				result[i] = extractValue(obj.Get(key), rntm)
			}
			return result
	}

	//check if it is a goja object
	obj, _ := objectFromJSValue(value, rntm)
	if obj != nil {
		return obj
	}

	//no, normal values!
	return value.Export()
}

func extractValues(values []goja.Value, rntm *Runtime) []interface{} {

	res := make([]interface{}, len(values))
	for i, val := range values {
		res[i] = extractValue(val, rntm)
	}
	return res
}


//user type to store data about a user
type User string

func (self User) Data() []byte {
	return []byte(self)
}

func UserFromData(data []byte) (User, error) {
	return User(data), nil
}


type printManager struct {
	messages []string
}

func NewPrintManager() *printManager {
	
	mngr := &printManager{
		messages: make([]string, 0),
	}
	return mngr
}

func (self *printManager) clearMessage() {	
	self.messages = self.messages[:0]
}

func (self *printManager) printMessage(msg string) {	
	self.messages = append(self.messages, msg)
}

func (self *printManager) GetMessages() []string {
	return self.messages
}
