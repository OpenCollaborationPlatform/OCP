package dml

import (
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/dop251/goja"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
)

var (
	reflectTypeArray = reflect.TypeOf([]interface{}{})
)

func init() {
	gob.Register(new(User))
}

// helper methods for DB access
func valueFromStore(store *datastore.Datastore, id Identifier, key []byte) (datastore.Value, error) {

	set, err := store.GetOrCreateSet(datastore.ValueType, false, id.Hash())
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	vset, done := set.(*datastore.ValueSet)
	if !done {
		return datastore.Value{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.Value{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *value, nil
}

func valueVersionedFromStore(store *datastore.Datastore, id Identifier, key []byte) (datastore.ValueVersioned, error) {

	set, err := store.GetOrCreateSet(datastore.ValueType, true, id.Hash())
	if err != nil {
		return datastore.ValueVersioned{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	vset, done := set.(*datastore.ValueVersionedSet)
	if !done {
		return datastore.ValueVersioned{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	value, err := vset.GetOrCreateValue(key)
	if err != nil {
		return datastore.ValueVersioned{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *value, nil
}

func listFromStore(store *datastore.Datastore, id Identifier, key []byte) (datastore.List, error) {

	set, err := store.GetOrCreateSet(datastore.ListType, false, id.Hash())
	if err != nil {
		return datastore.List{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	lset, done := set.(*datastore.ListSet)
	if !done {
		return datastore.List{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	list, err := lset.GetOrCreateList(key)
	if err != nil {
		return datastore.List{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *list, nil
}

func mapFromStore(store *datastore.Datastore, id Identifier, key []byte) (datastore.Map, error) {

	set, err := store.GetOrCreateSet(datastore.MapType, false, id.Hash())
	if err != nil {
		return datastore.Map{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	mset, done := set.(*datastore.MapSet)
	if !done {
		return datastore.Map{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	map_, err := mset.GetOrCreateMap(key)
	if err != nil {
		return datastore.Map{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *map_, nil
}

//should be implemented by everythign that is exposed to JS
type JSObject interface {
	GetJSObject(Identifier) *goja.Object
	GetJSPrototype() *goja.Object
	GetJSRuntime() *goja.Runtime
}

//returns the DML object if it is one. obj is nil if not. Error is only set if it is a
//dml object but proplems occured extracting it
func objectFromJSValue(jsval goja.Value, rntm *Runtime) (Identifier, error) {

	jsobj, ok := jsval.(*goja.Object)
	if !ok {
		return Identifier{}, nil
	}

	idProp := jsobj.Get("identifier")
	if idProp != nil {
		id := idProp.Export()
		identifier, ok := id.(Identifier)
		if !ok {
			return Identifier{}, fmt.Errorf("Javascript returned object, but it has errous Identifier method")
		}
		return identifier, nil
	}
	return Identifier{}, nil
}

//goja value handling
func extractValue(value goja.Value, rntm *Runtime) interface{} {

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
	id, _ := objectFromJSValue(value, rntm)
	if id.Valid() {
		return id
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
