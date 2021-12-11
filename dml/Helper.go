package dml

import (
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"
	"github.com/dop251/goja"
	"github.com/iancoleman/strcase"
)

var (
	reflectTypeArray = reflect.TypeOf([]interface{}{})
)

func init() {
	gob.Register(new(User))
}

/*Error handling*/

const Error_Arguments_Wrong = "arguments_wrong"
const Error_Key_Not_Available = "key_not_available"
const Error_Setup_Invalid = "setup_invalid"
const Error_Fatal = "fatal_problem"
const Error_Operation_Invalid = "operation_invalid"
const Error_Type = "type"
const Error_Syntax = "syntax_error"
const Error_Compiler = "compilation_failed"
const Error_Filesystem = "filesystem_not_accessible"

func newInternalError(reason, msg string, args ...interface{}) utils.OCPError {
	err := utils.NewError(utils.Internal, "runtime", reason, msg, args...)
	return err
}

func wrapInternalError(err error, reason string) error {
	if err != nil {
		return newInternalError(reason, err.Error())
	}
	return err
}

func newUserError(reason, msg string, args ...interface{}) utils.OCPError {
	err := utils.NewError(utils.Application, "runtime", reason, msg, args...)
	return err
}

func newSetupError(reason, msg string, args ...interface{}) utils.OCPError {
	err := utils.NewError(utils.Application, "setup", reason, msg, args...)
	return err
}

func wrapSetupError(err error, reason string) error {
	if err != nil {
		return newSetupError(reason, err.Error())
	}
	return err
}

//Extracts all standart JavaScript errors from goja error values
func wrapJSError(err error, vm *goja.Runtime) utils.OCPError {

	if err == nil {
		return nil
	}

	var ocpErr utils.OCPError = nil
	if ex, ok := err.(*goja.Exception); ok {

		//maybe directly an OCP error
		if oc, ok := ex.Value().Export().(utils.OCPError); ok {
			ocpErr = oc

			//then check if it is a JS object based on "Error"
		} else if obj := ex.Value().ToObject(vm); obj != nil && obj.ClassName() == "Error" {
			reason := obj.Get("name").Export().(string)
			ocpErr = utils.NewError(utils.Application, "javascript", strcase.ToSnake(reason), fmt.Sprintf("%v", ex.Value().Export()))

			//last resort: build a new error
		} else {
			ocpErr = utils.NewError(utils.Application, "javascript", "user_exception", fmt.Sprintf("%v", ex.Value().Export()))
		}

	} else if _, ok := err.(*goja.CompilerSyntaxError); ok {
		ocpErr = utils.NewError(utils.Application, "javascript", "syntax_error", err.Error())

	} else if _, ok := err.(*goja.CompilerReferenceError); ok {
		ocpErr = utils.NewError(utils.Application, "javascript", "reference_error", err.Error())

	} else {
		ocpErr = utils.NewError(utils.Application, "unknown", "unknown_error", err.Error())
	}

	return ocpErr
}

//little helper to combine a object (logic) and a Identifier (database access)
type dmlSet struct {
	obj Object
	id  Identifier
}

func (self dmlSet) valid() bool {
	//check if all inforation stored is valid and if they are consistent
	//beween each other

	if !self.id.Valid() {
		return false
	}
	if self.obj == nil {
		return false
	}

	idDt, err := self.obj.GetDataType(self.id)
	if err != nil {
		return false
	}

	if !idDt.IsEqual(self.obj.GetObjectDataType()) {
		return false
	}

	return true
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

//goja value handling
func toGojaValue(value interface{}, rntm *Runtime) goja.Value {

	switch value.(type) {
	case Identifier:
		id := value.(Identifier)
		set, err := rntm.getObjectSet(id)
		if err != nil {
			return rntm.jsvm.ToValue(err)
		} else {
			return set.obj.GetJSObject(set.id)
		}

	case dmlSet:
		set := value.(dmlSet)
		return set.obj.GetJSObject(set.id)

	case []Identifier:
		ids := value.([]Identifier)
		result := make([]goja.Value, len(ids))
		for i, id := range ids {
			set, err := rntm.getObjectSet(id)
			if err != nil {
				return rntm.jsvm.ToValue(err)
			} else {
				result[i] = set.obj.GetJSObject(set.id)
			}
		}
		return rntm.jsvm.ToValue(result)

	case []dmlSet:
		sets := value.([]dmlSet)
		result := make([]goja.Value, len(sets))
		for i, set := range sets {
			result[i] = set.obj.GetJSObject(set.id)
		}
		return rntm.jsvm.ToValue(result)
	}
	//default
	return rntm.jsvm.ToValue(value)
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

func listVersionedFromStore(store *datastore.Datastore, id Identifier, key []byte) (datastore.ListVersioned, error) {

	set, err := store.GetOrCreateSet(datastore.ListType, true, id.Hash())
	if err != nil {
		return datastore.ListVersioned{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	lset, done := set.(*datastore.ListVersionedSet)
	if !done {
		return datastore.ListVersioned{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	list, err := lset.GetOrCreateList(key)
	if err != nil {
		return datastore.ListVersioned{}, utils.StackError(err, "Unable to read %s from DB", string(key))
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

func mapVersionedFromStore(store *datastore.Datastore, id Identifier, key []byte) (datastore.MapVersioned, error) {

	set, err := store.GetOrCreateSet(datastore.MapType, true, id.Hash())
	if err != nil {
		return datastore.MapVersioned{}, utils.StackError(err, "Unable to load %s from database", id)
	}
	mset, done := set.(*datastore.MapVersionedSet)
	if !done {
		return datastore.MapVersioned{}, fmt.Errorf("Database access failed: wrong set returned")
	}
	map_, err := mset.GetOrCreateMap(key)
	if err != nil {
		return datastore.MapVersioned{}, utils.StackError(err, "Unable to read %s from DB", string(key))
	}
	return *map_, nil
}
