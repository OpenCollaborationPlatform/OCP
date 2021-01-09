/*
DML Runtime is the execution point for a dml data structure. It provides parsing of dml files,
loading the data from the existing storage (or creating a new storage) and handling of all
logic that makes up the dml structure (events, access, transaction, versioning etc.)

Key features:
 - Parsing of dml files and setting up of data structures and storages
 - Full handling of dml logic, events, functions, behaviours etc.
 - Providing access to the entities for read, write and call

Note:
 - It is not enbled for concurrentcy. It is task of the user to ensure syncronized
   access
*/

package dml

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/satori/go.uuid"

	datastore "github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

var mainIdKey = []byte("MainIdentifier")
var mainDtKey = []byte("MainDatatype")
var internalKey [32]byte

func init() {
	copy(internalKey[:], []byte("__internal"))
}

//Function prototype that can create new object types in DML
type CreatorFunc func(rntm *Runtime) (Object, error)

//Function prototype that gets called on any runtime event
type EventCallbackFunc func(path string, args ...interface{})

func NewRuntime() *Runtime {

	//js runtime with console support
	js := goja.New()
	new(require.Registry).Enable(js)
	console.Enable(js)

	rntm := &Runtime{
		printManager: NewPrintManager(),
		creators:     make(map[string]CreatorFunc, 0),
		eventCBs:     make(map[string]EventCallbackFunc, 0),
		jsvm:         js,
		datastore:    nil,
		mutex:        &sync.Mutex{},
		ready:        false,
		currentUser:  "none",
		objects:      make(map[DataType]Object, 0),
		behaviours:   make(map[string]BehaviourManager, 0),
	}

	//build the managers and expose
	/*	transMngr, err := NewTransactionManager(rntm)
		if err != nil {
			panic("Unable to initilize transaction manager")
		}
		rntm.behaviours["Transaction"] = transMngr
		rntm.jsvm.Set("Transaction", transMngr.GetJSObject())
	*/
	//add the datastructures
	rntm.RegisterObjectCreator("Data", NewData)
	rntm.RegisterObjectCreator("Variant", NewVariant)
	rntm.RegisterObjectCreator("Vector", NewVector)
	rntm.RegisterObjectCreator("Map", NewMap)
	rntm.RegisterObjectCreator("Graph", NewGraph)
	//	rntm.RegisterObjectCreator("Transaction", NewTransactionBehaviour)

	//setup globals
	SetupGlobals(rntm)

	return rntm
}

//builds a datastructure from a file
// - existing types must be registered to be recognized during parsing
type Runtime struct {
	*printManager

	creators map[string]CreatorFunc
	eventCBs map[string]EventCallbackFunc

	//components of the runtime
	jsvm      *goja.Runtime
	datastore *datastore.Datastore
	mutex     *sync.Mutex

	//internal state
	importPath  string
	ready       Boolean //True if a datastructure was read and setup, false if no dml file was parsed
	currentUser User    //user that currently access the runtime
	objects     map[DataType]Object
	main        DataType

	//managers
	behaviours map[string]BehaviourManager
}

// Setup / creation Methods
// ************************

func (self *Runtime) ParseFolder(path string) error {
	//check the main file
	main := filepath.Join(path, "main.dml")
	file, err := os.Open(main)
	if err != nil {
		return err
	}
	defer file.Close()

	//set the import path
	self.importPath = path

	//parse the file!
	return self.Parse(file)
}

//Parses the dml code and setups the full structure. Note: Cannot handle local
//imports
func (self *Runtime) Parse(reader io.Reader) error {

	//no double loading
	if self.ready == true {
		return fmt.Errorf("Runtime is already setup")
	}

	//we start with building the AST
	ast := &DML{}
	parser, err := participle.Build(&DML{}, participle.Lexer(&dmlDefinition{}))
	if err != nil {
		return err
	}

	err = parser.Parse(reader, ast)
	if err != nil {
		return utils.StackError(err, "Unable to parse dml code")
	}

	//first import everything needed
	for _, imp := range ast.Imports {
		err := self.importDML(imp)
		if err != nil {
			return utils.StackError(err, "Import failed: %v", imp.File)
		}
	}

	//process the AST into usable objects
	mainObj, err := self.buildObject(ast.Object)
	if err != nil {
		return utils.StackError(err, "Unable to parse dml code")
		//TODO clear the database entries...
	}

	//expose main object to JS
	getter := self.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {
		main, err := self.getMainObjectSet()
		if err != nil {
			panic(err.Error())
		}
		return main.obj.GetJSObject(main.id)
	})
	name := mainObj.GetProperty("name").GetValue(Identifier{}).(string) //name is a const property, hence we can access it with any identifier, even invalid ones
	self.jsvm.GlobalObject().DefineAccessorProperty(name, getter, nil, goja.FLAG_FALSE, goja.FLAG_TRUE)

	//store the state of the parsing
	self.main = mainObj.GetObjectDataType()
	self.ready = true

	return err
}

//Setups the database according to the parsed DML file
func (self *Runtime) InitializeDatastore(ds *datastore.Datastore) error {

	ds.Begin()
	defer ds.Rollback() //rollback on error, otherwise we commit before

	if !self.ready {
		return fmt.Errorf("Runtime not setup")
	}

	//all upcoming operations use this datastore
	self.datastore = ds

	//check if the data structure is setup, or if we need to do this
	set, err := self.datastore.GetOrCreateSet(datastore.ValueType, false, internalKey)
	if err != nil {
		return utils.StackError(err, "Unable to access DB")
	}
	vSet, _ := set.(*datastore.ValueSet)
	isSetup := vSet.HasKey(mainIdKey)

	if isSetup {
		return fmt.Errorf("Datastore is already setup")

	}

	//nothing setup yet, lets do it
	mainObj, ok := self.objects[self.main]
	if !ok {
		return fmt.Errorf("The runtime is not setup correctly")
	}
	mainID, err := self.setupObject(mainObj, Identifier{})
	if err != nil {
		return utils.StackError(err, "Unable to setup database")
	}
	value, err := vSet.GetOrCreateValue(mainIdKey)
	if err != nil {
		return utils.StackError(err, "Unable to access  database while setup")
	}
	err = value.Write(mainID)
	if err != nil {
		return utils.StackError(err, "Unable to access  database while setup")
	}

	//store the DB type
	dbDt, err := vSet.GetOrCreateValue(mainDtKey)
	if err != nil {
		return utils.StackError(err, "Unable to store runtime datatype")
	}
	err = dbDt.Write(mainObj.GetObjectDataType())
	if err != nil {
		return utils.StackError(err, "Unable to store runtime datatype")
	}

	//setup all object paths
	var path string
	if mainID.Name != "" {
		path = mainID.Name
	} else {
		path = mainID.Encode()
	}
	err = mainObj.SetObjectPath(mainID, path)
	if err != nil {
		return err
	}

	//call the created events
	if data, ok := mainObj.(Data); ok {
		data.Created(mainID)

	} else {
		return fmt.Errorf("Main object is Behaviour, not Data")
	}

	return ds.Commit()
}

//Function to extend the available data and behaviour types for this runtime
func (self *Runtime) RegisterObjectCreator(name string, fnc CreatorFunc) error {

	_, ok := self.creators[name]
	if ok {
		return nil //not an error, as due to import it could happen that things are reimportet
	}

	self.creators[name] = fnc
	return nil
}

//Function to register function to catch all runtime events
func (self *Runtime) RegisterEventCallback(name string, fnc EventCallbackFunc) error {

	_, ok := self.eventCBs[name]
	if ok {
		return fmt.Errorf("Event callback '%v' already registered", name)
	}

	self.eventCBs[name] = fnc
	return nil
}

//Function to register function to catch all runtime events
func (self *Runtime) UnregisterEventCallback(name string) error {

	delete(self.eventCBs, name)
	return nil
}

// 						Accessing / executing Methods
//*********************************************************************************

//run arbitrary javascript code on the loaded structure
func (self *Runtime) RunJavaScript(ds *datastore.Datastore, user User, code string) (interface{}, error) {

	self.clearMessage()
	self.datastore = ds
	self.datastore.Begin()

	//all upcoming operations use this datastore
	if err := self.checkDatastore(ds); err != nil {
		self.datastore.Rollback()
		return nil, err
	}

	//save the user for processing
	self.currentUser = user

	val, err := self.jsvm.RunString(code)

	if err != nil {
		self.postprocess(true)
		return nil, err
	}

	err = self.postprocess(false)
	if err != nil {
		return nil, err
	}

	//return default values
	return extractValue(val, self), err
}

func (self *Runtime) IsConstant(ds *datastore.Datastore, fullpath string) (bool, error) {

	self.datastore.Begin()
	defer self.datastore.Rollback()

	//all upcoming operations use this datastore
	if err := self.checkDatastore(ds); err != nil {
		return false, err
	}
	self.datastore = ds

	//get path and accessor
	idx := strings.LastIndex(string(fullpath), ".")
	path := fullpath[:idx]
	accessor := fullpath[(idx + 1):]

	//check if manager
	mngr, ok := self.behaviours[path]
	if ok && mngr.HasMethod(accessor) {
		return mngr.GetMethod(accessor).IsConst(), nil
	}

	//the relevant object
	dbSet, err := self.getObjectFromPath(path)
	if err != nil {
		return false, err
	}

	//check if it is a method that could be const
	if dbSet.obj.HasMethod(accessor) {
		fnc := dbSet.obj.GetMethod(accessor)
		return fnc.IsConst(), nil
	}

	//check if it is a property that could be const
	if dbSet.obj.HasProperty(accessor) {
		prop := dbSet.obj.GetProperty(accessor)
		return prop.IsConst(), nil
	}

	//events are always non-const
	if dbSet.obj.HasEvent(accessor) {
		return false, nil
	}

	//the only alternative left is a direct value access. This is always const
	return true, nil
}

func (self *Runtime) Call(ds *datastore.Datastore, user User, fullpath string, args ...interface{}) (interface{}, error) {

	self.clearMessage()

	self.datastore = ds
	err := self.datastore.Begin()
	if err != nil {
		return nil, utils.StackError(err, "Unable to access database")
	}

	if err := self.checkDatastore(ds); err != nil {
		self.datastore.Rollback()
		return nil, err
	}

	//save the user for processing
	self.currentUser = user

	//get path and accessor
	idx := strings.LastIndex(string(fullpath), ".")
	path := fullpath[:idx]
	accessor := fullpath[(idx + 1):]

	//first check if it is a Manager
	mngr, ok := self.behaviours[path]
	if ok {
		if !mngr.HasMethod(accessor) {
			return nil, fmt.Errorf("Manager %v does not have method %v", path[0], accessor)
		}
		fnc := mngr.GetMethod(accessor)
		result, e := fnc.Call(Identifier{}, args...)

		//did somethign go wrong?
		if e != nil {
			self.datastore.Rollback()
			return nil, e
		}
		if fnc.IsConst() {
			self.datastore.Rollback()
			return result, nil
		}
		err = self.postprocess(false)
		return result, err
	}

	//not a manager: now check if path is correct and object available
	dbSet, err := self.getObjectFromPath(path)
	if err != nil {
		self.datastore.Rollback()
		return nil, err
	}

	handled := false
	constant := false
	var result interface{} = nil
	err = nil

	//check if it is a method
	if dbSet.obj.HasMethod(accessor) {
		fnc := dbSet.obj.GetMethod(accessor)

		result, err = fnc.Call(dbSet.id, args...)
		handled = true

		if fnc.IsConst() {
			constant = true
		}
	}

	//a property maybe?
	if !handled && dbSet.obj.HasProperty(accessor) {
		prop := dbSet.obj.GetProperty(accessor)
		handled = true

		if len(args) == 0 {
			//read only
			result = prop.GetValue(dbSet.id)
			constant = true

		} else {
			err = prop.SetValue(dbSet.id, args[0])
			result = args[0]
		}
	}

	//an event?
	if !handled && dbSet.obj.HasEvent(accessor) {
		err = dbSet.obj.GetEvent(accessor).Emit(dbSet.id, args...)
		handled = true
	}

	//or a simple value?
	if !handled {
		dat, ok := dbSet.obj.(Data)
		if ok {
			result, err = dat.GetValueByName(dbSet.id, accessor)
			constant = true
			handled = true
		}
	}

	//was it handled? If not no change to db was done
	if !handled {
		self.datastore.Rollback()
		return nil, fmt.Errorf("No accessor %v known in object %v", accessor, path)
	}

	//did an error occure?
	if err != nil {
		self.postprocess(true)
		return nil, err
	}

	//postprocess correctly
	err = self.postprocess(constant)

	//we do not return dmlSets
	if set, ok := result.(dmlSet); ok {
		result = set.id
	}

	return result, err
}

// 							Internal Functions
//*********************************************************************************
func (self *Runtime) checkDatastore(ds *datastore.Datastore) error {

	set, err := ds.GetOrCreateSet(datastore.ValueType, false, internalKey)
	if err != nil {
		return utils.StackError(err, "Unable to access DB")
	}
	vSet, _ := set.(*datastore.ValueSet)
	dbDt, err := vSet.GetOrCreateValue(mainDtKey)
	if err != nil {
		return utils.StackError(err, "Datastore invalid")
	}

	if ok, err := dbDt.WasWrittenOnce(); !ok || err != nil {
		return fmt.Errorf("Datastore is not initialized by runtime")
	}

	result, err := dbDt.Read()
	if err != nil {
		return utils.StackError(err, "Datastore invalid")
	}

	dt, ok := result.(*DataType)
	if !ok {
		fmt.Errorf("Datastore invalid")
	}

	if !dt.IsEqual(self.main) {
		return fmt.Errorf("Datastore is initialized for different runtime")
	}

	//everything ok!
	return nil
}

func (self *Runtime) getMainObjectSet() (dmlSet, error) {

	if !self.ready {
		return dmlSet{}, fmt.Errorf("Runtime not setup correctly")
	}

	//check if the data structure is setup, or if we need to do this
	set, err := self.datastore.GetOrCreateSet(datastore.ValueType, false, internalKey)
	if err != nil {
		return dmlSet{}, err
	}
	vSet, _ := set.(*datastore.ValueSet)
	value, err := vSet.GetOrCreateValue(mainIdKey)
	if err != nil {
		return dmlSet{}, err
	}

	id, err := value.Read()
	if err != nil {
		return dmlSet{}, err
	}

	ident, ok := id.(*Identifier)
	if !ok {
		return dmlSet{}, fmt.Errorf("Stored data is not identifier, cannot access main objects")
	}

	return self.getObjectSet(*ident)
}

//get the object from the identifier path list (e.g. myobj.childname.yourobject)
//alternatively to names it can include identifiers (e.g. from Object.Identifier())
func (self *Runtime) getObjectFromPath(path string) (dmlSet, error) {

	names := strings.Split(path, `.`)
	if len(names) == 0 {
		return dmlSet{}, fmt.Errorf("Not a valid path to object: no names found")
	}

	main, err := self.getMainObjectSet()
	if err != nil {
		return dmlSet{}, err
	}

	var dbSet dmlSet
	if names[0] != main.id.Name {
		id, err := IdentifierFromEncoded(names[0])
		if err != nil {
			return dmlSet{}, fmt.Errorf("First identifier in %v cannot be found", path)
		}
		dt, err := main.obj.GetDataType(id)
		if err != nil {
			return dmlSet{}, fmt.Errorf("First identifier in %v cannot be found", path)
		}
		dtObj, ok := self.objects[dt]
		if !ok {
			return dmlSet{}, fmt.Errorf("First identifier in %v cannot be found", path)
		}
		dbSet = dmlSet{id: id, obj: dtObj}

	} else {
		dbSet = main
	}

	for _, name := range names[1:] {

		//check all childs to find the one with given name
		child, err := dbSet.obj.(Data).GetSubobjectByName(dbSet.id, name, true)
		if err != nil {
			//it may be an identifier within the path!
			id, err := IdentifierFromEncoded(name)
			if err == nil {
				set, err := self.getObjectSet(id)
				if err != nil {
					return dmlSet{}, utils.StackError(err, "Name %v is not available in object %v", name, dbSet.id.Name)
				}
				child = set
			} else {
				return dmlSet{}, utils.StackError(err, "Name %v is not available in object %v", name, dbSet.id.Name)
			}
		}

		//check if it is a behaviour, and if so end here
		_, isBehaviour := child.obj.(Behaviour)
		if isBehaviour {
			return child, nil
		}
		dbSet = child
	}
	return dbSet, nil
}

//postprocess for all done operations. Entry point for behaviours
//it executes behaviours including possible rollback if failed.
//if rollbackonly = true than a full rollback is executed
func (self *Runtime) postprocess(rollbackOnly bool) error {

	//ensure we always commit at the end
	defer self.datastore.Commit()

	//if not a full rollback we check if something has changed and start the relevant
	//behaviours. We only roll back if something goes wrong
	var postError error = nil
	rollback := rollbackOnly

	if !rollbackOnly {

		//handle behaviours... none yet. Transaction is handled in normal
		//execution flow
	}

	//rollback if required (caller requested or behaviours failed)
	if rollback {
		self.datastore.RollbackKeepOpen()
	}

	return postError
}

func (self *Runtime) importDML(astImp *astImport) error {

	//load the file and create a reader
	imppath := filepath.Join(self.importPath, astImp.File)
	reader, err := os.Open(imppath)
	if err != nil {
		return utils.StackError(err, "Unable to read %v (%v)", astImp.File, imppath)
	}
	defer reader.Close()

	//we start with building the AST
	ast := &DML{}
	parser, err := participle.Build(&DML{}, participle.Lexer(&dmlDefinition{}))
	if err != nil {
		return utils.StackError(err, "Unable to setup parser")
	}

	err = parser.Parse(reader, ast)
	if err != nil {
		return utils.StackError(err, "Unable to parse %v", astImp.File)
	}

	//first import everything needed
	for _, imp := range ast.Imports {
		err := self.importDML(imp)
		if err != nil {
			return utils.StackError(err, "Import failed: %v", imp.File)
		}
	}

	//we now register the imported ast as a creator
	creator := func(rntm *Runtime) (Object, error) {
		return rntm.buildObject(ast.Object)
	}

	//build the name, file or alias
	var name string
	if astImp.Alias != "" {
		name = astImp.Alias

	} else {
		file := filepath.Base(astImp.File)
		var extension = filepath.Ext(file)
		name = file[0 : len(file)-len(extension)]
	}

	return self.RegisterObjectCreator(name, creator)
}

//get the logic object for the datatype if it is complex
func (self *Runtime) getOrCreateObject(dt DataType) (Object, error) {

	if !dt.IsComplex() {
		return nil, fmt.Errorf("DataType is not complex")
	}

	obj, ok := self.objects[dt]
	if !ok {
		//object not yet setup. lets build it!
		ast, err := dt.complexAsAst()
		if err != nil {
			return nil, utils.StackError(err, "Unable to parse DataType for object creation")
		}
		obj, err = self.buildObject(ast)
		if err != nil {
			return nil, utils.StackError(err, "Unable to build object from DataType")
		}
		self.objects[dt] = obj
	}

	return obj, nil
}

//return the full dmlSet for the identifier, including the logic object
func (self *Runtime) getObjectSet(id Identifier) (dmlSet, error) {

	//get any object from the map
	obj, ok := self.objects[self.main]
	if !ok {
		return dmlSet{}, fmt.Errorf("Runtime seems bot to be setup correctly")
	}

	dt, err := obj.GetDataType(id)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access DB for identifier")
	}
	obj, err = self.getOrCreateObject(dt)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to create or access object for identifier")
	}
	return dmlSet{id: id, obj: obj}, nil
}

//Construct a data object from encoded description (as provided by type property)
// - it does not call "Created()" for event emission: to be done by caller
// - it does not call "SetObjectPath": to be done by caller
func (self *Runtime) constructObjectSet(dt DataType, parent Identifier) (dmlSet, error) {

	//build or get
	obj, err := self.getOrCreateObject(dt)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to access or create object for datatype")
	}

	//setup DB
	id, err := self.setupObject(obj, parent)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to setup database for object")
	}

	return dmlSet{id: id, obj: obj}, nil
}

//due to recursive nature of objects we need an extra function
func (self *Runtime) buildObject(astObj *astObject) (Object, error) {

	//get the datatype, and check if we have that type already
	dt := MustNewDataType(astObj)

	//check if we build this already
	obj, exist := self.objects[dt]
	if exist {
		return obj, nil
	}

	//we need the objects name first. Search for the id property assignment
	var objName string
	for _, astAssign := range astObj.Assignments {
		if astAssign.Key[0] == "name" {
			objName = *astAssign.Value.String
		}
	}

	//build the object!
	creator, ok := self.creators[astObj.Identifier]
	if !ok {
		return nil, fmt.Errorf("No object type \"%s\" exists", astObj.Identifier)
	}

	var err error
	obj, err = creator(self)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create object %v (%v)", objName, astObj.Identifier)
	}
	obj.SetObjectDataType(dt)
	self.objects[dt] = obj

	//Now we create all additional properties and set them up in js
	for _, astProp := range astObj.Properties {
		err := self.addProperty(obj, astProp)
		if err != nil {
			return nil, utils.StackError(err, "Unable to create property %v in object %v", astProp.Key, objName)
		}
	}
	err = obj.SetupProperties(self, obj.GetJSPrototype(), obj)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create javascript property interface for %v", objName)
	}

	//now we create all new events
	for _, astEvent := range astObj.Events {

		event, err := self.buildEvent(astEvent, obj)
		if err != nil {
			return nil, utils.StackError(err, "Unable to create event %v in object %v", astEvent.Key, objName)
		}
		err = obj.AddEvent(event)
		if err != nil {
			return nil, utils.StackError(err, "Unable to add event %v to object %v", astEvent.Key, objName)
		}
	}
	obj.SetupJSEvents(obj.GetJSPrototype())

	//than all methods (including defaults if required)
	for _, fnc := range astObj.Functions {

		method, err := self.buildMethod(fnc)
		if err != nil {
			return nil, utils.StackError(err, "Unable to create method %v in object %v", *fnc.Name, objName)
		}
		obj.AddMethod(*fnc.Name, method)
	}
	obj.SetupJSMethods(self, obj.GetJSPrototype())

	//expose parent to js
	getter := self.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {
		id := call.This.ToObject(self.jsvm).Get("identifier").Export()
		identifier, ok := id.(Identifier)
		if !ok {
			panic(fmt.Sprintf("Called object does not have identifier setup correctly: %v", id))
		}
		parent, err := obj.GetParent(identifier)
		if err != nil {
			//return nil, as no parent is also returned as error
			return self.jsvm.ToValue(nil)
		}
		if !parent.valid() {
			return self.jsvm.ToValue(nil)
		}
		return parent.obj.GetJSObject(parent.id)
	})
	obj.GetJSPrototype().DefineAccessorProperty("parent", getter, nil, goja.FLAG_FALSE, goja.FLAG_TRUE)

	//go on with all subobjects (if not behaviour)
	_, isBehaviour := obj.(Behaviour)
	if !isBehaviour {

		for _, astChild := range astObj.Objects {

			//build the child
			child, err := self.buildObject(astChild)
			if err != nil {
				return nil, err
			}

			//check if this child is a behaviour, and handle accordingly
			behaviour, isBehaviour := child.(Behaviour)
			if isBehaviour {
				obj.(Data).AddBehaviourObject(astChild.Identifier, behaviour)
			} else {
				obj.(Data).AddChildObject(child.(Data))
			}

			//expose child as parent property (do this here as now name is easily accessbile
			childName := child.GetProperty("name").GetDefaultValue().(string)
			getter = self.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {
				id := call.This.ToObject(self.jsvm).Get("identifier").Export()
				identifier, ok := id.(Identifier)
				if !ok {
					panic(fmt.Sprintf("Called object does not have identifier setup correctly: %v", id))
				}

				var name = childName
				child, err := obj.(Data).GetChildByName(identifier, name)
				if err != nil {
					panic(utils.StackError(err, "Unable to access child").Error())
				}

				return child.obj.GetJSObject(child.id)
			})
			obj.GetJSPrototype().DefineAccessorProperty(childName, getter, nil, goja.FLAG_FALSE, goja.FLAG_TRUE)

		}

		//expose children list to javascript
		getter := self.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {
			id := call.This.ToObject(self.jsvm).Get("identifier").Export()
			identifier, ok := id.(Identifier)
			if !ok {
				panic(fmt.Sprintf("Called object does not have identifier setup correctly: %v", id))
			}
			children, err := obj.(Data).GetChildren(identifier)
			if err != nil {
				panic(utils.StackError(err, "Unable to collect children").Error())
			}
			result := make([]*goja.Object, len(children))
			for i, child := range children {
				result[i] = child.obj.GetJSObject(child.id)
			}
			return self.jsvm.ToValue(result)
		})
		obj.GetJSPrototype().DefineAccessorProperty("children", getter, nil, goja.FLAG_FALSE, goja.FLAG_TRUE)

	} else if len(astObj.Objects) > 0 {
		return nil, fmt.Errorf("Behaviours cannot have child objects")
	}

	//with everything in place we are able to process the assignments
	for _, assign := range astObj.Assignments {

		//get the object we need to assign to
		assignObj := obj
		var assignIdx int
		for i, subkey := range assign.Key {
			assignIdx = i
			if assignObj.HasProperty(subkey) || assignObj.HasEvent(subkey) {
				break
			}
		}

		parts := assign.Key[assignIdx:]
		if assignObj.HasProperty(parts[0]) {
			err := self.assignProperty(assign, assignObj.GetProperty(parts[0]))
			if err != nil {
				return nil, utils.StackError(err, "Unable to assign to property")
			}
		} else if obj.HasEvent(parts[0]) {
			err := self.assignEvent(assign, assignObj.GetEvent(parts[0]))
			if err != nil {
				return nil, err
			}

		} else {
			return nil, fmt.Errorf("Key of assignment is not a property or event")
		}
	}

	return obj, nil
}

func (self *Runtime) setupObject(obj Object, parent Identifier) (Identifier, error) {

	//get the infos we need for the ID
	astObj, err := obj.GetObjectDataType().complexAsAst()
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to setup object")
	}
	objType := astObj.Identifier
	var objName string
	for _, astAssign := range astObj.Assignments {
		if astAssign.Key[0] == "name" {
			objName = *astAssign.Value.String
		}
	}

	//build the ID
	uid := uuid.NewV4().String()
	id := Identifier{parent.Hash(), objType, objName, uid}

	//initalize the DB
	err = obj.InitializeDB(id)
	if err != nil {
		return Identifier{}, err
	}

	//write the requiered values
	err = obj.SetDataType(id, obj.GetObjectDataType())
	if err != nil {
		return Identifier{}, err
	}
	err = obj.SetParentIdentifier(id, parent)
	if err != nil {
		return Identifier{}, err
	}

	//setup all children and behaviours
	data, isData := obj.(Data)
	if isData {
		children := data.GetChildObjects()
		for _, child := range children {
			childID, err := self.setupObject(child, id)
			if err != nil {
				return Identifier{}, err
			}
			err = data.AddChildIdentifier(id, childID)
			if err != nil {
				return Identifier{}, err
			}
		}

		behaviours := data.Behaviours()
		for _, behaviour := range behaviours {
			bhvrID, err := self.setupObject(data.GetBehaviourObject(behaviour), id)
			if err != nil {
				return Identifier{}, err
			}
			err = data.SetBehaviourIdentifier(id, bhvrID.Type, bhvrID)
			if err != nil {
				return Identifier{}, err
			}
		}
	}

	return id, nil
}

func (self *Runtime) assignProperty(asgn *astAssignment, prop Property) error {

	//we really assign a property, lets go
	if asgn.Function != nil {
		return fmt.Errorf("A function cannot be assigned to a property")
	}

	var err error = nil
	if asgn.Value.Int != nil {
		err = prop.SetDefaultValue(*asgn.Value.Int)
	} else if asgn.Value.String != nil {
		err = prop.SetDefaultValue(*asgn.Value.String)
	} else if asgn.Value.Number != nil {
		err = prop.SetDefaultValue(*asgn.Value.Number)
	} else if asgn.Value.Bool != nil {
		err = prop.SetDefaultValue(bool(*asgn.Value.Bool))
	} else if asgn.Value.Type != nil {
		err = prop.SetDefaultValue(MustNewDataType(asgn.Value.Type))
	}
	return err
}

func (self *Runtime) assignEvent(asgn *astAssignment, evt Event) error {

	if asgn.Function == nil {
		return fmt.Errorf("Only function can be asigned to event")
	}

	//wrap it into something that can be processed by goja
	//(a anonymous function on its own is not allowed)
	val, err := self.jsvm.RunString("fnc = function(" +
		strings.Join(asgn.Function.Parameters[:], ",") +
		")" + asgn.Function.Body)

	if err != nil {
		return err
	}
	fnc, ok := val.Export().(func(goja.FunctionCall) goja.Value)
	if !ok {
		return fmt.Errorf("Must be function")
	}
	evt.RegisterObjectJSCallback(fnc)

	//cleanup the global var we needed
	_, err = self.jsvm.RunString("delete fnc")
	if err != nil {
		return err
	}

	return nil
}

//this function adds the javascript method directly to the object
func (self *Runtime) buildMethod(astFnc *astFunction) (Method, error) {

	if astFnc.Name == nil {
		return nil, fmt.Errorf("Object method must have a name")
	}

	code := *astFnc.Name + "= function(" +
		strings.Join(astFnc.Parameters[:], ",") + ")" +
		astFnc.Body

	val, err := self.jsvm.RunString(code)
	if err != nil {
		return nil, err
	}

	//cleanup the global var we needed
	_, err = self.jsvm.RunString("delete " + *astFnc.Name)
	if err != nil {
		return nil, err
	}

	return NewMethod(val.Export(), astFnc.Const == "const")
}

//func (self *Runtime) registerEvtFunction

func (self *Runtime) buildEvent(astEvt *astEvent, parent Object) (Event, error) {

	//build the event
	evt := NewEvent(astEvt.Key, parent)

	//and see if we should add a default callback
	if astEvt.Default != nil {
		//wrap it into something that can be processed by goja
		//(a anonymous function on its own is not allowed)
		val, err := self.jsvm.RunString("fnc = function(" +
			strings.Join(astEvt.Default.Parameters[:], ",") +
			")" + astEvt.Default.Body)

		if err != nil {
			return nil, err
		}
		fnc, ok := val.Export().(func(goja.FunctionCall) goja.Value)
		if !ok {
			return nil, fmt.Errorf("Must be function")
		}
		evt.RegisterObjectJSCallback(fnc)

		//cleanup the global var we needed
		_, err = self.jsvm.RunString("delete fnc")
		if err != nil {
			return nil, err
		}
	}

	return evt, nil
}

func (self *Runtime) addProperty(obj Object, astProp *astProperty) error {

	//property can have only plain types
	if astProp.Type.Object != nil {
		return fmt.Errorf("object can only be of plain type")
	}
	var dt DataType
	switch astProp.Type.Pod {
	case "string":
		dt = MustNewDataType("string")
	case "int":
		dt = MustNewDataType("int")
	case "float":
		dt = MustNewDataType("float")
	case "bool":
		dt = MustNewDataType("bool")
	case "type":
		dt = MustNewDataType("type")
	case "raw":
		dt = MustNewDataType("raw")
	}

	var constprop bool = false
	if astProp.Const != "" {
		constprop = true
	}

	if astProp.Default == nil {
		if constprop {
			return fmt.Errorf("Constant property needs to have a value assigned")
		}

		err := obj.AddProperty(astProp.Key, dt, dt.GetDefaultValue(), constprop)
		if err != nil {
			return utils.StackError(err, "Cannot add property to object")
		}

		return nil
	}

	var err error
	if astProp.Default.String != nil {
		err = obj.AddProperty(astProp.Key, dt, *astProp.Default.String, constprop)

	} else if astProp.Default.Int != nil {
		err = obj.AddProperty(astProp.Key, dt, *astProp.Default.Int, constprop)

	} else if astProp.Default.Number != nil {
		err = obj.AddProperty(astProp.Key, dt, *astProp.Default.Number, constprop)

	} else if astProp.Default.Bool != nil {
		err = obj.AddProperty(astProp.Key, dt, bool(*astProp.Default.Bool), constprop)

	} else if astProp.Default.Type != nil {
		err = obj.AddProperty(astProp.Key, dt, MustNewDataType(astProp.Default.Type), constprop)
	}

	if err != nil {
		return utils.StackError(err, "Unable to add property to object")
	}

	return nil
}

//This function is called by all emitted events. It does forward it to outside of
//The runtime
func (self *Runtime) emitEvent(objPath, event string, args ...interface{}) error {

	eventpath := objPath + "." + event
	for _, cb := range self.eventCBs {
		cb(eventpath, args...)
	}
	return nil
}
