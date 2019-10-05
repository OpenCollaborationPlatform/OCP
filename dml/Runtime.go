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
	datastore "github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

//Function prototype that can create new object types in DML
type CreatorFunc func(id Identifier, parent Identifier, rntm *Runtime) Object

func NewRuntime(ds *datastore.Datastore) *Runtime {

	ds.Begin()
	defer ds.Commit()

	//js runtime with console support
	js := goja.New()
	new(require.Registry).Enable(js)
	console.Enable(js)

	cr := make(map[string]CreatorFunc, 0)
	rntm := &Runtime{
		creators:           cr,
		jsvm:               js,
		jsObjMap:           js.NewObject(),
		datastore:          ds,
		mutex:              &sync.Mutex{},
		ready:              false,
		currentUser:        "none",
		objects:            make(map[Identifier]Data, 0),
		mainObj:            nil,
		initialObjRefcount: 0,
		transactions:       &TransactionManager{},
	}

	//build the managers and expose
	transMngr, err := NewTransactionManager(rntm)
	if err != nil {
		panic(utils.StackError(err, "Unable to initilize transaction manager"))
	}
	rntm.transactions = transMngr
	rntm.jsvm.Set("Transaction", transMngr.jsobj)

	//add the datastructures
	rntm.RegisterObjectCreator("Data", NewData)
	rntm.RegisterObjectCreator("Variant", NewVariant)
	rntm.RegisterObjectCreator("Vector", NewVector)
	rntm.RegisterObjectCreator("Transaction", NewTransactionBehaviour)

	//setup globals
	rntm.jsvm.Set("Objects", rntm.jsObjMap)
	SetupGlobals(rntm)

	return rntm
}

//builds a datastructure from a file
// - existing types must be registered to be recognized during parsing
type Runtime struct {
	creators map[string]CreatorFunc

	//components of the runtime
	jsvm      *goja.Runtime
	jsObjMap  *goja.Object
	datastore *datastore.Datastore
	mutex     *sync.Mutex

	//internal state
	importPath 		  string
	ready              Boolean //True if a datastructure was read and setup, false if no dml file was parsed
	currentUser        User    //user that currently access the runtime
	objects            map[Identifier]Data
	mainObj            Data
	initialObjRefcount uint64

	//managers
	transactions *TransactionManager
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

	self.datastore.Begin()
	defer self.datastore.Commit()

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

	//we now build up the unchangeable object structure, hence set refcount super high
	self.initialObjRefcount = uint64(math.MaxUint64 / 2)

	//first import everything needed
	
	for _, imp := range ast.Imports {
		err := self.importDML(imp)
		if err != nil {
			return utils.StackError(err, "Import failed: %v", imp.File)
		}
	}

	//process the AST into usable objects
	obj, err := self.buildObject(ast.Object, Identifier{}, "", make([]*astObject, 0))
	if err != nil {
		return utils.StackError(err, "Unable to parse dml code")
		//TODO clear the database entries...
	}
	self.mainObj = obj.(Data)
	self.ready = true

	//all objects created from here on are deletable
	self.initialObjRefcount = 0

	//set the JS main entry point
	self.jsvm.Set(self.mainObj.Id().Name, self.mainObj.GetJSObject())

	//commit objects which are totally new (so they have no updates initially)
	for _, obj := range self.objects {
		if has, _ := obj.HasUpdates(); has {
			obj.FixStateAsVersion()
		}
	}

	return err
}

//calls setup on all Data Objects. The string is the path up to the object, 
//e.g. for object Toplevel.Sublevel.MyObject path ist Toplevel.Sublevel
//th epath is empty for the main object as well as all dynamic data, e.g. vector entries
func (self *Runtime) SetupAllObjects(setup func(string, Data) error) error {
	
	//check if setup is possible
	if !self.ready {
		return fmt.Errorf("Unable to setup objects: Runtime not initialized")
	}

	//iterate all data object in the hirarchy
	has := make(map[Identifier]struct{}, 0)
	path := "" 
	err := setupDataChildren(path, self.mainObj, has, setup)
	if err != nil {
		return utils.StackError(err, "Unable to setup all objects")
	}

	//call setup for all dynamic objects (objects in the global list but not yet
	//reached via the hirarchy)
	for _, obj := range self.objects {
		_, ok := has[obj.Id()]
		if ok {
			continue
		}
		dataobj, ok := obj.(Data)
		if !ok {
			continue
		}
		err := setup("", dataobj)
		if err != nil { 
			return utils.StackError(err, "Unable to setup all dynamic objects")
		}
	}
	
	return nil
}

//Function to extend the available data and behaviour types for this runtime
func (self *Runtime) RegisterObjectCreator(name string, fnc CreatorFunc) error {

	_, ok := self.creators[name]
	if ok {
		return fmt.Errorf("Object name already registered")
	}

	self.creators[name] = fnc
	return nil
}


// 						Accessing / executing Methods
//*********************************************************************************

//run arbitrary javascript code on the loaded structure
func (self *Runtime) RunJavaScript(user User, code string) (interface{}, error) {

	self.datastore.Begin()

	//save the user for processing
	self.currentUser = user

	val, err := self.jsvm.RunString(code)

	if err != nil {
		self.postprocess(true)
		return nil, err
	}

	err = self.postprocess(false)

	//check if it is a dml object and convert to dml object
	obj, ok := val.(*goja.Object)
	if ok {
		
		fncobj := obj.Get("Identifier")
		if fncobj != nil {
			fnc := fncobj.Export()
			wrapper, ok := fnc.(func(goja.FunctionCall) goja.Value)
			if ok {
				encoded := wrapper(goja.FunctionCall{})
				id, err := IdentifierFromEncoded(encoded.Export().(string))
				if err != nil {
					return nil, utils.StackError(err, "Unable to convert returned object from javascript")
				}
				obj, ok := self.objects[id]
				if !ok {
					return nil, utils.StackError(err, "Cannot find object returned from javascript")
				}
				return obj, nil
	
			} else {
				return nil, fmt.Errorf("Javascript returned object, but it has errous Identifier method")
			}
		}
	}

	//return default values
	return val.Export(), err
}

func (self *Runtime) CallMethod(user User, path string, method string, args ...interface{}) (interface{}, error) {

	self.datastore.Begin()

	//save the user for processing
	self.currentUser = user

	//first check if path is correct and method available
	obj, err := self.getObjectFromPath(path)
	if err != nil {
		self.datastore.Rollback()
		return nil, utils.StackError(err, "Unable to find object %v", path)
	}
	if !obj.HasMethod(method) {
		self.datastore.Rollback()
		return nil, fmt.Errorf("No method %v available in object %v", method, path)
	}

	fnc := obj.GetMethod(method)
	result := fnc.Call(args...)

	//did somethign go wrong?
	err, ok := result.(error)
	if ok && err != nil {
		self.postprocess(true)
		return nil, utils.StackError(err, "Execution of function failed")
	}

	//postprocess correctly
	err = self.postprocess(false)

	return result, err
}

func (self *Runtime) ReadProperty(user User, path string, property string) (interface{}, error) {

	self.datastore.Begin()
	defer self.datastore.Rollback()

	//save the user for processing
	self.currentUser = user

	//first check if path is correct and method available
	obj, err := self.getObjectFromPath(path)
	if err != nil {
		return nil, err
	}
	if !obj.HasProperty(property) {
		return nil, fmt.Errorf("No property %v available in object %v", property, path)
	}

	prop := obj.GetProperty(property)
	return prop.GetValue(), nil
}

func (self *Runtime) WriteProperty(user User, path string, property string, val interface{}) error {

	self.datastore.Begin()

	//save the user for processing
	self.currentUser = user

	//first check if path is correct and method available
	obj, err := self.getObjectFromPath(path)
	if err != nil {
		self.datastore.Rollback()
		return err
	}
	if !obj.HasProperty(property) {
		self.datastore.Rollback()
		return fmt.Errorf("No property %v available in object %v", property, path)
	}

	prop := obj.GetProperty(property)
	err = prop.SetValue(val)
	if err != nil {
		self.postprocess(true)
		return utils.StackError(err, "Setting property failed")
	}

	//postprocess correctly
	return self.postprocess(false)
}

// 							Internal Functions
//*********************************************************************************

func setupDataChildren(path string, obj Data, has map[Identifier]struct{}, setup func(string, Data) error) error {
	
	//execute on the object itself!
	err := setup(path, obj)
	if err != nil {
		return err
	}
	has[obj.Id()] = struct{}{}
	
	//advance path and execute all children
	path = path + "." + obj.Id().Name
	for _, child := range obj.GetChildren() {
		
		datachild, ok := child.(Data)
		if ok {
			err := setupDataChildren(path, datachild, has, setup)
			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

func (self *Runtime) removeObject(obj Object) {

	delete(self.objects, obj.Id())
	//TODO: unkown how to do it from go
	self.jsvm.RunString(fmt.Sprintf("delete Objects.%v", obj.Id().Encode()))
}

//get the object from the identifier path list (e.g. myobj.childname.yourobject)
//alternatively to names it can include identifiers (e.g. from Object.Identifier())
func (self *Runtime) getObjectFromPath(path string) (Object, error) {

	names := strings.Split(path, `.`)
	if len(names) == 0 {
		return nil, fmt.Errorf("Not a valid path to object: no IDs found")
	}

	var obj Data
	if names[0] != self.mainObj.Id().Name {
		id, err := IdentifierFromEncoded(names[0])
		if err != nil {
			return nil, fmt.Errorf("First identifier %v cannot be found", names[0])
		}
		listobj, ok := self.objects[id]
		if !ok{
			return nil, fmt.Errorf("First identifier %v cannot be found", names[0])
		}
		obj = listobj
	
	} else {
		obj = self.mainObj	
	}

	for _, name := range names[1:] {

		//check all childs to find the one with given name
		child, err := obj.GetChildByName(name)
		if err != nil {
			//maybe its an identifier
			id, err := IdentifierFromEncoded(name)
			if err != nil {
				return nil, fmt.Errorf("Identifier %v cannot be found", name)
			}
			listobj, ok := self.objects[id]
			if !ok{
				return nil, fmt.Errorf("Identifier %v cannot be found", name)
			}
			obj = listobj
		
		} else {
			obj = child
		}
	}
	return obj, nil
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

		//check all objects if anything has changed
		for _, obj := range self.objects {
			has, err := obj.HasUpdates()
			if err != nil {
				return utils.StackError(err, "Unable to check object for new updates")
			}
			if has {

				//handle transaction behaviour
				if obj.HasBehaviour("Transaction") {
					err := self.transactions.Add(obj)
					if err != nil {
						postError = utils.StackError(err, "Adding to Transaction failed: Rollback")
						rollback = true
						break
					}
				}

				//fix the data
				_, err := obj.FixStateAsVersion()
				if err != nil {
					postError = utils.StackError(err, "Fixing version failed: Rollback")
					rollback = true
					break
				}

				//handle the versioning

			}
		}
	}

	//rollback if required (caller requested or behaviours failed)
	if rollback {
		self.datastore.Rollback()
		self.datastore.Begin() //reopen to allow GetRefCount()
	}

	//garbace collection: which objects can be removed?
	removers := make([]Identifier, 0)
	for id, obj := range self.objects {
		rc, err := obj.GetRefcount()
		if err != nil {
			return err
		}
		if rc == 0 {
			removers = append(removers, id)
		}
	}
	for _, id := range removers {
		delete(self.objects, id)
		self.jsObjMap.Set(id.Encode(), nil)
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
	creator := func(id Identifier, parent Identifier, rntm *Runtime) Object {
		//to assgn the correct object name we need to override the ID pasignment. This must be available
		//on object build time as the identifier is created with it
		idSet := false
		for _, astAssign := range ast.Object.Assignments {
			if astAssign.Key[0] == "name" {
				*astAssign.Value.String = id.Name
				idSet = true
				break
			}
		}
		if !idSet {
			val := &astValue{String: &id.Name}
			asgn := &astAssignment{Key: []string{"name"}, Value: val}
			ast.Object.Assignments = append(ast.Object.Assignments, asgn)
		}

		obj, _ := rntm.buildObject(ast.Object, parent, id.Uuid, make([]*astObject, 0))
		return obj
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

//due to recursive nature of objects we need an extra function
func (self *Runtime) buildObject(astObj *astObject, parent Identifier, uuid string, recBehaviours []*astObject) (Object, error) {

	//see if we can build it, and do so if possible
	creator, ok := self.creators[astObj.Identifier]
	if !ok {
		return nil, fmt.Errorf("No object type \"%s\" exists", astObj.Identifier)
	}

	//we need the objects name first. Search for the id property assignment
	var objName string
	for _, astAssign := range astObj.Assignments {
		if astAssign.Key[0] == "name" {
			objName = *astAssign.Value.String
		}
	}
	
	//create the object ID and check for uniqueness
	id := Identifier{Parent: parent.Hash(), Name: objName, Type: astObj.Identifier, Uuid: uuid}
	_, has := self.objects[id]
	if has {	
		return nil, fmt.Errorf("Object with same type (%s) and ID (%s) already exists", id.Type, id.Name)
	}

	//setup the object including datastore
	obj := creator(id, parent, self)
	obj.SetDataType(MustNewDataType(astObj))
	obj.SetRefcount(self.initialObjRefcount)

	//check if data or behaviour
	_, isBehaviour := obj.(Behaviour)

	if !isBehaviour {
		//add to rntm
		self.objects[obj.Id()] = obj.(Data)
		self.jsObjMap.Set(obj.Id().Encode(), obj.GetJSObject())
	}

	//expose to javascript
	jsobj := obj.GetJSObject()
	if parent.Valid() && objName != "" {
		parentObj := self.objects[parent]
		parentObj.GetJSObject().Set(objName, jsobj)
	}

	//Now we create all additional properties and set them up in js
	for _, astProp := range astObj.Properties {
		err := self.addProperty(obj, astProp)
		if err != nil {
			return nil, utils.StackError(err, "Unable to create property %v in object %v", astProp.Key, objName)
		}
	}
	err := obj.SetupJSProperties(self.jsvm, jsobj)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create javascript property interface for %v", objName)
	}

	//now we create all new events
	for _, astEvent := range astObj.Events {

		event, err := self.buildEvent(astEvent, obj)
		if err != nil {
			return nil, utils.StackError(err, "Unable to create event %v in object %v", astEvent.Key, objName)
		}
		err = obj.AddEvent(astEvent.Key, event)
		if err != nil {
			return nil, utils.StackError(err, "Unable to add event %v to object %v", astEvent.Key, objName)
		}
		jsobj.Set(astEvent.Key, obj.GetEvent(astEvent.Key).GetJSObject())
	}

	//than all methods (including defaults if required)
	for _, fnc := range astObj.Functions {

		method, err := self.buildMethod(fnc)
		if err != nil {
			return nil, utils.StackError(err, "Unable to create method %v in object %v", fnc.Name, objName)
		}
		obj.AddMethod(*fnc.Name, method)
	}
	obj.SetupJSMethods(self.jsvm, jsobj)

	//go on with all subobjects (if not behaviour)
	if !isBehaviour {

		//create our local version of the recursive behaviour map to allow adding values for children
		localRecBehaviours := make([]*astObject, len(recBehaviours))
		copy(localRecBehaviours, recBehaviours)

		//sort children to process behaviours first, so that recursive ones are
		//ready for all children
		objectToFrontIfAvailable(&astObj.Objects, `Transaction`)
		objectToFrontIfAvailable(&astObj.Objects, `Version`)

		//add the recursive Behaviours
		for _, bvr := range localRecBehaviours {
			addFrontIfNotAvailable(&astObj.Objects, bvr)
		}

		jsChildren := make([]goja.Value, 0)

		for _, astChild := range astObj.Objects {

			//build the child
			child, err := self.buildObject(astChild, obj.Id(), "", localRecBehaviours)
			if err != nil {
				return nil, err
			}

			//check if this child is a behaviour, and if so if it is recursive
			behaviour, isBehaviour := child.(Behaviour)
			if isBehaviour && behaviour.GetProperty(`recursive`).GetValue().(bool) {
				localRecBehaviours = append(localRecBehaviours, astChild)
			}

			//handle hirarchy
			if isBehaviour {
				obj.(Data).AddBehaviour(behaviour.Id().Type, behaviour)
			} else {
				obj.(Data).AddChild(child.(Data))

			}
			jsChildren = append(jsChildren, child.GetJSObject())
		}

		jsobj.DefineDataProperty("children", self.jsvm.ToValue(jsChildren), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)

	} else if len(astObj.Objects) > 0 {
		return nil, fmt.Errorf("Behaviours cannot have child objects")
	}

	//with everything in place we are able to process the assignments
	//needs to be here
	for _, assign := range astObj.Assignments {

		//get the object we need to assign to
		assignObj := obj
		var assignIdx int
		for i, subkey := range assign.Key {
			assignIdx = i
			if assignObj.HasProperty(subkey) || assignObj.HasEvent(subkey) {
				break
			}
			if assignObj.GetParent().Id().Name == subkey {
				assignObj = assignObj.GetParent()
				continue
			}
			data, ok := assignObj.(Data)
			if ok {
				child, err := data.GetChildByName(subkey)
				if err != nil {
					return nil, fmt.Errorf("Unable to assign to %v: unknown identifier", assign.Key)
				}
				assignObj = child
			} else {
				return nil, fmt.Errorf("Unable to assign to %v: unknown identifier", assign.Key)
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

	//load the dynamic objects
	dynamic, ok := obj.(DynamicData)
	if ok {
		err := dynamic.Load()
		if err != nil {
			return nil, utils.StackError(err, "Unable to create object, dynamic data cannot be loaded")
		}
	}

	//ant last we add the general object properties: parent etc...
	if parent.Valid() {
		jsobj.DefineDataProperty("parent", self.objects[parent].GetJSObject(), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	} else {
		jsobj.DefineDataProperty("parent", self.jsvm.ToValue(nil), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	}

	return obj, nil
}

func (self *Runtime) assignProperty(asgn *astAssignment, prop Property) error {

	parts := asgn.Key

	//possibilities are:
	//	1.the last key is the property
	//	2.the second last is the property and the last the event
	if prop.HasEvent(parts[len(parts)-1]) {
		return self.assignEvent(asgn, prop.GetEvent(parts[len(parts)-1]))
	}

	//we really assign a property, lets go
	if asgn.Function != nil {
		return fmt.Errorf("A function cannot be assigned to a property")
	}

	if asgn.Value.Int != nil {
		prop.SetValue(*asgn.Value.Int)
	} else if asgn.Value.String != nil {
		prop.SetValue(*asgn.Value.String)
	} else if asgn.Value.Number != nil {
		prop.SetValue(*asgn.Value.Number)
	} else if asgn.Value.Bool != nil {
		prop.SetValue(bool(*asgn.Value.Bool))
	} else if asgn.Value.Type != nil {
		prop.SetValue(MustNewDataType(asgn.Value.Type))
	}
	return nil
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
	evt.RegisterJSCallback(fnc)

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

	return NewMethod(val.Export())
}

//func (self *Runtime) registerEvtFunction

func (self *Runtime) buildEvent(astEvt *astEvent, parent Object) (Event, error) {

	//build the arguements slice
	args := make([]DataType, len(astEvt.Params))
	for i, ptype := range astEvt.Params {
		//only pod types allowd
		if ptype.Object != nil {
			return nil, fmt.Errorf("event arguments can only be POD types")
		}
		ptype := MustNewDataType(ptype.Pod)
		args[i] = ptype
	}

	//build the event
	evt := NewEvent(parent.GetJSObject(), self.jsvm, args...)

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
		evt.RegisterJSCallback(fnc)

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

//helper to move certain object to the front ob the objectlist if they are in the list
func objectToFrontIfAvailable(objects *[]*astObject, name string) {

	if len((*objects)) == 0 || (*objects)[0].Identifier == name {
		return
	}
	if (*objects)[len(*objects)-1].Identifier == name {
		(*objects) = append([]*astObject{(*objects)[len(*objects)-1]}, (*objects)[:len(*objects)-1]...)
		return
	}
	for p, x := range *objects {
		if x.Identifier == name {
			(*objects) = append([]*astObject{x}, append((*objects)[:p], (*objects)[p+1:]...)...)
			break
		}
	}
}

//helper to add a astObject in the front of the slice if not yet in the slice
func addFrontIfNotAvailable(objects *[]*astObject, obj *astObject) {

	//check if already available
	for _, x := range *objects {
		if x.Identifier == obj.Identifier {
			return
		}
	}

	//add front
	*objects = append([]*astObject{obj}, (*objects)...)
}
