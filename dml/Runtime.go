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
	datastore "CollaborationNode/datastores"
	"CollaborationNode/utils"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

//Function prototype that can create new object types in DML
type CreatorFunc func(name string, parent identifier, rntm *Runtime) Object

func NewRuntime(ds *datastore.Datastore) *Runtime {

	//js runtime with console support
	js := goja.New()
	new(require.Registry).Enable(js)
	console.Enable(js)

	cr := make(map[string]CreatorFunc, 0)
	rntm := &Runtime{
		creators:     cr,
		jsvm:         js,
		datastore:    ds,
		mutex:        &sync.Mutex{},
		ready:        false,
		currentUser:  "none",
		objects:      make(map[identifier]Data, 0),
		mainObj:      nil,
		transactions: &TransactionManager{},
	}

	//build the managers and expose
	transMngr, err := NewTransactionManager(rntm)
	if err != nil {
		panic(utils.StackError(err, "Unable to initilize transaction manager"))
	}
	rntm.transactions = transMngr
	rntm.jsvm.Set("Transaction", transMngr.jsobj)

	//add the datastructures
	rntm.registerObjectCreator("Data", NewData)
	rntm.registerObjectCreator("Vector", NewVector)
	rntm.registerObjectCreator("Transaction", NewTransactionBehaviour)

	return rntm
}

//builds a datastructure from a file
// - existing types must be registered to be recognized during parsing
type Runtime struct {
	creators map[string]CreatorFunc

	//components of the runtime
	jsvm      *goja.Runtime
	datastore *datastore.Datastore
	mutex     *sync.Mutex

	//internal state
	ready       Boolean //True if a datastructure was read and setup, false if no dml file was parsed
	currentUser User    //user that currently access the runtime
	objects     map[identifier]Data
	mainObj     Data

	//managers
	transactions *TransactionManager
}

// Setup / creation Methods
// ************************

//Parses the dml code and setups the full structure
func (self *Runtime) Parse(reader io.Reader) error {

	//no double loading
	if self.ready == true {
		return fmt.Errorf("Runtime is already setup")
	}

	//we start with building the AST
	ast := &DML{}
	parser, err := participle.Build(&DML{}, &dmlDefinition{})
	if err != nil {
		return err
	}

	err = parser.Parse(reader, ast)
	if err != nil {
		return utils.StackError(err, "Unable to parse dml code")
	}

	//process the AST into usable objects
	obj, err := self.buildObject(ast.Object, identifier{}, make([]*astObject, 0))
	if err != nil {
		return utils.StackError(err, "Unable to parse dml code")
		//TODO clear the database entries...
	}
	self.mainObj = obj.(Data)
	self.ready = true

	//set the JS main entry point
	self.jsvm.Set(self.mainObj.Id().Name, self.mainObj.GetJSObject())

	return err
}

// 						Accessing / executing Methods
//*********************************************************************************

//run arbitrary javascript code on the loaded structure
func (self *Runtime) RunJavaScript(code string) (interface{}, error) {

	state, err := self.preprocess()
	if err != nil {
		return nil, err
	}

	val, err := self.jsvm.RunString(code)

	if err != nil {
		self.postprocess(state, true)
		return nil, err
	}

	err = self.postprocess(state, false)

	return val.Export(), err
}

func (self *Runtime) CallMethod(path string, method string, args ...interface{}) (interface{}, error) {

	//first check if path is correct and method available
	obj, err := self.getObjectFromPath(path)
	if err != nil {
		return nil, err
	}
	if !obj.HasMethod(method) {
		return nil, fmt.Errorf("No method %v available in object %v", method, path)
	}

	//start preprocessing
	state, err := self.preprocess()
	if err != nil {
		return nil, err
	}

	fnc := obj.GetMethod(method)
	result := fnc.Call(args...)

	//did somethign go wrong?
	err, ok := result.(error)
	if ok && err != nil {
		self.postprocess(state, true)
		return nil, err
	}

	//postprocess correctly
	err = self.postprocess(state, false)

	return result, err
}

func (self *Runtime) RegisterEvent(path string, event string, cb EventCallback) error {

	//first check if path is correct and method available
	obj, err := self.getObjectFromPath(path)
	if err != nil {
		return err
	}
	if !obj.HasEvent(event) {
		return fmt.Errorf("No event %v available in object %v", event, path)
	}

	evt := obj.GetEvent(event)
	return evt.RegisterCallback(cb)
}

func (self *Runtime) ReadProperty(path string, property string) (interface{}, error) {

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

//for testing only, so unexportet
func (self *Runtime) getObject() (Object, error) {
	return self.mainObj, nil
}

// 							Internal Functions
//*********************************************************************************

//Function to extend the available data and behaviour types for this runtime
func (self *Runtime) registerObjectCreator(name string, fnc CreatorFunc) error {

	_, ok := self.creators[name]
	if ok {
		return fmt.Errorf("Object name already registered")
	}

	self.creators[name] = fnc
	return nil
}

//get the object from the identifier path list (e.g. myobj.childname.yourobject)
func (self *Runtime) getObjectFromPath(path string) (Object, error) {

	names := strings.Split(path, `.`)
	if len(names) == 0 {
		return nil, fmt.Errorf("Not a valid path to object: no IDs found")
	}

	if names[0] != self.mainObj.Id().Name {
		return nil, fmt.Errorf("First identifier cannot be found")
	}

	obj := self.mainObj
	for _, name := range names[1:] {

		//check all childs to find the one with given name
		child, err := obj.GetChildByName(name)
		if err != nil {
			return nil, err
		}
		obj = child
	}
	return obj, nil
}

//collect current state of objects for potential reset
func (self *Runtime) preprocess() (map[identifier]datastore.VersionID, error) {

	state := make(map[identifier]datastore.VersionID)
	for _, obj := range self.objects {

		//check if there are versions. If not we need to make sure the first one is created
		if !obj.HasVersions() {
			_, err := obj.FixStateAsVersion()
			if err != nil {
				return nil, utils.StackError(err, "Unable to create initial version of object for preprocessing")
			}
		}

		v, err := obj.GetLatestVersion()
		if err != nil {
			return nil, utils.StackError(err, "Unable to retrieve latest version of object during preprocessing")
		}
		state[obj.Id()] = v
	}
	return state, nil
}

//postprocess for all done operations. Entry point for behaviours
func (self *Runtime) postprocess(prestate map[identifier]datastore.VersionID, rollbackOnly bool) error {

	//if not a full rollback we check if something has changed and start the relevant
	//behaviours. We only roll back if something goes wrong
	var postError error = nil
	rollback := rollbackOnly

	if !rollbackOnly {

		//check all objects if anything has changed
		for _, obj := range self.objects {
			if obj.HasUpdates() {

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

	//do rollback if required (caller requested or behaviours failed)
	if rollback {
		for _, obj := range self.objects {

			v, ok := prestate[obj.Id()]
			if ok {
				//ok means obj was there bevore and only need to be reset

				latest, err := obj.GetLatestVersion()
				if err != nil {
					return utils.StackError(err, "Rollback failed")
				}

				if latest > v {
					//a new version was already created, we need to delete the old one
					//and reset the head accordingly
					err = obj.LoadVersion(v)
					if err != nil {
						if postError != nil {
							err = utils.StackError(postError, err.Error())
						}
						return utils.StackError(err, "Rollback failed")
					}

					err = obj.RemoveVersionsUpFrom(v)
					if err != nil {
						if postError != nil {
							err = utils.StackError(postError, err.Error())
						}
						return utils.StackError(err, "Rollback failed")
					}

					obj.ResetHead()
					err = obj.LoadVersion(datastore.VersionID(datastore.HEAD))
					if err != nil {
						if postError != nil {
							err = utils.StackError(postError, err.Error())
						}
						return utils.StackError(err, "Rollback failed")
					}

				} else {
					//no new version was created yet, a simple reset is enough
					obj.ResetHead()
				}
			}
			//TODO: Handle not ok: howto remove object correctly?
		}
	}
	return postError
}

//due to recursive nature og objects we need an extra function
func (self *Runtime) buildObject(astObj *astObject, parent identifier, recBehaviours []*astObject) (Object, error) {

	//see if we can build it, and do so if possible
	creator, ok := self.creators[astObj.Identifier]
	if !ok {
		return nil, fmt.Errorf("No object type \"%s\" exists", astObj.Identifier)
	}

	//we need the objects name first. Search for the id property assignment
	var objName string
	for _, astAssign := range astObj.Assignments {
		if astAssign.Key[0] == "id" {
			objName = *astAssign.Value.String
		}
	}
	if objName == "" {
		return nil, fmt.Errorf("we need the ID proeprty, otherwise everything falls appart")
	}

	//setup the object including datastore and check for uniqueness
	obj := creator(objName, parent, self)
	if parent.valid() {
		objId := obj.Id()
		//if unique in the parents childrens we are generaly unique, as parent is part of our ID
		for _, sibling := range self.objects[parent].GetChildren() {
			if sibling.Id().Type == objId.Type &&
				sibling.Id().Name == objId.Name {

				return nil, fmt.Errorf("Object with same type (%s) and ID (%s) already exists", objId.Type, objId.Name)
			}
		}
	}

	//check if data or behaviour
	bhvr, isBehaviour := obj.(Behaviour)

	//expose to javascript
	jsobj := obj.GetJSObject()
	if parent.valid() {
		parentObj := self.objects[parent]
		parentObj.GetJSObject().Set(objName, jsobj)
	}

	//Now we create all additional properties and set them up in js
	for _, astProp := range astObj.Properties {
		err := self.addProperty(obj, astProp)
		if err != nil {
			return nil, err
		}
	}
	err := obj.SetupJSProperties(self.jsvm, jsobj)
	if err != nil {
		return nil, err
	}

	//now we create all new events
	for _, astEvent := range astObj.Events {

		event, err := self.buildEvent(astEvent, obj)
		if err != nil {
			return nil, err
		}
		err = obj.AddEvent(astEvent.Key, event)
		if err != nil {
			return nil, err
		}
		jsobj.Set(astEvent.Key, obj.GetEvent(astEvent.Key).GetJSObject())
	}

	//than all methods (including defaults if required)
	if isBehaviour {
		bhvr.SetupDefaults()
	}
	for _, fnc := range astObj.Functions {

		method, err := self.buildMethod(fnc)
		if err != nil {
			return nil, err
		}
		obj.AddMethod(*fnc.Name, method)
	}
	obj.SetupJSMethods(self.jsvm, jsobj)

	//create our local version of the recursive behaviour map to allow adding values for children
	localRecBehaviours := make([]*astObject, len(recBehaviours))
	copy(localRecBehaviours, recBehaviours)

	//go on with all subobjects (if not behaviour)
	if !isBehaviour {

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
			child, err := self.buildObject(astChild, obj.Id(), localRecBehaviours)
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
			err := self.assignProperty(assign, assignObj.GetProperty(parts[0]), localRecBehaviours)
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

	//ant last we add the general object properties: parent etc...
	if parent.valid() {
		jsobj.DefineDataProperty("parent", self.objects[parent].GetJSObject(), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	} else {
		jsobj.DefineDataProperty("parent", self.jsvm.ToValue(nil), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	}

	return obj, nil
}

func (self *Runtime) assignProperty(asgn *astAssignment, prop Property, recBehaviours []*astObject) error {

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
		//type property is special. Due to recursive behaviours the type depends on the
		//parent in which it is defined. Hence to get a full type definition we need
		//to add the recursive behaviours that are defined for the parent to the type definition
		astObj := asgn.Value.Type
		for _, bvr := range recBehaviours {
			addFrontIfNotAvailable(&astObj.Objects, bvr)
		}
		prop.SetValue(astObj)
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
		args[i] = stringToType(ptype.Type)
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

	var dt DataType
	var defaultVal interface{}
	switch astProp.Type.Type {
	case "string":
		dt = String
		defaultVal = string("")
	case "int":
		dt = Int
		defaultVal = int64(0)
	case "float":
		dt = Float
		defaultVal = float64(0.0)
	case "bool":
		dt = Bool
		defaultVal = bool(false)
	case "type":
		dt = Type
		defaultVal = "int"
	}

	var constprop bool = false
	if astProp.Const != "" {
		constprop = true
	}

	if astProp.Default == nil {
		if constprop {
			return fmt.Errorf("Constant property needs to have a value assigned")
		}

		err := obj.AddProperty(astProp.Key, dt, defaultVal, constprop)
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
		err = obj.AddProperty(astProp.Key, dt, astProp.Default.Type, constprop)
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
