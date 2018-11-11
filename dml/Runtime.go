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
	"CollaborationNode/datastores"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

//Function prototype that can create new object types in DML
type CreatorFunc func(datastore *datastore.Datastore, name string, parent Object, vm *goja.Runtime) Object

//struct that describes the runtime state and is shared between all runtime objects
type RuntimeState struct {
	Ready       Boolean //True if a datastructure was read and setup, false if no dml file was parsed
	CurrentUser User    //user that currently access the runtime
}

func NewRuntime(ds *datastore.Datastore) Runtime {

	//js runtime with console support
	js := goja.New()
	new(require.Registry).Enable(js)
	console.Enable(js)

	cr := make(map[string]CreatorFunc, 0)
	return Runtime{
		creators:  cr,
		jsvm:      js,
		datastore: ds,
		mainObj:   nil,
		mutex:     &sync.Mutex{},
		state:     &RuntimeState{Ready: false, CurrentUser: "none"},
	}
}

//builds a datastructure from a file
// - existing types must be registered to be recognized during parsing
type Runtime struct {
	creators  map[string]CreatorFunc
	jsvm      *goja.Runtime
	datastore *datastore.Datastore
	mainObj   Object
	mutex     *sync.Mutex
	state     *RuntimeState
}

// Setup / creation Methods
// ************************

//Function to extend the available data and behaviour types for this runtime
func (self *Runtime) RegisterObjectCreator(name string, fnc CreatorFunc) error {

	_, ok := self.creators[name]
	if ok {
		return fmt.Errorf("Object name already registered")
	}

	self.creators[name] = fnc
	return nil
}

//Parses the dml file and setups the full structure
func (self *Runtime) ParseFile(path string) error {

	//no double loading
	if self.state.Ready == true {
		return fmt.Errorf("Runtime is already setup")
	}

	//we start with building the AST
	ast := &DML{}
	parser, err := participle.Build(&DML{}, &dmlDefinition{})
	if err != nil {
		return err
	}

	//read in the file and parse
	filereader, err := os.Open(path)
	if err != nil {
		return err
	}
	err = parser.Parse(filereader, ast)
	if err != nil {
		return err
	}

	//process the AST into usable objects
	obj, err := self.buildObject(ast.Object, nil)
	if err != nil {
		return err
		//TODO clear the database entries...
	}
	self.mainObj = obj
	self.state.Ready = true

	return err
}

// 						Accessing / executing Methods
//*********************************************************************************

//run arbitrary javascript code on the loaded structure
func (self *Runtime) RunJavaScript(code string) (interface{}, error) {

	val, err := self.jsvm.RunString(code)

	if err != nil {
		return nil, err
	}
	return val.Export(), nil
}

func (self *Runtime) GetObject() (Object, error) {
	return self.mainObj, nil
}

// 							Internal Functions
//*********************************************************************************

//due to recursive nature og objects we need an extra function
func (self *Runtime) buildObject(astObj *astObject, parent Object) (Object, error) {

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
	obj := creator(self.datastore, objName, parent, self.jsvm)
	if parent != nil {
		objId := obj.Id()
		//if unique in the parents childrens we are generaly unique, as parent is part of our ID
		for _, sibling := range parent.GetChildren() {
			if sibling.Id().Type == objId.Type &&
				sibling.Id().Name == objId.Name {

				return nil, fmt.Errorf("Object with same type (%s) and ID (%s) already exists", objId.Type, objId.Name)
			}
		}
	}

	//expose to javascript
	jsobj := obj.GetJSObject()
	if parent != nil {
		parent.GetJSObject().Set(objName, jsobj)
	} else {
		self.jsvm.Set(objName, jsobj)
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

		event, err := self.buildEvent(astEvent)
		if err != nil {
			return nil, err
		}
		err = obj.AddEvent(astEvent.Key, event)
		if err != nil {
			return nil, err
		}
		jsobj.Set(astEvent.Key, obj.GetEvent(astEvent.Key).GetJSObject())
	}

	//than all methods
	for _, fnc := range astObj.Functions {

		method, err := self.buildMethod(fnc)
		if err != nil {
			return nil, err
		}
		obj.AddMethod(*fnc.Name, method)
	}
	obj.SetupJSMethods(self.jsvm, jsobj)

	//go on with all subobjects
	jsChildren := make([]goja.Value, 0)
	for _, astChild := range astObj.Objects {
		child, err := self.buildObject(astChild, obj)
		if err != nil {
			return nil, err
		}
		obj.AddChild(child)
		jsChildren = append(jsChildren, child.GetJSObject())
	}

	//with everything in place we are able to process the assignments
	for _, assign := range astObj.Assignments {

		parts := assign.Key
		assign.Key = parts[1:]
		if obj.HasProperty(parts[0]) {
			err := self.assignProperty(assign, obj.GetProperty(parts[0]))
			if err != nil {
				return nil, err
			}
		} else if obj.HasEvent(parts[0]) {
			err := self.assignEvent(assign, obj.GetEvent(parts[0]))
			if err != nil {
				return nil, err
			}

		} else {
			return nil, fmt.Errorf("Key of assignment is not a property or event")
		}
	}

	//ant last we add the general object properties: children, parent etc...
	jsobj.DefineDataProperty("children", self.jsvm.ToValue(jsChildren), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	if parent != nil {
		jsobj.DefineDataProperty("parent", parent.GetJSObject(), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	} else {
		jsobj.DefineDataProperty("parent", self.jsvm.ToValue(nil), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	}

	return obj, nil
}

func (self *Runtime) assignProperty(asgn *astAssignment, prop Property) error {

	parts := asgn.Key
	if len(parts) > 0 && parts[0] != "" {
		asgn.Key = parts[1:]
		if prop.HasEvent(parts[0]) {
			return self.assignEvent(asgn, prop.GetEvent(parts[0]))
		} else {
			return fmt.Errorf("Property does not provide event with key %s", parts[0])
		}
	}

	//we found the right property, lets assign
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
		prop.SetValue(*asgn.Value.Bool)
	}
	return nil
}

func (self *Runtime) assignEvent(asgn *astAssignment, evt Event) error {

	//the key must end here
	parts := asgn.Key
	if len(parts) != 0 {
		return fmt.Errorf("Event does not provide given key")
	}
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

func (self *Runtime) buildEvent(astEvt *astEvent) (Event, error) {

	//build the arguements slice
	args := make([]DataType, len(astEvt.Params))
	for i, ptype := range astEvt.Params {
		args[i] = stringToType(ptype.Type)
	}

	//build the event
	evt := NewEvent(self.jsvm, args...)

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
	switch astProp.Type.Type {
	case "string":
		dt = String
	case "int":
		dt = Int
	case "float":
		dt = Float
	case "bool":
		dt = Bool
	}

	var constprop bool = false
	if astProp.Const != "" {
		constprop = true
	}

	err := obj.AddProperty(astProp.Key, dt, constprop)
	if err != nil {
		return err
	}

	if astProp.Default == nil {
		if constprop {
			return fmt.Errorf("Constant property needs to have a value assigned")
		}
		return nil
	}

	if astProp.Default.String != nil {
		err = obj.GetProperty(astProp.Key).SetValue(*astProp.Default.String)

	} else if astProp.Default.Int != nil {
		err = obj.GetProperty(astProp.Key).SetValue(*astProp.Default.Int)

	} else if astProp.Default.Number != nil {
		err = obj.GetProperty(astProp.Key).SetValue(*astProp.Default.Number)

	} else if astProp.Default.Bool != nil {
		err = obj.GetProperty(astProp.Key).SetValue(bool(*astProp.Default.Bool))
	}

	if err != nil {
		return err
	}

	return nil
}
