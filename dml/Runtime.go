package dml

import (
	"CollaborationNode/datastores"
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

type CreatorFunc func(datastore *datastore.Datastore, name string, vm *goja.Runtime) Object

func NewRuntime(ds *datastore.Datastore) Runtime {

	//js runtime with console support
	js := goja.New()
	new(require.Registry).Enable(js)
	console.Enable(js)

	cr := make(map[string]CreatorFunc, 0)
	return Runtime{cr, js, ds, nil}
}

//builds a datastructure from a file
// - existing types must be registered to be recognized during parsing
type Runtime struct {
	creators  map[string]CreatorFunc
	jsvm      *goja.Runtime
	datastore *datastore.Datastore
	mainObj   Object
}

func (self *Runtime) RegisterObjectCreator(name string, fnc CreatorFunc) error {

	_, ok := self.creators[name]
	if ok {
		return fmt.Errorf("Object name already registered")
	}

	self.creators[name] = fnc
	return nil
}

func (self *Runtime) ParseFile(path string) error {

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
	obj, err := self.buildObject(ast.Object)
	if err != nil {
		return err
		//TODO clear the database entries...
	}
	self.mainObj = obj
	return err
}

func (self *Runtime) RunJavaScript(code string) (interface{}, error) {

	val, err := self.jsvm.RunString(code)

	if err != nil {
		return nil, err
	}
	return val.Export(), nil
}

func (self *Runtime) GetObject() Object {
	return self.mainObj
}

//due to recursive nature og objects we need an extra function
func (self *Runtime) buildObject(astObj *astObject) (Object, error) {

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

	//setup the object including datastore and expose it to js
	obj := creator(self.datastore, objName, self.jsvm)
	jsobj := obj.GetJSObject()
	self.jsvm.Set(objName, jsobj)

	//Now we create all additional properties and set them up in js
	for _, astProp := range astObj.Properties {
		prop, err := self.buildProperty(obj.GetDataStore(), astProp)
		if err != nil {
			return nil, err
		}
		obj.AddProperty(astProp.Key, prop)
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
	for _, astChild := range astObj.Objects {
		child, err := self.buildObject(astChild)
		if err != nil {
			return nil, err
		}
		obj.AddChild(child)
		child.SetParent(obj)
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
	children := obj.GetChildren()
	jsChildren := make([]goja.Value, len(children))
	for i, child := range children {
		//collect children jsvalue for later use as property
		jsChildren[i] = self.jsvm.Get(child.Id())
	}
	jsobj.DefineDataProperty("children", self.jsvm.ToValue(jsChildren), goja.FLAG_FALSE, goja.FLAG_TRUE, goja.FLAG_TRUE)

	getter := self.jsvm.ToValue(func(call goja.FunctionCall) goja.Value {
		if obj.GetParent() != nil {
			return self.jsvm.Get(obj.GetParent().Id())
		}
		return self.jsvm.ToValue(nil)
	})
	jsobj.DefineAccessorProperty("parent", getter, nil, goja.FLAG_TRUE, goja.FLAG_TRUE)

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

func (self *Runtime) buildProperty(ds datastore.Store, astProp *astProperty) (Property, error) {

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

	prop, err := NewProperty(astProp.Key, dt, ds, self.jsvm)
	if err != nil {
		return nil, err
	}

	if astProp.Default == nil {
		return prop, nil
	}

	if astProp.Default.String != nil {
		err = prop.SetValue(*astProp.Default.String)

	} else if astProp.Default.Int != nil {
		err = prop.SetValue(*astProp.Default.Int)

	} else if astProp.Default.Number != nil {
		err = prop.SetValue(*astProp.Default.Number)

	} else if astProp.Default.Bool != nil {
		err = prop.SetValue(bool(*astProp.Default.Bool))
	}

	if err != nil {
		return nil, err
	}

	return prop, nil
}
