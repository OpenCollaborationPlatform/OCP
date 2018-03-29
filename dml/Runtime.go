package dml

import (
	"CollaborationNode/datastores"
	"fmt"
	"os"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

type CreatorFunc func(datastore *datastore.Datastore, name string) Object

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
	//bufferedReader := bufio.NewReader(filereader)
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
		if astAssign.Key == "id" {
			objName = *astAssign.Value.String
		}
	}
	if objName == "" {
		panic("we need the ID proeprty, otherwise everything falls appart...")
	}

	//setup the object including datastore and expose it to js
	obj := creator(self.datastore, objName)
	jsobj := self.jsvm.NewObject()
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
	/*	for _, evt := range astObj.Events {

		}

		//than all functions
		for _, fnc := range astObj.Functions {

		}
	*/
	//go on with all subobjects
	for _, astChild := range astObj.Objects {
		child, err := self.buildObject(astChild)
		if err != nil {
			return nil, err
		}
		obj.AddChild(astChild.Identifier, child)
	}

	//with everything in place we are able to process the assignments
	/*	for _, assign := range astObj.Assignments {

		}*/

	return obj, nil
}

func (self *Runtime) buildProperty(ds datastore.Store, astProp *astProperty) (Property, error) {

	var dt PropertyType
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

	prop, err := NewProperty(astProp.Key, dt, ds)
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
		err = prop.SetValue(*astProp.Default.Bool)
	}

	if err != nil {
		return nil, err
	}

	return prop, nil
}
