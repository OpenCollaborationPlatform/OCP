package dml

import (
	"fmt"
	"os"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
)

type CreatorFunc func() Object

//builds a datastructure from a file
// - existing types must be registered to be recognized during parsing
type Runtime struct {
	creators map[string]CreatorFunc
	jsvm     *goja.Runtime
	mainObj  Object
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
		return nil, err
	}

	//read in the file and parse
	filereader, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	err = parser.Parse(filereader, ast)
	if err != nil {
		return nil, err
	}

	//process the AST into usable objects
	obj, err := self.buildObject(ast.Object)
	self.mainObj = obj
	return err
}

//due to recursive nature og objects we need an extra function
func (self *Runtime) buildObject(astObj *astObject) (Object, error) {

	//see if we can build it, and do so if possible
	creator, ok := self.creators[astObj.Identifier]
	if !ok {
		return nil, fmt.Errorf("No object type \"%s\" exists", astObj.Identifier)
	}
	obj := creator()

	//first we create all additional properties
	for _, astProp := range astObj.Properties {
		prop, err := self.buildProperty(astProp)
		if err != nil {
			return nil, err
		}
		obj.AddProperty(astProp.Key, prop)
	}

	//now we create all new events
	for _, evt := range astObj.Events {

	}

	//than all functions
	for _, fnc := range astObj.Functions {

	}

	//go on with all subobjects
	for _, astChild := range astObj.Objects {
		child, err := self.buildObject(astChild)
		if err != nil {
			return nil, err
		}
		obj.AddChild(astChild.Identifier, child)
	}

	//with everything in place we are able to process the assignments
	for _, assign := range astObj.Assignments {

	}

	return obj, nil
}

func (self *Runtime) buildProperty(astProp *astProperty) (Property, error) {

	var dt DataType
	switch astProp.Type {
	case "string":
		dt = String
	case "int":
		dt = Int
	case "float":
		dt = Float
	case "bool":
		dt = Bool
	}

	prop, err := MakeProperty(dt)
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
