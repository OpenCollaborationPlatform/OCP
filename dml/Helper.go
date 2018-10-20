package dml

import (
	"fmt"

	"github.com/dop251/goja"
)

//should be implemented by everythign that is exposed to JS
type JSObject interface {
	GetJSObject() *goja.Object
	GetJSRuntime() *goja.Runtime
}

//user type to store data about a user
type User string

type DataType int

const (
	String DataType = 1
	Int    DataType = 2
	Float  DataType = 3
	Bool   DataType = 4
	File   DataType = 5
	Raw    DataType = 6
)

func typeToString(t DataType) string {

	switch t {
	case String:
		return "string"
	case Int:
		return "int"
	case Float:
		return "float"
	case Bool:
		return "bool"
	case File:
		return "file"
	case Raw:
		return "raw"
	}
	return ""
}

func stringToType(t string) DataType {

	switch t {
	case "string":
		return String
	case "int":
		return Int
	case "float":
		return Float
	case "bool":
		return Bool
	case "file":
		return File
	case "raw":
		return Raw
	}
	return Int
}

func mustBeType(pt DataType, val interface{}) error {
	//check if the type is correct
	switch val.(type) {
	case int, int32, int64:
		if pt != Int {
			return fmt.Errorf(`wrong type, got 'int' and expected '%s'`, typeToString(pt))
		}
	case float32, float64:
		if pt != Float {
			return fmt.Errorf(`wrong type, got 'float' and expected '%s'`, typeToString(pt))
		}
	case string:
		if pt != String {
			return fmt.Errorf(`wrong type, got 'string' and expected '%s'`, typeToString(pt))
		}
	case bool:
		if pt != Bool {
			return fmt.Errorf(`wrong type, got 'bool' and expected '%s'`, typeToString(pt))
		}
	default:
		return fmt.Errorf("Unknown type: %T", val)
	}
	return nil
}
