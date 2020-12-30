package dml

import (
	//	"bytes"
	//	"fmt"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/alecthomas/participle"
	"github.com/dop251/goja"
	//	uuid "github.com/satori/go.uuid"
)

func SetupGlobals(rntm *Runtime) {

	//constructor for DML data type
	rntm.jsvm.Set("DataType", func(call goja.ConstructorCall) *goja.Object {

		//get the type description, which must be passed as argument
		if len(call.Arguments) != 1 {
			panic("Wrong arguments: Only type description must be passed")
		}
		typeArg := call.Arguments[0].Export()
		typestr, ok := typeArg.(string)
		if !ok {
			panic(rntm.jsvm.ToValue("A valid type description must be given as argument"))
		}

		var dt DataType
		switch typestr {
		case "int", "float", "string", "bool", "type", "object":
			dt, _ = NewDataType(typestr)
		default:
			ast := &DML{}
			parser, err := participle.Build(&DML{}, participle.Lexer(&dmlDefinition{}))
			if err != nil {
				panic(utils.StackError(err, "Unable to setup dml parser").Error())
			}

			err = parser.ParseString(typestr, ast)
			if err != nil {
				panic(utils.StackError(err, "Unable to parse dml code").Error())
			}
			dt, err = NewDataType(ast.Object)
			if err != nil {
				panic(utils.StackError(err, "Unable to create DataType from DML code").Error())
			}
		}

		return rntm.jsvm.ToValue(dt).(*goja.Object)
	})

	rntm.jsvm.Set("print", func(call goja.FunctionCall) goja.Value {

		if len(call.Arguments) != 1 {
			panic("Print takes only single string argument")
		}

		res := call.Arguments[0].Export()
		str, ok := res.(string)

		if !ok {
			panic("Print takes only single string argument")
		}

		rntm.printMessage(str)
		return nil
	})

}

/*
//Construct a data object from encoded description (as provided by type property)
//and a given identifier. Note that it does not setup behaviours, that must be done by
//the caller. Same as Constructed()
func LoadObject(rntm *Runtime, dt DataType, id Identifier, parent Identifier) (Object, error) {

	if !dt.IsComplex() {
		return nil, fmt.Errorf("Not a complex datatype which can be build into Object")
	}

	ph := parent.Hash()
	if !bytes.Equal(id.Parent[:], ph[:]) {
		return nil, fmt.Errorf("Cannot load object: Parent it not the one used in identifier")
	}

	astObj, err := dt.complexAsAst()
	if err != nil {
		return nil, utils.StackError(err, "Unable to build object from type description")
	}

	//build the object (without parent, but with uuid)
	obj, err := rntm.buildObject(astObj, parent, id.Uuid)
	//no behaviour setup, LoadObject is only called out of buildObject

	if err != nil {
		return nil, utils.StackError(err, "Unable to create subobject")
	}

	if !obj.Id().Equal(id) {
		return nil, fmt.Errorf("Loaded object is faulty: Identifiers do not match (%v vs %v)", obj.Id(), id)
	}

	return obj, nil
}
*/
