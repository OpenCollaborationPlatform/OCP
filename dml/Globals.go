package dml

import (
	"github.com/ickby/CollaborationNode/utils"
	"fmt"

	"github.com/dop251/goja"
	"github.com/alecthomas/participle"
	uuid "github.com/satori/go.uuid"
)

func SetupGlobals(rntm *Runtime) {

	//constructor for DML object
	rntm.jsvm.Set("Object", func(call goja.ConstructorCall) *goja.Object {

		//get the type description, which must be passed as argument
		if len(call.Arguments) != 1 {
			panic("Wrong arguments: DataType only must be passed")
		}
		typeArg := call.Arguments[0].Export()
		datatype, ok := typeArg.(DataType)
		if !ok {
			panic(rntm.jsvm.ToValue("A valid type description must be given as argument"))
		}

		obj, err := ConstructObject(rntm, datatype, "")
		if err != nil {
			panic(rntm.jsvm.ToValue(utils.StackError(err, "Unable to build object from type desciption").Error()))
		}
		return obj.GetJSObject()
	})
	
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

}

//Construct a data object from encoded description (as provided by type property)
//Note that if no name is given a random name is generated to ensure uniqueness. If
//the object shall be restored instead provide the name to restore
func ConstructObject(rntm *Runtime, dt DataType, name string) (Object, error) {

	if !dt.IsComplex() {
		return nil, fmt.Errorf("Not a complex datatype which can be build into Object")
	}

	astObj, err := dt.complexAsAst()
	if err != nil {
		return nil, utils.StackError(err, "Unable to build object from type description")
	}

	//set a uuid to ensure unique identifier. Important if there is no parent!
	uid := uuid.NewV4().String()

	//build the object (without parent, but with uuid)
	obj, err := rntm.buildObject(astObj, Identifier{}, uid, make([]*astObject, 0))

	if err != nil {
		return nil, utils.StackError(err, "Unable to create subobject")
	}

	return obj, nil
}
