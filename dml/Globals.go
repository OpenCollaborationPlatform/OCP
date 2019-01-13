package dml

import (
	"CollaborationNode/utils"
	"fmt"

	"github.com/dop251/goja"
	uuid "github.com/satori/go.uuid"
)

func SetupGlobals(rntm *Runtime) {

	//constructor for DML object
	rntm.jsvm.Set("Object", func(call goja.ConstructorCall) *goja.Object {

		//get the type description, which must be passed as argument
		if len(call.Arguments) != 1 {
			panic("Wrong arguments: Only type description must be passed")
		}
		typeArg := call.Arguments[0].Export()
		typeStr, ok := typeArg.(string)
		if !ok {
			panic(rntm.jsvm.ToValue("A valid type description must be given as argument"))
		}

		obj, err := ConstructObject(rntm, MustNewDataType(typeStr), "")
		if err != nil {
			panic(rntm.jsvm.ToValue(utils.StackError(err, "Unable to build object from type desciption").Error()))
		}
		return obj.GetJSObject()
	})

}

//Construct a data object from encoded description (as provided by type property)
//Note that if no name is given a random name is generated to ensure uniqueness. If
//the object shall be restored instead of brandnew provide the type
func ConstructObject(rntm *Runtime, dt DataType, name string) (Object, error) {

	if !dt.IsComplex() {
		return nil, fmt.Errorf("Not a complex datatype which can be build into Object")
	}

	astObj, err := dt.ComplexAsAst()
	if err != nil {
		return nil, utils.StackError(err, "Unable to build object from type description")
	}

	//set a uuid name to ensure unique identifier if none is set. Important if there is no parent!
	if name == "" {
		id := uuid.NewV4()
		name = id.String()
	}
	isAssigned := false
	for _, astAssign := range astObj.Assignments {
		if astAssign.Key[0] == "id" {
			*astAssign.Value.String = name
			isAssigned = true
		}
	}
	if !isAssigned {
		val := &astValue{String: &name}
		asgn := &astAssignment{Key: []string{"id"}, Value: val}
		astObj.Assignments = append(astObj.Assignments, asgn)
	}

	//check uniquiness
	tmpID := identifier{}
	tmpID.Type = astObj.Identifier
	tmpID.Name = name
	_, ok := rntm.objects[tmpID]
	if ok {
		return nil, fmt.Errorf("No unique name given: cannot create object")
	}

	//build the object (without parent)
	obj, err := rntm.buildObject(astObj, identifier{}, make([]*astObject, 0))

	if err != nil {
		return nil, utils.StackError(err, "Unable to create subobject")
	}

	return obj, nil
	return nil, nil
}

//functions
func ParseDML(code string) {

}
