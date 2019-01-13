package dml

import (
	"CollaborationNode/utils"
	"encoding/json"
	"fmt"

	"github.com/mr-tron/base58/base58"
)

//a datatype can be either a pod type or any complex dml object
type DataType struct {
	value string
}

func NewDataType(val interface{}) (DataType, error) {

	var result DataType
	switch val.(type) {

	case string:
		result = DataType{val.(string)}
		if !result.IsValid() {
			return DataType{}, fmt.Errorf("Provided string is not valid DataType")
		}

	case DataType:
		result = val.(DataType)

	case *astDataType:

		ast := val.(*astDataType)
		if ast.Object != nil {
			data, err := json.Marshal(val)
			if err != nil {
				return DataType{}, utils.StackError(err, "Unable to marshal AST type representation into DataType")
			}
			result = DataType{base58.Encode(data)}

		} else {
			result = DataType{ast.Pod}
		}
	}

	return result, nil
}

func MustNewDataType(val interface{}) DataType {
	res, err := NewDataType(val)
	if err != nil {
		panic(err.Error())
	}
	return res
}

func (self DataType) IsValid() bool {

	//first check if it is a plain type
	return true
}

func (self DataType) AsString() string {
	return self.value
}

func (self DataType) IsPOD() bool {

	//check if the type is correct
	switch self.value {
	case "string", "float", "int", "bool":
		return true
	}
	return false
}

func (self DataType) MustBeTypeOf(val interface{}) error {

	//check first if it is a object (as this includes many subtypes switch does not work)
	obj, ok := val.(Object)
	if ok {
		//a object as value is ok if we store all objects
		if self.IsObject() {
			return nil
		}

		//or if the object is exactly the complex type we store
		if self.IsComplex() && obj.DataType().value == self.value {
			return nil
		}
		return fmt.Errorf(`wrong type, got 'object' and expected '%s'`, self.AsString())
	}

	//check if the type is correct
	switch val.(type) {
	case int, int32, int64:
		if !self.IsInt() {
			return fmt.Errorf(`wrong type, got 'int' and expected '%s'`, self.AsString())
		}
	case float32, float64:
		if !self.IsFloat() {
			return fmt.Errorf(`wrong type, got 'float' and expected '%s'`, self.AsString())
		}
	case string:
		if !self.IsString() {
			return fmt.Errorf(`wrong type, got 'string' and expected '%s'`, self.AsString())
		}
	case bool, Boolean:
		if !self.IsBool() {
			return fmt.Errorf(`wrong type, got 'bool' and expected '%s'`, self.AsString())
		}
	case DataType:
		if !self.IsType() {
			return fmt.Errorf(`wrong type, got 'type' and expected '%s'`, self.AsString())
		}
	default:
		return fmt.Errorf("Unknown type: %T", val)
	}

	return nil

}

func (self DataType) ComplexAsAst() (*astObject, error) {
	if !self.IsComplex() {
		return nil, fmt.Errorf("DataType is not complex, convertion into AST not possible")
	}

	data, err := base58.Decode(self.value)
	if err != nil {
		return nil, utils.StackError(err, "Passed string is not a valid type description: unable to decode")
	}
	var astObj *astObject
	err = json.Unmarshal(data, &astObj)
	if err != nil {
		return nil, utils.StackError(err, "Passed string is not a valid type desciption: unable to unmarshal")
	}
	return astObj, nil
}

func (self DataType) IsString() bool { return self.value == "string" }
func (self DataType) IsInt() bool    { return self.value == "int" }
func (self DataType) IsFloat() bool  { return self.value == "float" }
func (self DataType) IsBool() bool   { return self.value == "bool" }
func (self DataType) IsObject() bool { return self.value == "object" }
func (self DataType) IsType() bool   { return self.value == "type" }
func (self DataType) IsComplex() bool {
	return !self.IsString() &&
		!self.IsInt() &&
		!self.IsFloat() &&
		!self.IsBool() &&
		!self.IsObject() &&
		!self.IsType()
}