package dml

import (
	"encoding/json"
	"fmt"
	"github.com/ickby/CollaborationNode/utils"
)


//changes value to its main type from multiple subtypes, e.g. int64 from int and int16
//Note: No type checking is done!
func UnifyDataType(val interface{}) interface{} {

	switch val.(type) {
	case int:
		return int64(val.(int))
	case int8:
		return int64(val.(int8))
	case int16:
		return int64(val.(int16))
	case int32:
		return int64(val.(int16))
	case uint:
		return int64(val.(uint))
	case uint8:
		return int64(val.(uint8))
	case uint16:
		return int64(val.(uint16))
	case uint32:
		return int64(val.(uint32))
	case uint64:
		return int64(val.(uint64))
	case float32:
		return int64(val.(float32))
	case Boolean:
		return bool(val.(bool))
	}
	
	//everything else is correct
	return val
}

/*	DataType: a object which holds all available datatypes in DML. Those include:
 *
 *	POD:
 * 		- The usual supported datatypes of every language
 * 		- int, float, bool, string
 *
 *	Type:
 * 		- This describes the DataType itself. So a DataType can be a DataType.
 *		- Type is hence a superset of all types
 *		- Keyword is "type"
 * 		- This is used for properties:  property type MyType: Data{}
 *
 * 	Complex:
 *		- The complex composed DML datatypes
 *		- Anything the user create in DML is a DataType "complex"
 *		- e.g. Data{ Data{} } is a complex datatype
 *
 *	None:
 *		- Describing nothing. Can be used as DataType, but does not reflect any
 * 		  type
 * 		- used for properties: property type MyType: none
 */

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
			data, err := json.Marshal(ast.Object)
			if err != nil {
				return DataType{}, utils.StackError(err, "Unable to marshal AST type representation into DataType")
			}
			result = DataType{string(data)}

		} else {
			result = DataType{ast.Pod}
		}

	case *astObject:

		ast := val.(*astObject)
		data, err := json.Marshal(ast)
		if err != nil {
			return DataType{}, utils.StackError(err, "Unable to marshal AST type representation into DataType")
		}
		result = DataType{string(data)}
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

	//as every else strange string is interpretet as complex datatype and we cannot
	//easily check it is a valid complex, hence we only check if string is empty
	return self.value != ""

}

func (self DataType) IsEqual(dt DataType) bool {
	return self.value == dt.value
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

	//nil is special
	if val == nil {
		if self.IsNone() {
			return nil
		}
		return fmt.Errorf("wrong object type, got '%T' and expected 'nil'", val)
	}

	//check if the type is correct
	switch UnifyDataType(val).(type) {
	case int64:
		if !self.IsInt() && !self.IsFloat() {
			return fmt.Errorf(`wrong type, got 'int' and expected '%s'`, self.AsString())
		}
	case float64:
		if !self.IsFloat() {
			return fmt.Errorf(`wrong type, got 'float' and expected '%s'`, self.AsString())
		}
	case string:
		if !self.IsString() {
			return fmt.Errorf(`wrong type, got 'string' and expected '%s'`, self.AsString())
		}
	case bool:
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

func (self DataType) complexAsAst() (*astObject, error) {

	if !self.IsComplex() {
		return nil, fmt.Errorf("DataType is not complex, convertion into AST not possible")
	}

	var astObj *astObject
	err := json.Unmarshal([]byte(self.value), &astObj)
	if err != nil {
		return nil, utils.StackError(err, "Passed string is not a valid type desciption: unable to unmarshal")
	}
	return astObj, nil
}

func (self DataType) IsNone() bool   { return self.value == "none" }
func (self DataType) IsString() bool { return self.value == "string" }
func (self DataType) IsInt() bool    { return self.value == "int" }
func (self DataType) IsFloat() bool  { return self.value == "float" }
func (self DataType) IsBool() bool   { return self.value == "bool" }
func (self DataType) IsType() bool   { return self.value == "type" }
func (self DataType) IsComplex() bool {
	return !self.IsPOD() &&
		!self.IsNone() &&
		!self.IsType()
}

func (self DataType) GetDefaultValue() interface{} {

	switch self.value {
	case "string":
		return string("")
	case "int":
		return int64(0)
	case "float":
		return float64(0.0)
	case "bool":
		return bool(false)
	case "type":
		return MustNewDataType("none")
	}

	return nil
}
