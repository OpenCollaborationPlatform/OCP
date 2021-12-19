package dml

import (
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/mr-tron/base58"

	"github.com/OpenCollaborationPlatform/OCP/utils"
)

func init() {
	gob.Register(new(DataType))
	utils.Decoder.RegisterEncotable("dt", DataTypeDecode)
}

//changes Value to its main type from multiple subtypes, e.g. int64 from int and int16
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
		return float64(val.(float32))
	case Boolean:
		return bool(val.(bool))
	case *utils.Cid:
		id := val.(*utils.Cid)
		return *id
	case *DataType:
		dt := val.(*DataType)
		return *dt
	case *Key:
		key := val.(*Key)
		return *key
	}

	//everything else is correct
	return val
}

/* +extract target:dmltypes

Datatypes
---------
DataTypes in the dml language are a mix of standart POD types and some special dml
ones required for the functionality of distributed datastructures.

.. note:: The dml types are used for storage, in JS and via WAMP api. This includes
		  serialization, and recreation in of data in your language of choice. Therefore some
		  difference exists on how the types work when storing data, accessing from
		  JavaScript or the WAMP Api.

As a language for datastorage, dml can store and handle all JSON serializable datatypes. But not all
of those possibilities are exposed as datatypes. This comes from the fact, that the concrete types are intended
for use in defining configuration properties of Objects and Behaviours, which are to be set by the user. This process
is simplified if the type of those properties is clear to the user, and error checking can be done on
his input. For data storage use with your application you most likely want to use the the :dml:type:`var` type,
as it is more versataile.

.. note:: The dml type system was created on the fly as needed. Do not expect a perfectly reasonable and always consistent
		  language architecture around them.

Plain old datatypes
^^^^^^^^^^^^^^^^^^^

.. dml:type:: int

	Integer datatype with 64bit size. All other integer sizes are converted to
	64 bit internally. Larger integers are not supported. In JS, and hence JSON
	searialization, this is converted to 'number' type

.. dml:type:: float

	Floating point number with 64bit size. All other floating point sizes are
	converted to 64 bit

.. dml:type:: bool

	Boolean datatype, can be true or false.In JS, and hence JSON
	searialization, this is converted to 'boolean' type

.. dml:type:: string

	String datatype. Same in JA and JSON.

Special dml types
^^^^^^^^^^^^^^^^^

 .. dml:type:: var

	Variable datatype, can be anything. Note that it can hold any DML type, but even
	more. It can hold anything that is JSON searializable, like Lists or Maps. The purpose
	of this type is to enable maximal flexible datastorage for the user. This type is
	what you want to use for most of your storage needs.

	.. note:: There is no performance benefit or penaulty for using discrete types over var.

.. dml:type:: key

	Key describes a subset of all dml types, describing those that can be used as a key.
  	This includes :dml:type:`string`, :dml:type:`int`, :dml:type:`bool`, and :dml:type:`raw`.
	This type is used by datatypes that have user definable keys like :dml:prop:`Map.key`

.. dml:type:: raw

	This datatype describes raw binary data. It is exposed as CID in a property raw.
	The CID itself does not have any methods or useful interaction options, but can be
	used together with the WAMP api to retrieve the raw data.

DML types for type handling
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 .. dml:type:: type:

	This type describes all types. Allowed values are all possible types as defined
	in dml, e.g. string, int, raw. It is used in Objects to specify the types used for
	special functionality, like the value of a Map.

 	DML allows to create custom types as composites from objects. That means that any
	valid dml Description like Data{} can be used as a new type, and is a valid argument
	for 'type' type. With this it is possible to use composite types e.g. as value for a
	Map.

	When accessing in JS or serializing the string representation of the datatype is
	provided. This is the type name for most types, and a encoded string in case of
	a composite type. Those strings can also be used to set a type proeprty via WAMP api.

.. dml:type:: none

	A none type, not meaning anything. It is the type version of JS undefined. The use
	case for this is to be able to not specify anything for type properties if not wanted.
	Some Objects may use this possibility to disable functionality.
*/

/*	DataType: a object which holds all available datatypes in DML. Those include:
 *
 *	POD:
 * 		- The usual supported datatypes of every language
 * 		- int, float, bool, string
 *
 * 	Raw:
 *  		- This behaves like a POD type and exposes a CID
 * 		- It can not realy do anything except convey the information
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
 *
 *  Var:
 * 		- Variable, can be any of the above datatypes
 *      - Is allowed to change at runtime if not const
 *
 *  Key:
 *		- A subset of "Type", describing all types that can be used as key
 * 		- Supported: String, Int, Bool, Cid
 *      - Used by datatypes that have user definable keys like maps: property key accessor: string
 */

//a datatype can be either a pod type or any complex dml object
type DataType struct {
	Value string
}

func NewDataType(val interface{}) (DataType, error) {

	var result DataType
	switch val.(type) {

	case string:
		result = DataType{val.(string)}
		if !result.IsValid() {
			return DataType{}, newUserError(Error_Type, "Provided string is not valid DataType")
		}

	case DataType:
		result = val.(DataType)

	case *astDataType:

		ast := val.(*astDataType)
		if ast.Object != nil {
			data, err := json.Marshal(ast.Object)
			if err != nil {
				return DataType{}, wrapInternalError(err, Error_Fatal)
			}
			result = DataType{string(data)}

		} else {
			result = DataType{ast.Pod}
		}

	case *astObject:

		ast := val.(*astObject)
		data, err := json.Marshal(ast)
		if err != nil {
			return DataType{}, wrapInternalError(err, Error_Fatal)
		}
		result = DataType{string(data)}
	}

	return result, nil
}

func DataTypeDecode(code string) (interface{}, error) {

	data, err := base58.Decode(code)
	if err != nil {
		return nil, wrapInternalError(err, Error_Fatal).(utils.OCPError)
	}
	return DataType{string(data)}, nil
}

func MustNewDataType(val interface{}) DataType {
	res, err := NewDataType(val)
	if err != nil {
		return DataType{"var"}
	}
	return res
}

func (self DataType) IsValid() bool {

	//as every else strange string is interpretet as complex datatype and we cannot
	//easily check it is a valid complex, hence we only check if string is empty
	return self.Value != ""

}

func (self DataType) IsEqual(dt DataType) bool {
	return self.Value == dt.Value
}

func (self DataType) AsString() string {
	return self.Value
}

func (self DataType) IsPOD() bool {

	//check if the type is correct
	switch self.Value {
	case "string", "float", "int", "bool":
		return true
	}
	return false
}

func (self DataType) MustBeTypeOf(val interface{}) error {

	//var is special: we accept everything
	if self.IsVar() {
		return nil
	}

	//nil is special: cannot be handled by switc
	if val == nil {
		if self.IsNone() {
			return nil
		}
		return newUserError(Error_Type, fmt.Sprintf("wrong object type, got '%T' and expected 'nil'", val))
	}

	//check if the type is correct
	switch UnifyDataType(val).(type) {
	case int64:
		if !self.IsInt() && !self.IsFloat() && !self.IsKey() {
			return newUserError(Error_Type, fmt.Sprintf(`wrong type, got 'int' and expected '%s'`, self.AsString()))
		}
	case float64:
		if !self.IsFloat() {
			return newUserError(Error_Type, fmt.Sprintf(`wrong type, got 'float' and expected '%s'`, self.AsString()))
		}
	case string:
		if !self.IsString() && !self.IsKey() {
			return newUserError(Error_Type, fmt.Sprintf(`wrong type, got 'string' and expected '%s'`, self.AsString()))
		}
	case bool:
		if !self.IsBool() && !self.IsKey() {
			return newUserError(Error_Type, fmt.Sprintf(`wrong type, got 'bool' and expected '%s'`, self.AsString()))
		}
	case DataType:
		if self.IsKey() {
			arg := UnifyDataType(val).(DataType)
			if !arg.IsString() && !arg.IsBool() && !arg.IsInt() && !arg.IsRaw() {
				return newUserError(Error_Type, fmt.Sprintf("wrong type, got %v, but expected a supported key datatype (int, string, bool, raw)", arg.AsString()))
			}
			return nil
		}
		if !self.IsType() {
			return newUserError(Error_Type, fmt.Sprintf(`wrong type, got 'type' and expected '%s'`, self.AsString()))
		}
	case utils.Cid:
		if !self.IsRaw() && !self.IsKey() {
			return newUserError(Error_Type, fmt.Sprintf(`wrong type, got 'raw' and expected '%s'`, self.AsString()))
		}
	default:
		return newUserError(Error_Type, fmt.Sprintf("Unknown type: %T", val))
	}

	return nil
}

func (self DataType) complexAsAst() (*astObject, error) {

	if !self.IsComplex() {
		return nil, newInternalError(Error_Operation_Invalid, "DataType is not complex, convertion into AST not possible")
	}

	var astObj *astObject
	err := json.Unmarshal([]byte(self.Value), &astObj)
	if err != nil {
		return nil, wrapInternalError(err, Error_Fatal)
	}
	return astObj, nil
}

func (self DataType) IsNone() bool   { return self.Value == "none" }
func (self DataType) IsString() bool { return self.Value == "string" }
func (self DataType) IsInt() bool    { return self.Value == "int" }
func (self DataType) IsFloat() bool  { return self.Value == "float" }
func (self DataType) IsBool() bool   { return self.Value == "bool" }
func (self DataType) IsType() bool   { return self.Value == "type" }
func (self DataType) IsRaw() bool    { return self.Value == "raw" }
func (self DataType) IsVar() bool    { return self.Value == "var" }
func (self DataType) IsKey() bool    { return self.Value == "key" }
func (self DataType) IsComplex() bool {
	return !self.IsPOD() &&
		!self.IsNone() &&
		!self.IsRaw() &&
		!self.IsType() &&
		!self.IsVar() &&
		!self.IsKey()
}

func (self DataType) GetDefaultValue() interface{} {

	switch self.Value {
	case "string":
		return string("")
	case "int":
		return int64(0)
	case "float":
		return float64(0.0)
	case "bool":
		return bool(false)
	case "raw":
		return utils.CidUndef
	case "type":
		return MustNewDataType("none")
	case "key":
		return MustNewDataType("none")
	}

	//none and var return nil as default
	return nil
}

func (self DataType) Encode() string {
	return "ocp_dt_" + base58.Encode([]byte(self.Value))
}
