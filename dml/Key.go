package dml

import (
	"fmt"
	"strconv"

	"github.com/OpenCollaborationPlatform/OCP/utils"
)

type Key struct {
	data interface{}
}

func NewKey(data interface{}) (Key, error) {

	dt := DataType{"key"}
	if dt.MustBeTypeOf(data) != nil {
		return Key{}, newInternalError(Error_Arguments_Wrong, "Provided data cannot be used as key", "Data", data)
	}
	return Key{UnifyDataType(data)}, nil
}

func MustNewKey(data interface{}) Key {
	key, err := NewKey(data)
	if err != nil {
		panic(err.Error())
	}
	return key
}

func (self Key) AsString() string {
	return fmt.Sprintf("%v", self.data)
}

func (self Key) Equal(second Key) bool {
	return self.AsString() == second.AsString()
}

func (self Key) Data() interface{} {
	return self.data
}

func (self Key) AsDataType(dt DataType) (interface{}, error) {

	//maybe we are already the correct datatype
	if dt.MustBeTypeOf(self.data) == nil {
		return self.data, nil
	}

	if dt.IsString() {
		return self.AsString(), nil
	}

	if dt.IsInt() {
		i, err := strconv.ParseInt(self.AsString(), 10, 64)
		if err != nil {
			return -1, newUserError(Error_Operation_Invalid, "Key cannot be used as integer", "Key", self.AsString())
		}
		return i, nil
	}

	if dt.IsBool() {
		if self.AsString() != "true" && self.AsString() != "false" {
			return nil, newUserError(Error_Operation_Invalid, "Key cannot be used as boolean", "Key", self.AsString())
		}
		return self.AsString() == "true", nil
	}

	if dt.IsRaw() {
		raw, ok := self.data.(utils.Cid)
		if !ok {
			return nil, newUserError(Error_Operation_Invalid, "Key cannot be used as raw", "Key", self.AsString())
		}
		return raw, nil
	}

	return nil, newUserError(Error_Operation_Invalid, "Key cannot be used as provided datatype", "Key", self.AsString(), "DataType", dt.AsString())
}
