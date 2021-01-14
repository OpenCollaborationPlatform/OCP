package utils

import (
	"fmt"
	"strings"
	"sync"
)

var Decoder = newDecoder()

//A interface for types that could be decoded into string, and hence be used for marshalling
//The output of the Encode function has to has a defined prefix:
//ocp_**_ where ** is a arbitrary type specific string. This type identifier is
//used by the Decoder to recognize the type. "_" is not allowed in the data string
type Encotable interface {
	Encode() string
}

type DecotableFunc = func(string) (interface{}, error)

type decoder struct {
	decoders map[string]DecotableFunc
	mutex    sync.RWMutex
}

func newDecoder() decoder {
	return decoder{
		decoders: make(map[string]DecotableFunc, 0),
		mutex:    sync.RWMutex{},
	}
}

func (self decoder) RegisterEncotable(prefix string, fnc DecotableFunc) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.decoders[prefix] = fnc
}

func (self decoder) IsEncoded(data string) bool {
	return strings.HasPrefix(data, "ocp_")
}

func (self decoder) InterfaceIsEncoded(data interface{}) bool {

	if str, ok := data.(string); ok {
		return self.IsEncoded(str)
	}
	return false
}

func (self decoder) Decode(data string) (interface{}, error) {

	elements := strings.Split(data, "_")
	if elements[0] != "ocp" {
		return nil, fmt.Errorf("Data is not a valid encoded ocp type")
	}

	self.mutex.RLock()
	fnc, ok := self.decoders[elements[1]]
	self.mutex.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Data is not a valid encoded type")
	}
	return fnc(elements[2])
}

func (self decoder) DecodeInterface(data interface{}) (interface{}, error) {

	if str, ok := data.(string); ok {
		return self.Decode(str)
	}
	return nil, fmt.Errorf("Interface is not a encoded string")
}
