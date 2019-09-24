package dml

import (
	"github.com/ickby/CollaborationNode/utils"
	"bytes"
	"crypto/sha256"
	"encoding/json"

	"github.com/mr-tron/base58/base58"
)

type identifier struct {
	Parent [32]byte
	Type   string
	Name   string
}

func IdentifierFromData(data []byte) (identifier, error) {

	var result identifier
	err := json.Unmarshal(data, &result)
	if err != nil {
		return identifier{}, utils.StackError(err, "Unable to recreate identifier from data")
	}
	return result, nil
}

func IdentifierFromEncoded(code string) (identifier, error) {

	data, err := base58.Decode(code)
	if err != nil {
		return identifier{}, utils.StackError(err, "Unable to decode strig to identifier data")
	}
	return IdentifierFromData(data)
}

func (self identifier) data() []byte {
	data, _ := json.Marshal(self)
	return data
}

func (self identifier) hash() [32]byte {
	return sha256.Sum256(self.data())
}

func (self identifier) encode() string {
	return base58.Encode(self.data())
}

func (self identifier) equal(id identifier) bool {
	return bytes.Equal(self.Parent[:], id.Parent[:]) && self.Type == id.Type && self.Name == id.Name
}

func (self identifier) valid() bool {
	return self.Type != "" && self.Name != ""
}
