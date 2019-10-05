package dml

import (
	"github.com/ickby/CollaborationNode/utils"
	"bytes"
	"crypto/sha256"
	"encoding/json"

	"github.com/mr-tron/base58/base58"
)

type Identifier struct {
	Parent [32]byte
	Type   string
	Name   string
	Uuid	   string
}

func IdentifierFromData(data []byte) (Identifier, error) {

	var result Identifier
	err := json.Unmarshal(data, &result)
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to recreate Identifier from data")
	}
	return result, nil
}

func IdentifierFromEncoded(code string) (Identifier, error) {

	data, err := base58.Decode(code)
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to decode strig to Identifier data")
	}
	return IdentifierFromData(data)
}

func (self Identifier) Data() []byte {
	data, _ := json.Marshal(self)
	return data
}

func (self Identifier) Hash() [32]byte {
	return sha256.Sum256(self.Data())
}

func (self Identifier) Encode() string {
	return base58.Encode(self.Data())
}

func (self Identifier) Equal(id Identifier) bool {
	return bytes.Equal(self.Parent[:], id.Parent[:]) && self.Type == id.Type && self.Name == id.Name
}

func (self Identifier) Valid() bool {
	return self.Type != "" && self.Name != ""
}
