package dml

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/mr-tron/base58/base58"
)

func init() {
	gob.Register(new(Identifier))
}

type Identifier struct {
	Parent [32]byte
	Type   string
	Name   string
	Uuid   string
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
	if !self.Valid() {
		return [32]byte{}
	}
	return sha256.Sum256(self.Data())
}

func (self Identifier) Encode() string {
	return base58.Encode(self.Data())
}

func (self Identifier) Equal(id Identifier) bool {
	return bytes.Equal(self.Parent[:], id.Parent[:]) && self.Type == id.Type && self.Name == id.Name
}

func (self Identifier) Valid() bool {
	return self.Type != "" || self.Name != "" || self.Uuid != ""
}

func (self Identifier) String() string {

	var emptyByte [32]byte
	return fmt.Sprintf("Id{Name: %v; Type: %v; HasParent: %v, Uuid: %v}", self.Name, self.Type, !bytes.Equal(self.Parent[:], emptyByte[:]), self.Uuid)
}
