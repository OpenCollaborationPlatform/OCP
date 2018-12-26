package dml

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/mr-tron/base58/base58"
)

type identifier struct {
	Parent [32]byte
	Type   string
	Name   string
}

func IdentifierFromData(data []byte) (identifier, error) {

	var result interface{}
	json.Unmarshal(data, result)
	id, ok := result.(identifier)
	if !ok {
		return identifier{}, fmt.Errorf("Data is not identifier")
	}
	return id, nil
}

func (self identifier) data() []byte {
	data, _ := json.Marshal(self)
	return data
}

func (self identifier) hash() [32]byte {
	return sha256.Sum256(self.data())
}

func (self identifier) encodedHash() string {
	hash := self.hash()
	return base58.Encode(hash[:])
}

func (self identifier) equal(id identifier) bool {
	return bytes.Equal(self.Parent[:], id.Parent[:]) && self.Type == id.Type && self.Name == id.Name
}

func (self identifier) valid() bool {
	return self.Type != "" && self.Name != ""
}
