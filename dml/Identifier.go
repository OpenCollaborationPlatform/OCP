package dml

import (
	"crypto/sha256"
	"encoding/json"

	"github.com/mr-tron/base58/base58"
)

type identifier struct {
	Parent [32]byte
	Type   string
	Name   string
}

func (self identifier) hash() [32]byte {

	data, err := json.Marshal(self)
	if err != nil {
		var data [32]byte
		return data
	}
	return sha256.Sum256(data)
}

func (self identifier) encodedHash() string {
	hash := self.hash()
	return base58.Encode(hash[:])
}
