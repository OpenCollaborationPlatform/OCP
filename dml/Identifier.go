package dml

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/OpenCollaborationPlatform/OCP /utils"

	"github.com/mr-tron/base58/base58"
)

func init() {
	gob.Register(new(Identifier))
	utils.Decoder.RegisterEncotable("id", identifierDecode)
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
		return Identifier{}, wrapInternalError(err, Error_Fatal)
	}
	return result, nil
}

func IdentifierFromEncoded(code string) (Identifier, error) {

	parts := strings.Split(code, "_")
	if len(parts) != 3 || parts[0] != "ocp" || parts[1] != "id" {
		return Identifier{}, newInternalError(Error_Fatal, "Invalid ecoded identifier")
	}
	data, err := base58.Decode(parts[2])
	if err != nil {
		return Identifier{}, wrapInternalError(err, Error_Fatal)
	}

	return IdentifierFromData(data)
}

func identifierDecode(code string) (interface{}, error) {
	data, err := base58.Decode(code)
	if err != nil {
		return Identifier{}, wrapInternalError(err, Error_Fatal).(utils.OCPError)
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
	return "ocp_id_" + base58.Encode(self.Data())
}

func (self Identifier) Equals(id Identifier) bool {
	return bytes.Equal(self.Parent[:], id.Parent[:]) && self.Type == id.Type && self.Name == id.Name && self.Uuid == id.Uuid
}

func (self Identifier) Valid() bool {
	return self.Type != "" || self.Name != "" || self.Uuid != ""
}

func (self Identifier) String() string {

	var emptyByte [32]byte
	return fmt.Sprintf("Id{Name: %v; Type: %v; HasParent: %v, Uuid: %v}", self.Name, self.Type, !bytes.Equal(self.Parent[:], emptyByte[:]), self.Uuid)
}
