package replica

import (
	"CollaborationNode/utils"
	"encoding/json"

	crypto "github.com/libp2p/go-libp2p-crypto"
)

/*A log is a commit entry for all replicas. It holds the cmd in form of raw byte data
as well as all information needed to ensure its commited in the correct order. That
includes the commit index as well as the Epoch it is valid for. A log must be signed
by the leader of a given epoch to be valid. That allows log verification from untrusted
sources. */
type Log struct {
	Index     uint64
	Epoch     uint64
	Type      uint8
	Data      []byte
	Signature []byte
}

func (self *Log) IsValid() bool {
	return self.Data != nil
}

func (self *Log) Sign(key crypto.RsaPrivateKey) error {

	self.Signature = make([]byte, 0)
	data, err := self.ToBytes()
	if err != nil {
		return utils.StackError(err, "Unable to marshal log for signing")
	}
	sign, err := key.Sign(data)
	if err != nil {
		return utils.StackError(err, "Unable to sign log")
	}
	self.Signature = sign
	return nil
}

func (self *Log) Verify(key crypto.RsaPublicKey) bool {

	sign := self.Signature
	self.Signature = make([]byte, 0)
	defer func() { self.Signature = sign }()

	data, err := self.ToBytes()
	if err != nil {
		return false
	}
	valid, err := key.Verify(data, sign)

	if err != nil {
		return false
	}
	return valid
}

func (self *Log) ToBytes() ([]byte, error) {
	return json.Marshal(self)
}

func LogFromBytes(data []byte) (Log, error) {
	var log Log
	err := json.Unmarshal(data, &log)
	return log, err
}
