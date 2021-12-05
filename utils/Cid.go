package utils

//condent identifier
import (
	"encoding"
	"encoding/gob"
	"strings"

	cid "github.com/ipfs/go-cid"
)

var _ encoding.BinaryMarshaler = Cid{}
var _ encoding.BinaryUnmarshaler = (*Cid)(nil)

func init() {
	Decoder.RegisterEncotable("cid", cidDecode)
	gob.Register(new(Cid))
}

var CidUndef = Cid{cid.Undef}

type Cid struct {
	cid.Cid
}

func FromP2PCid(p2p cid.Cid) Cid {
	return Cid{p2p}
}

func CidDecode(code string) (Cid, error) {
	parts := strings.Split(code, "_")
	if len(parts) != 3 || parts[0] != "ocp" || parts[1] != "cid" {
		return CidUndef, NewError(Internal, "utils", "codec", "Invalid ecoded cid")
	}
	id, err := cid.Decode(parts[2])
	if err != nil {
		return CidUndef, NewError(Internal, "utils", "codec", err.Error())
	}
	return Cid{id}, nil
}

func cidDecode(code string) (interface{}, error) {
	id, err := cid.Decode(code)
	if err != nil {
		return CidUndef, NewError(Internal, "utils", "codec", err.Error())
	}
	return Cid{id}, nil
}

func (self Cid) Encode() string {
	return "ocp_cid_" + self.Cid.String()
}

func (self Cid) P2P() cid.Cid {
	return self.Cid
}

// It implements the encoding.BinaryUnmarshaler interface.
// Needed as we want to be able to use CidUndef in gob
func (self *Cid) UnmarshalBinary(data []byte) error {

	if data == nil || len(data) == 0 {
		self.Cid = cid.Undef
		return nil
	}
	casted, err := cid.Cast(data)
	if err != nil {
		return StackError(err, "Unable to decode p2p cid")
	}
	self.Cid = casted
	return nil
}

func (self Cid) MarshalBinary() ([]byte, error) {
	return self.Cid.MarshalBinary()
}
