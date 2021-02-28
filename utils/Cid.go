package utils

//condent identifier
import (
	"encoding/gob"
	"strings"

	cid "github.com/ipfs/go-cid"
)

func init() {
	gob.Register(new(Cid))
	Decoder.RegisterEncotable("cid", cidDecode)
}

var CidUndef = Cid{cid.Undef}

type Cid struct {
	cid.Cid
}

func CidDecode(code string) (Cid, error) {
	parts := strings.Split(code, "_")
	if len(parts) != 3 || parts[0] != "ocp" || parts[1] != "cid" {
		return CidUndef, NewError(Internal, "utils", "Invalid ecoded cid")
	}
	id, err := cid.Decode(parts[2])
	if err != nil {
		return CidUndef, NewError(Internal, "utils", err.Error())
	}
	return Cid{id}, nil
}

func cidDecode(code string) (interface{}, error) {
	id, err := cid.Decode(code)
	if err != nil {
		return CidUndef, NewError(Internal, "utils", err.Error())
	}
	return Cid{id}, nil
}

func (self Cid) Encode() string {
	return "ocp_cid_" + self.Cid.String()
}

func (self Cid) P2P() cid.Cid {
	return self.Cid
}
