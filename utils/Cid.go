package utils

//condent identifier
import (
	"encoding/gob"
	"fmt"
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
		return CidUndef, fmt.Errorf("Invalid ecoded cid")
	}
	id, err := cid.Decode(parts[2])
	return Cid{id}, err
}

func cidDecode(code string) (interface{}, error) {
	id, err := cid.Decode(code)
	return Cid{id}, err
}

func (self Cid) Encode() string {
	return "ocp_cid_" + self.Cid.String()
}

func (self Cid) P2P() cid.Cid {
	return self.Cid
}
