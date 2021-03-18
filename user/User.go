package user

import (
	"github.com/OpenCollaborationPlatform/OCP /utils"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

type UserID string

func (id UserID) Pretty() string {
	return string(id)
}

//create a cid from the swarm ID to be used in the dht
func (id UserID) Cid() utils.Cid {
	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1}

	c, _ := pref.Sum([]byte(id))
	return utils.Cid{c}
}
