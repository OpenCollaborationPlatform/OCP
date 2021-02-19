// Identification.go
package p2p

import (
	"io/ioutil"
	"sync"

	crypto "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
)

type PeerID = peer.ID

var InvalidPeer = PeerID("")

func PeerIDFromString(id string) (PeerID, error) {

	peerid, err := peer.IDB58Decode(id)
	if err != nil {
		return InvalidPeer, err
	}
	return PeerID(peerid), nil
}

func PeerIDFromPublicKey(pk crypto.PubKey) (PeerID, error) {
	id, err := peer.IDFromPublicKey(pk)
	return PeerID(id), err
}

func PeerIDFromPublicKeyFile(file string) (PeerID, error) {

	content, err := ioutil.ReadFile(file)
	if err != nil {
		return InvalidPeer, err
	}
	key, err := crypto.UnmarshalPublicKey(content)
	if err != nil {
		return InvalidPeer, err
	}
	id, _ := peer.IDFromPublicKey(key)

	return PeerID(id), nil
}

//A concurency safe store for peers
type PeerSet struct {
	ps map[PeerID]struct{}
	lk sync.RWMutex
}

func NewPeerSet() *PeerSet {
	ps := new(PeerSet)
	ps.ps = make(map[PeerID]struct{})
	return ps
}

func (ps *PeerSet) Add(p PeerID) {
	ps.lk.Lock()
	ps.ps[p] = struct{}{}
	ps.lk.Unlock()
}

func (ps *PeerSet) Remove(p PeerID) {

	if !ps.Contains(p) {
		return
	}
	ps.lk.Lock()
	delete(ps.ps, p)
	ps.lk.Unlock()
}

func (ps *PeerSet) Contains(p PeerID) bool {
	ps.lk.RLock()
	_, ok := ps.ps[p]
	ps.lk.RUnlock()
	return ok
}

func (ps *PeerSet) Size() int {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	return len(ps.ps)
}

func (ps *PeerSet) Peers() []PeerID {
	ps.lk.Lock()
	out := make([]PeerID, 0, len(ps.ps))
	for p, _ := range ps.ps {
		out = append(out, p)
	}
	ps.lk.Unlock()
	return out
}
