package p2p

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

type SwarmConfOp struct {
	Remove bool
	Peer   PeerID
	Auth   AUTH_STATE
}

func (self SwarmConfOp) ToBytes() []byte {

	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(self)
	if err != nil {
		return nil
	}
	return b.Bytes()
}

func swarmConfOpFromBytes(data []byte) (SwarmConfOp, error) {

	var op SwarmConfOp
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&op)
	return op, err
}

//this is a replica state
type SwarmConfigutarion struct {
	Peer  map[PeerID]AUTH_STATE
	mutex sync.RWMutex
}

func newSwarmConfiguration() SwarmConfigutarion {
	return SwarmConfigutarion{make(map[PeerID]AUTH_STATE), sync.RWMutex{}}
}

//state interface
func (self *SwarmConfigutarion) Apply(data []byte) interface{} {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	op, err := swarmConfOpFromBytes(data)
	if err != nil {
		return fmt.Errorf("No a correct operation")
	}

	if op.Remove {
		delete(self.Peer, op.Peer)

	} else {
		self.Peer[op.Peer] = op.Auth
	}

	return nil
}

//state interface
func (self *SwarmConfigutarion) Snapshot() ([]byte, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(self)
	if err != nil {
		return make([]byte, 0), err
	}
	return b.Bytes(), nil
}

//state interface
func (self *SwarmConfigutarion) LoadSnapshot(data []byte) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&self)
	return err
}

//Custom functions

func (self *SwarmConfigutarion) HasPeer(peer PeerID) bool {

	self.mutex.RLock()
	defer self.mutex.RUnlock()
	_, has := self.Peer[peer]
	return has
}

func (self *SwarmConfigutarion) GetPeers(state AUTH_STATE) []PeerID {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	result := make([]PeerID, 0)
	for peer, auth := range self.Peer {

		switch state {

		case AUTH_NONE:
			result = append(result, peer)

		case AUTH_READONLY:
			if auth != AUTH_NONE {
				result = append(result, peer)
			}

		case AUTH_READWRITE:
			if auth == AUTH_READWRITE {
				result = append(result, peer)
			}
		}
	}

	return result
}

func (self *SwarmConfigutarion) PeerAuth(peer PeerID) AUTH_STATE {

	self.mutex.RLock()
	defer self.mutex.RUnlock()
	state, ok := self.Peer[peer]
	if !ok {
		return AUTH_NONE
	}
	return state
}
