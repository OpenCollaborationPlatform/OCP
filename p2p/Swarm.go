// Swarm: main functions
package p2p

import (
	"context"
	"path/filepath"
	"sync"

	crypto "github.com/libp2p/go-libp2p-crypto"
	"github.com/spf13/viper"
)

//Type that represents a collection if peers which are connected together and form
//a swarm. It allows to share data between all peers, as well as have common events
//and provide rpc calls
//The following properties hold:
// - Each peer is connected to all other peers in the swarm (for now at least)
// -- No forwarding of events
// -- No douplication of event
// - If a peer is allowed to send and receive data depends on the swarms peer list
// -- Authorisation must happen outside of the swarm: AddPeer has holy information
// -- Messages need to be signed with swarm key, but swarmkey is not exclusive right
//	  guarantee (a peer could have been removed from allowed peers afterwards)
// - Data cannot be send to a peer, only be requested by it
// - Data is split and transfer is split to all peers if possible
// - Event and rpc handling mimics the wamp interface
type Swarm struct {
	//general stuff
	peerLock sync.RWMutex
	host     *Host
	ID       SwarmID
	privKey  crypto.PrivKey
	pubKey   crypto.PubKey
	//peers    map[PeerID]peerConnection
	public bool
	ctx    context.Context
	cancel context.CancelFunc
}

type SwarmID string

func (id SwarmID) Pretty() string {
	return string(id)
}

//not accassible outside the package: should be used via Host only
func newSwarm(host *Host, id SwarmID, public bool, privKey crypto.PrivKey, pubKey crypto.PubKey) *Swarm {

	//the context to use for all goroutines
	ctx, cancel := context.WithCancel(context.Background())

	swarm := &Swarm{
		host:     host,
		peerLock: sync.RWMutex{},
		ID:       id,
		privKey:  privKey,
		pubKey:   pubKey,
		public:   public,
		//peers:          make(map[PeerID]peerConnection, 0),
		ctx:    ctx,
		cancel: cancel}

	return swarm
}

func (self *Swarm) Path() string {
	dir := viper.GetString("directory")
	return filepath.Join(dir, self.ID.Pretty())
}

/* Peer handling
 *************  */

//Peer is added, either as readonly, or with write allowance.
// - Connection only succeeds if the other peer has same swarm
// - Connection only succedds if the other peer has us added to swarm
// - Connection is retried periodically whenever it fails
func (s *Swarm) AddPeer(pid PeerID, readOnly bool) error {
	/*
		s.peerLock.Lock()
		//check if peer exist already
		_, ok := s.peers[pid]
		if ok {
			s.peerLock.Unlock()
			return nil
		}
		//and now add it
		s.peers[pid] = peerConnection{WriteAccess: !readOnly}
		s.peerLock.Unlock()

		s.connectPeer(pid)
	*/
	return nil
}

func (s *Swarm) RemovePeer(peer PeerID) {

}

func (s *Swarm) HasPeer(peer PeerID) bool {
	/*
		s.peerLock.RLock()
		defer s.peerLock.RUnlock()
		_, ok := s.peers[peer]
		return ok*/
	return false
}

/* General functions
 ******************  */

func (s *Swarm) IsPublic() bool {
	return s.public
}

func (s *Swarm) Close() {
	s.cancel()
}
