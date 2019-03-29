// Swarm: main functions
package p2p

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/viper"
)

// Type that represents a collection if peers which are connected together and form
// a swarm. It allows to share data between all peers, as well as have common events
// and provide rpc calls.
//
// A swarm does always allow reading for its pears! It is not possible to make it secure,
// don't use it if the shared information is not public. Even if a peer is not added it is
// easily possible to catch the information for attacking nodes.
//
// It does allow for a certain Authorisation sheme: ReadOnly or ReadWrite. It has the
// folloing impact on the swarm services:
// - RPC:
// 	-- ReadOnly Peer can be adressed for all RPC calls it offers (may fail dependend on its on AUTH info)
//  -- ReadOnly Peer is allowed to call all registered ReadOnly RPCs of this swarm
//  -- ReadOnly Peer is not allowed to call all registered ReadWrtie RPCs of this swarm. Those calls will fail.
//  -- ReadWrite Peer is additionaly allowed to call ReadWrite RPC's of this swarm
// - Event:
//  -- ReadOnly Peer will receive all events send from this swarm (may fail dependend on its on AUTH info)
//  -- ReadOnly Peer can publish ReadOnly events to this swarm
//  -- ReadOnly Peer can not publish ReadWrite event to this swarm
//  -- ReadWrite Peer can additionally publish ReadWrite events to this swarm
// - Data:
//  -- ReadOnly Peer will receive all shared file information and the files itself
//  -- ReadOnly Peer will not be able to add new files to the swarm
//  -- ReadWrite Peer will be able to add new files to the swarm
type Swarm struct {
	//provided services
	Event *swarmEventService
	Rpc   *swarmRpcService
	Data  DataService
	State *sharedStateService

	//general stuff
	host     *Host
	ID       SwarmID
	peers    map[PeerID]AUTH_STATE
	peerLock sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc

	//some intenral data
	path string
}

type SwarmID string

func (id SwarmID) Pretty() string {
	return string(id)
}

//not accassible outside the package: should be used via Host only
func newSwarm(host *Host, id SwarmID) *Swarm {

	//the context to use for all goroutines
	ctx, cancel := context.WithCancel(context.Background())

	swarm := &Swarm{
		host:   host,
		ID:     id,
		peers:  make(map[PeerID]AUTH_STATE),
		ctx:    ctx,
		cancel: cancel,
		path:   filepath.Join(viper.GetString("directory"), id.Pretty())}

	//build the services
	swarm.Rpc = newSwarmRpcService(swarm)
	swarm.Event = newSwarmEventService(swarm)
	swarm.State = newSharedStateService(swarm)
	swarm.Data = newSwarmDataService(swarm)

	//ensure our folder exist
	os.MkdirAll(swarm.GetPath(), os.ModePerm)

	return swarm
}

/* Peer handling
 *************  */

//Peer is added and hence allowed to use all swarm functionality
func (s *Swarm) AddPeer(pid PeerID, state AUTH_STATE) error {

	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	_, ok := s.peers[pid]
	if ok {
		return fmt.Errorf("Peer already added")
	}
	s.peers[pid] = state
	return nil
}

func (s *Swarm) RemovePeer(peer PeerID) error {

	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	delete(s.peers, peer)
	return nil
}

func (s *Swarm) HasPeer(peer PeerID) bool {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	_, has := s.peers[peer]
	return has
}

func (self *Swarm) PeerAuth(peer PeerID) AUTH_STATE {

	//the swarm itself is always readwrite
	//TODO: Makes this sense or should it be correct state?
	if peer == self.host.ID() {
		return AUTH_READWRITE
	}

	self.peerLock.RLock()
	defer self.peerLock.RUnlock()
	state, ok := self.peers[peer]
	if !ok {
		return AUTH_NONE
	}
	return state
}

/* General functions
 ******************  */

func (self *Swarm) GetPath() string {
	return self.path
}

func (s *Swarm) Close() {
	s.cancel()
}
