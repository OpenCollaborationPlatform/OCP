// Swarm: main functions
package p2p

import (
	"context"
	"os"
	"path/filepath"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/OpenCollaborationPlatform/OCP /utils"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
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
	host   *Host
	ID     SwarmID
	conf   SwarmConfiguration
	ctx    context.Context
	cancel context.CancelFunc
	logger hclog.Logger

	//some internal data
	path string
}

type SwarmID string

func (id SwarmID) Pretty() string {
	return string(id)
}

//create a cid from the swarm ID to be used in the dht
func (id SwarmID) Cid() cid.Cid {
	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1}

	c, _ := pref.Sum([]byte(id))
	return c
}

//little helper to add swarms to the states
func SwarmStates(states ...State) []State {
	return states
}

func NoStates() []State {
	return make([]State, 0)
}

func SwarmPeers(peers ...PeerID) []PeerID {
	return peers
}

func NoPeers() []PeerID {
	return make([]PeerID, 0)
}

/*******************************************************************************
								Swarm
*******************************************************************************/

//not accassible outside the package: should be used via Host only
//host: 			The p2p Host the swarm is build upon
//id:			A swarm ID uniquely identifying the swam
//states:		All states that shall be shared by the swarm
//bootstrap:		True if this is a new swarm and should be startup, false if we join an existing swarm
//knownPeers:	A list of known peers that are in the swarm (only relevant if bootstrap=false)
func newSwarm(ctx context.Context, host *Host, id SwarmID, states []State, bootstrap bool, knownPeers []PeerID, logger hclog.Logger) (*Swarm, error) {

	//the context to use for all goroutines
	swarmctx, cancel := context.WithCancel(context.Background())

	swarm := &Swarm{
		host:   host,
		ID:     id,
		conf:   newSwarmConfiguration(),
		ctx:    swarmctx,
		cancel: cancel,
		path:   filepath.Join(host.path, "Documents", string(id)),
		logger: logger,
		Event:  nil,
		State:  nil,
		Data:   nil,
		Rpc:    nil,
	}

	//ensure our folder exist
	err := os.MkdirAll(swarm.GetPath(), os.ModePerm)
	if err != nil {
		return nil, newInternalError(Error_Operation_Invalid, "cannot create swarm folder", "error", err.Error())
	}

	//build the services
	err = nil
	swarm.Rpc = newSwarmRpcService(swarm)

	swarm.Event = newSwarmEventService(swarm)
	swarm.Event.RegisterTopic("peerAdded", AUTH_READWRITE)
	swarm.Event.RegisterTopic("peerRemoved", AUTH_READWRITE)
	swarm.Event.RegisterTopic("peerAuthChanged", AUTH_READWRITE)
	swarm.Event.RegisterTopic("peerActivityChanged", AUTH_READWRITE)

	swarm.State, err = newSharedStateService(swarm)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create shared state service")
	}
	swarm.Data = newSwarmDataService(swarm)

	//setup the shared states, own and user ones
	swarm.State.share(&swarm.conf)
	for _, state := range states {
		err := swarm.State.share(state)
		if err != nil {
			return nil, utils.StackError(err, "Unable to setup swarm")
		}
	}

	//startup state sharing. If bootstrap we add ourself to the config
	swarm.State.startup(bootstrap)
	if bootstrap {
		op := SwarmConfOp{false, host.ID(), AUTH_READWRITE}
		_, err := swarm.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())
		if err != nil {
			return nil, utils.StackError(err, "Unable to setup swarm")
		}

	} else {
		if len(knownPeers) != 0 {
			err := swarm.State.connect(ctx, knownPeers)
			if err != nil {
				return nil, utils.StackError(err, "Unable to connect to swarm via provided peers")
			}

		} else {
			//let's go and search a swarm member!
			peerChan := swarm.host.findSwarmPeersAsync(ctx, swarm.ID, 1)

			select {
			case peer, more := <-peerChan:
				if !more {
					//we are not able to find any peers... that is bad!
					return nil, newConnectionError(Error_Process, "Unable to find peers to join swarm")
				}
				err := swarm.State.connect(ctx, []PeerID{peer})
				if err != nil {
					//we are not able to join the swarm... that is bad!
					return nil, utils.StackError(err, "Unable to connect to swarm states")
				}
				break

			case <-ctx.Done():
				//we did not find any swarm member... return with error
				return nil, newConnectionError(Error_Process, "Did not find any swarm members before timeout")
			}

			//connect to all
		}
	}

	//make our self known!
	err = host.dht.Provide(ctx, id.Cid(), true)
	if err != nil {
		return nil, wrapConnectionError(err, Error_Process)
	}

	//and make sure we stay known!
	ticker := time.NewTicker(10 * time.Hour)
	go func() {
		for {
			select {
			case <-ticker.C:
				providectx, _ := context.WithTimeout(swarmctx, 60*time.Minute)
				host.dht.Provide(providectx, id.Cid(), true)

			case <-swarmctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	return swarm, nil
}

/* Peer handling
 *************  */

//Peer is added and hence allowed to use all swarm functionality.
func (s *Swarm) AddPeer(ctx context.Context, pid PeerID, state AUTH_STATE) error {

	if !s.State.IsRunning() {
		return newInternalError(Error_Setup, "Swarm not fully setup: cannot add peer")
	}

	//build the operation
	op := SwarmConfOp{
		Remove: false,
		Peer:   pid,
		Auth:   state,
	}

	_, err := s.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())

	if err == nil {
		s.Event.Publish("peerAdded", pid.Pretty(), AuthStateToString(state))
	} else {
		s.logger.Debug("Adding peer failed", "peer", pid, "auth", state, "error", err)
	}

	return utils.StackOnError(err, "Unable to add conf update command to state")
}

func (s *Swarm) RemovePeer(ctx context.Context, peer PeerID) error {

	if !s.State.IsRunning() {
		return newInternalError(Error_Setup, "Swarm not fully setup: cannot remove peer")
	}

	if peer == s.host.ID() {
		return newUserError(Error_Operation_Invalid, "Cannot remove yourself")
	}

	//build the operation
	op := SwarmConfOp{
		Remove: true,
		Peer:   peer,
		Auth:   AUTH_NONE,
	}

	_, err := s.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())

	if err == nil {
		s.Event.Publish("peerRemoved", peer.Pretty())
	} else {
		s.logger.Debug("Removing peer failed", "peer", peer, "error", err)
	}

	return utils.StackOnError(err, "Unable to add conf update command to state")
}

func (s *Swarm) ChangePeer(ctx context.Context, peer PeerID, auth AUTH_STATE) error {

	if !s.State.IsRunning() {
		return newInternalError(Error_Setup, "Swarm not fully setup: cannot remove peer")
	}

	if peer == s.host.ID() {
		return newUserError(Error_Operation_Invalid, "Cannot change your own authorisation")
	}

	if !s.HasPeer(peer) {
		return newUserError(Error_Operation_Invalid, "Peer is not in swarm, cannot change auth")
	}

	if s.PeerAuth(peer) == auth {
		s.logger.Debug("Changing peer not needed, already has auth", "peer", peer, "auth", auth)
		return nil
	}

	//build the operation
	op := SwarmConfOp{
		Remove: false,
		Peer:   peer,
		Auth:   auth,
	}

	_, err := s.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())

	if err == nil {
		s.logger.Debug("Changing peer worked", "peer", peer.Pretty(), "auth", AuthStateToString(auth))
		err := s.Event.Publish("peerAuthChanged", peer.Pretty(), AuthStateToString(auth))
		if err != nil {
			s.logger.Error("Publishing changed auth event failed", "peer", peer.Pretty(), "auth", AuthStateToString(auth), "error", err)
		}
	} else {
		s.logger.Debug("Changing peer failed", "peer", peer, "auth", auth, "error", err)
	}

	return utils.StackOnError(err, "Unable to add conf update command to state")
}

func (s *Swarm) HasPeer(peer PeerID) bool {
	return s.conf.HasPeer(peer)
}

//returns all peers with the given or higher auth state.
//E.g. AUTH_READONLY returns all peers with read only as well as read write auth
//If you want all peers use AUTH_NONE
func (s *Swarm) GetPeers(state AUTH_STATE) []PeerID {
	return s.conf.GetPeers(state)
}

func (self *Swarm) PeerAuth(peer PeerID) AUTH_STATE {
	return self.conf.PeerAuth(peer)
}

/* General functions
 ******************  */

//Tries to connect to given peer. It keeps it open as long as the swarm exists
//(afterwards may be closed when too many connections are open)
func (self *Swarm) connect(ctx context.Context, peer PeerID) error {

	err := self.host.Connect(ctx, peer, false)
	if err == nil {
		self.host.host.ConnManager().Protect(peer, string(self.ID))
	}

	return err
}

//Allows the connection to be closed when the host seems fit
func (self *Swarm) closeConnection(peer PeerID) error {

	if self.host.host.ConnManager().IsProtected(peer, string(self.ID)) {
		self.host.host.ConnManager().Unprotect(peer, string(self.ID))
	}
	return nil
}

func (self *Swarm) GetPath() string {
	return self.path
}

func (s *Swarm) Close(ctx context.Context) {

	if !s.State.IsRunning() {
		return
	}

	//clear the datafolder (make sure this always happen, even on timeout)
	path := s.GetPath()
	defer os.RemoveAll(path)

	if s.State != nil {
		s.State.Close(ctx)
	}
	if s.Data != nil {
		s.Data.Close()
	}
	if s.Event != nil {
		s.Event.Stop()
	}
	s.cancel()

	//unprotect all peers
	for _, peer := range s.host.host.Peerstore().PeersWithAddrs() {
		s.host.host.ConnManager().Unprotect(peer, string(s.ID))
	}

	s.host.removeSwarm(s.ID)
	s.logger.Debug("Swarm closed successfully")
}

func (s *Swarm) GetHost() *Host {
	return s.host
}
