// Swarm: main functions
package p2p

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ickby/CollaborationNode/utils"
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
func newSwarm(host *Host, id SwarmID, states []State, bootstrap bool, knownPeers []PeerID) (*Swarm, error) {

	//the context to use for all goroutines
	ctx, cancel := context.WithCancel(context.Background())

	swarm := &Swarm{
		host:   host,
		ID:     id,
		conf:   newSwarmConfiguration(),
		ctx:    ctx,
		cancel: cancel,
		path:   host.path}

	//ensure our folder exist
	os.MkdirAll(swarm.GetPath(), os.ModePerm)

	//build the services
	swarm.Rpc = newSwarmRpcService(swarm)
	swarm.Event = newSwarmEventService(swarm)
	swarm.State = newSharedStateService(swarm)
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
		addctx, _ := context.WithTimeout(ctx, 2*time.Second)
		_, err := swarm.State.AddCommand(addctx, "SwarmConfiguration", op.ToBytes())
		if err != nil {
			return nil, utils.StackError(err, "Unable to setup swarm")
		}

	} else {
		if len(knownPeers) != 0 {
			cnnctx, _ := context.WithTimeout(ctx, 5*time.Second)
			err := swarm.State.connect(cnnctx, knownPeers)
			if err != nil {
				return nil, utils.StackError(err, "Unable to connect to swarm via provided peers")
			}

		} else {
			//let's go and search a swarm member!
			findctx, cncl := context.WithTimeout(ctx, 60*time.Second)
			addrChan := host.dht.FindProvidersAsync(findctx, id.Cid(), 5)
		loop:
			for {
				select {
				case info := <-addrChan:
					if info.ID.Validate() == nil && len(info.Addrs) != 0 {
						cncl()
						err := host.SetMultipleAdress(PeerID(info.ID), info.Addrs)
						if err != nil {
							//we are unable to set the adresst: try the next one we find
							break
						}
						err = host.Connect(findctx, PeerID(info.ID))
						if err != nil {
							//we are unable to connect: try the next one we find
							break
						}
						err = swarm.State.connect(findctx, []PeerID{PeerID(info.ID)})
						if err != nil {
							//we are not able to join the swarm... that is bad!
							return nil, utils.StackError(err, "Unable to connect to swarm states")
						}
						break loop
					}

				case <-findctx.Done():
					//we did not find any swarm member... return with error
					return nil, fmt.Errorf("Did not find any swarm member before timeout")
				}
			}
		}
	}

	//make our self known!
	prvdctx, _ := context.WithTimeout(ctx, 1*time.Second)
	err := host.dht.Provide(prvdctx, id.Cid(), true)
	if err != nil {
		return nil, utils.StackError(err, "Unable to announce swarm")
	}
	return swarm, nil
}

/* Peer handling
 *************  */

//Peer is added and hence allowed to use all swarm functionality.
func (s *Swarm) AddPeer(ctx context.Context, pid PeerID, state AUTH_STATE) error {

	if !s.State.IsRunning() {
		return fmt.Errorf("Swarm not fully setup: cannot add peer")
	}

	//build the operation
	op := SwarmConfOp{
		Remove: false,
		Peer:   pid,
		Auth:   state,
	}

	_, err := s.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())
	return err
}

func (s *Swarm) RemovePeer(ctx context.Context, peer PeerID) error {

	if !s.State.IsRunning() {
		return fmt.Errorf("Swarm not fully setup: cannot remove peer")
	}

	//build the operation
	op := SwarmConfOp{
		Remove: true,
		Peer:   peer,
		Auth:   AUTH_NONE,
	}

	_, err := s.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())
	return err
}

func (s *Swarm) ChangePeer(ctx context.Context, peer PeerID, auth AUTH_STATE) error {

	if !s.State.IsRunning() {
		return fmt.Errorf("Swarm not fully setup: cannot add peer")
	}

	//build the operation
	op := SwarmConfOp{
		Remove: false,
		Peer:   peer,
		Auth:   auth,
	}

	_, err := s.State.AddCommand(ctx, "SwarmConfiguration", op.ToBytes())
	return err
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

func (self *Swarm) GetPath() string {
	return self.path
}

func (s *Swarm) Close(ctx context.Context) {

	s.Event.Stop()
	s.Data.Close()
	s.State.Close(ctx)
	s.cancel()

	s.host.removeSwarm(s.ID)
}
