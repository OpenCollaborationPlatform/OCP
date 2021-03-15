package p2p

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ickby/CollaborationNode/p2p/replica"
	"github.com/ickby/CollaborationNode/utils"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/libp2p/go-libp2p-core/peer"
)

type State = replica.State
type StateEvent = replica.Event
type StateObserver = replica.Observer
type State_Event_Type = replica.Event_Type

const (
	STATE_EVENT_PEER_ACTIVE        = replica.EVENT_PEER_ADDED
	STATE_EVENT_PEER_INACTIVE      = replica.EVENT_PEER_REMOVED
	STATE_EVENT_MAJORITY_AVAILABLE = replica.EVENT_MAJORITY_AVAILABLE
	STATE_EVENT_MAJORITY_LOST      = replica.EVENT_MAJORITY_LOST
)

/******************************************************************************
							RPC API for replica
*******************************************************************************/

type ReplicaAPI struct {
	rep *replica.Replica
}

func (self *ReplicaAPI) AddCommand(ctx context.Context, op replica.Operation, ret *bool) error {

	if !self.rep.IsRunning() {
		return fmt.Errorf("Node not running: can't be the leader")
	}

	err := self.rep.AddCommand(ctx, op)
	*ret = (err == nil)
	return utils.StackOnError(err, "Unable to add command to replica")
}

// Api callable by Read Only auth
type ReplicaReadAPI struct {
	rep  *replica.Replica
	conf *SwarmConfiguration
}

func (self *ReplicaReadAPI) GetLeader(ctx context.Context, inp struct{}, ret *peer.ID) error {

	if !self.rep.IsRunning() {
		return newInternalError(Error_Setup, "Node not running: can't get the leader")
	}

	value, err := self.rep.GetLeader(ctx)
	*ret = value
	return utils.StackOnError(err, "Unable to get leader from replica")
}

//join is ReadAPI as also read only peers need to call it for themself. If joining is allowed will be
//checked in this function
func (self *ReplicaReadAPI) Join(ctx context.Context, peer PeerID, ret *AUTH_STATE) error {

	if !self.rep.IsRunning() {
		return newInternalError(Error_Setup, "Node not running: can't be the leader")
	}

	if !self.conf.HasPeer(peer) {
		*ret = AUTH_NONE
		return newConnectionError(Error_Authorisation, "Peer is not allowed to join the state sharing")
	}
	auth := self.conf.PeerAuth(peer)
	err := self.rep.ConnectPeer(ctx, peer, auth == AUTH_READWRITE)
	*ret = auth
	return utils.StackOnError(err, "Unable to connect peer to replica")
}

//leav is ReadAPI as also read only peers need to call it for themself.
func (self *ReplicaReadAPI) Leave(ctx context.Context, peer PeerID, ret *struct{}) error {

	if !self.rep.IsRunning() {
		return newInternalError(Error_Setup, "Node not running: can't be the leader")
	}

	if !self.conf.HasPeer(peer) {
		return newConnectionError(Error_Authorisation, "Peer is not part of the state sharing: can't leave")
	}
	err := self.rep.DisconnectPeer(ctx, peer)
	return utils.StackOnError(err, "Unable to disconnect peer from replica")
}

/******************************************************************************
							shared state service
*******************************************************************************/

type sharedStateService struct {
	swarm  *Swarm
	rep    *replica.Replica
	api    ReplicaAPI
	rApi   ReplicaReadAPI
	logger hclog.Logger
	repObs StateObserver
}

func newSharedStateService(swarm *Swarm) (*sharedStateService, error) {

	//setup replica
	path := filepath.Join(swarm.GetPath())
	rep, err := replica.NewReplica(string(swarm.ID), path, swarm.host.host, swarm.host.dht)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create replica")
	}

	return &sharedStateService{swarm, rep, ReplicaAPI{}, ReplicaReadAPI{},
		swarm.logger.Named("State"), StateObserver{}}, nil

}

func (self *sharedStateService) IsRunning() bool {
	return self.rep.IsRunning()
}

func (self *sharedStateService) AddCommand(ctx context.Context, state string, cmd []byte) (interface{}, error) {

	if !self.IsRunning() {
		return nil, newInternalError(Error_Setup, "Not running: cannot add command")
	}

	//build the operation
	op := replica.NewOperation(state, cmd)

	//we use a default timeout to prevent stalling in case replica is in a deadlock
	cmdCtx, _ := context.WithTimeout(ctx, 5*time.Second)

	leader, err := self.rep.GetLeader(cmdCtx)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get leader for state")
	}

	err = self.swarm.connect(cmdCtx, PeerID(leader))
	if err != nil {
		return nil, utils.StackError(err, "Unable to connect to leader")
	}

	resultC, err := self.rep.CreateResultChannel(op.OpID)
	if err != nil {
		return nil, newInternalError(Error_Setup, "Unable to receive result due to operation doubling")
	}

	var ret interface{}
	err = self.swarm.Rpc.CallContext(cmdCtx, leader, "ReplicaAPI", "AddCommand", op, &ret)
	if err != nil {
		return nil, err
	}

	select {
	case result := <-resultC:
		return result, nil

	case <-cmdCtx.Done():
		return nil, newConnectionError(Error_Process, "Command timed out")
	}

	return nil, newConnectionError(Error_Process, "Not able to add command")
}

func (self *sharedStateService) Close(ctx context.Context) {

	if !self.IsRunning() {
		return
	}

	//if we are connecteed we need to change that: remove our self from the cluster
	isLast, err := self.rep.IsLastPeer(self.swarm.host.ID())
	if !isLast && err == nil {
		self.logger.Debug("Leaving replica with multiple active nodes")

		//fetch leader and call him to leave
		leader, err := self.rep.GetLeader(ctx)
		if err == nil && leader != PeerID("") {

			//if we are leader we transfer leadership first
			if leader == self.swarm.host.ID() {
				self.logger.Debug("Leaving as leader, try to transfer leadership first")
				leader, err = self.rep.TransferLeadership(ctx)
				if err != nil {
					self.logger.Warn("Leadership transfer before leave failed, hard shutdown", "error", err)
					self.rep.Shutdown(ctx)
					return
				}
			}

			//call leader to remove us!
			err = self.swarm.connect(ctx, PeerID(leader))
			if err != nil {
				self.logger.Warn("Unable to connect to leader on leave, hard shutdown", "leader", leader.Pretty())
				self.rep.Shutdown(ctx)
				return
			}

			var ret interface{}
			err := self.swarm.Rpc.CallContext(ctx, leader, "ReplicaReadAPI", "Leave", self.swarm.host.ID(), &ret)
			if err != nil {
				self.logger.Warn("Leaving replication failed, hard shutdown", "leader", leader, "error", err)
				self.rep.Shutdown(ctx)
				return
			} else {
				self.logger.Debug("Successfully left replication")
			}
		}
	}

	self.logger.Debug("Close replica")
	self.rep.CloseObserver(self.repObs)
	self.rep.Close(ctx)
}

func (self *sharedStateService) ActivePeers() ([]PeerID, error) {

	peers, err := self.rep.ConnectedPeers()
	if err != nil {
		return nil, utils.StackError(err, "Unable to query replica connected peers")
	}
	result := make([]PeerID, len(peers))
	for i, p := range peers {
		result[i] = PeerID(p)
	}
	return result, nil
}

func (self *sharedStateService) HasMajority() bool {
	if !self.IsRunning() {
		return false
	}
	return self.rep.HasMajority()
}

// Internal functions
// ***********************************************************************************

func (self *sharedStateService) share(state replica.State) error {

	//get the name
	var name string
	if t := reflect.TypeOf(state); t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}

	//add to replica
	return utils.StackOnError(self.rep.AddState(name, state), "Unable to add state to replica")
}

func (self *sharedStateService) startup(bootstrap bool) error {

	//check if we need to bootstrap or join
	if bootstrap {
		err := self.rep.Bootstrap()
		if err != nil {
			return utils.StackError(err, "Unable to bootstrap replica")
		}

	} else {
		err := self.rep.Join()
		if err != nil {
			return utils.StackError(err, "Unable to join replica")
		}
	}

	self.repObs = self.rep.NewObserver(false)
	go self.eventLoop(self.repObs)

	//setup API
	self.api = ReplicaAPI{self.rep}
	err := self.swarm.Rpc.Register(&self.api, AUTH_READWRITE)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica API")
	}
	self.rApi = ReplicaReadAPI{self.rep, &self.swarm.conf}
	err = self.swarm.Rpc.Register(&self.rApi, AUTH_READONLY)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica Read API")
	}

	return nil
}

//try to connect to peers sharing the same states
func (self *sharedStateService) connect(ctx context.Context, peers []PeerID) error {

	//we go over all peers to fetch the leader of the current state
	callctx := ctx
	if len(peers) > 1 {
		callctx, _ = context.WithTimeout(ctx, 1*time.Second)
	}
	var err error = nil
	var leader peer.ID
	for _, peer := range peers {

		err = self.swarm.connect(callctx, peer)
		if err != nil {
			continue
		}

		err = self.swarm.Rpc.CallContext(callctx, peer, "ReplicaReadAPI", "GetLeader", struct{}{}, &leader)
		if err == nil {
			break
		}

		//check if the context is still alive
		select {
		case <-ctx.Done():
			if err != nil {
				return utils.StackError(err, "Unable to inquery leader (asking peer %v)", peer)
			}
			return newConnectionError(Error_Process, "Connect timed out: unable to find leader")
		default:
			break
		}
	}

	if err != nil {
		return newConnectionError(Error_Process, "Unable to find leader of swarm", "error", err.Error())
	}

	//call the leader to let us join
	var auth AUTH_STATE
	err = self.swarm.Rpc.CallContext(callctx, leader, "ReplicaReadAPI", "Join", self.swarm.host.ID(), &auth)
	err = utils.StackOnError(err, "Unable to call leader with join rpc")
	return err
}

func (self *sharedStateService) eventLoop(obs StateObserver) {

	for evt := range obs.EventChannel() {
		if evt.Event == STATE_EVENT_PEER_ACTIVE {
			self.logger.Debug("Peer became active", "peer", evt.Peer.Pretty())
			go self.swarm.connect(self.swarm.ctx, evt.Peer)
			self.swarm.Event.Publish("peerActivityChanged", evt.Peer.Pretty(), true)

		} else if evt.Event == STATE_EVENT_PEER_INACTIVE {
			self.logger.Debug("Peer became inactive", "peer", evt.Peer.Pretty())
			self.swarm.Event.Publish("peerActivityChanged", evt.Peer.Pretty(), false)

		} else if evt.Event == replica.EVENT_LEADER_CHANGED {
			self.logger.Debug("New leader", "peer", evt.Peer.Pretty())

		} else if evt.Event == STATE_EVENT_MAJORITY_AVAILABLE {
			self.logger.Debug("Majority became available")

		} else if evt.Event == STATE_EVENT_PEER_INACTIVE {
			self.logger.Debug("Majority lost")
		}
	}
}
