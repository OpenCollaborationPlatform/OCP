package p2p

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ickby/CollaborationNode/p2p/replica"
	"github.com/ickby/CollaborationNode/utils"

	"github.com/libp2p/go-libp2p-core/peer"
)

type State = replica.State

/******************************************************************************
							RPC API for replica
*******************************************************************************/

type ReplicaAPI struct {
	rep *replica.Replica
}

func (self *ReplicaAPI) AddCommand(ctx context.Context, op replica.Operation, ret *interface{}) error {

	if !self.rep.IsRunning() {
		return fmt.Errorf("Node not running: can't be the leader")
	}

	value, err := self.rep.AddCommand(ctx, op)
	*ret = value
	return err
}

type ReplicaReadAPI struct {
	rep  *replica.Replica
	conf *SwarmConfiguration
}

func (self *ReplicaReadAPI) GetLeader(ctx context.Context, inp struct{}, ret *peer.ID) error {

	if !self.rep.IsRunning() {
		return fmt.Errorf("Node not running: can't be the leader")
	}

	value, err := self.rep.GetLeader(ctx)
	*ret = value
	return err
}

//join is ReadAPI as also read only peers need to call it for themself. If joining is allowed will be
//checked in this function
func (self *ReplicaReadAPI) Join(ctx context.Context, peer PeerID, ret *AUTH_STATE) error {

	if !self.rep.IsRunning() {
		return fmt.Errorf("Node not running: can't be the leader")
	}

	if !self.conf.HasPeer(peer) {
		*ret = AUTH_NONE
		return fmt.Errorf("Peer is not allowed to join the state sharing")
	}
	auth := self.conf.PeerAuth(peer)
	err := self.rep.ConnectPeer(ctx, peer.pid(), auth == AUTH_READWRITE)
	*ret = auth
	return err
}

//leav is ReadAPI as also read only peers need to call it for themself.
func (self *ReplicaReadAPI) Leave(ctx context.Context, peer PeerID, ret *struct{}) error {

	if !self.rep.IsRunning() {
		return fmt.Errorf("Node not running: can't be the leader")
	}

	if !self.conf.HasPeer(peer) {
		return fmt.Errorf("Peer is not part of the state sharing: can't leave")
	}
	err := self.rep.DisconnectPeer(ctx, peer.pid())
	return err
}

/******************************************************************************
							shared state service
*******************************************************************************/

type sharedStateService struct {
	swarm *Swarm
	rep   *replica.Replica
	api   ReplicaAPI
	rApi  ReplicaReadAPI
}

func newSharedStateService(swarm *Swarm) (*sharedStateService, error) {

	//setup replica
	path := filepath.Join(swarm.GetPath())
	rep, err := replica.NewReplica(string(swarm.ID), path, swarm.host.host, swarm.host.dht)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create replica")
	}

	return &sharedStateService{swarm, rep, ReplicaAPI{}, ReplicaReadAPI{}}, nil
}

func (self *sharedStateService) IsRunning() bool {
	return self.rep.IsRunning()
}

//internal
func (self *sharedStateService) share(state replica.State) error {

	//get the name
	var name string
	if t := reflect.TypeOf(state); t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}

	//add to replica
	return self.rep.AddState(name, state)
}

func (self *sharedStateService) AddCommand(ctx context.Context, state string, cmd []byte) (interface{}, error) {

	if !self.IsRunning() {
		return nil, fmt.Errorf("Not running: cannot add command")
	}

	//build the operation
	op := replica.NewOperation(state, cmd)

	//fetch leader and call
	for {
		leader, err := self.rep.GetLeader(ctx)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get leader for state")
		}

		err = self.swarm.host.EnsureConnection(ctx, PeerID(leader))
		if err != nil {
			return nil, utils.StackError(err, "Unable to connect to leader")
		}

		var ret interface{}
		err = self.swarm.Rpc.CallContext(ctx, leader, "ReplicaAPI", "AddCommand", op, &ret)
		if err == nil {
			return ret, nil
		}

		select {
		case <-ctx.Done():
			if err != nil {
				return nil, utils.StackError(err, "Timout, no futher try on add command")
			}
			return nil, fmt.Errorf("Add command timed out")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil, fmt.Errorf("Not able to add command: failed")
}

func (self *sharedStateService) Close(ctx context.Context) {

	//if we are connecteed we need to change that: remove our self from the cluster
	//if we are the last peer in the cluster we simply shut down
	isLast, err := self.rep.IsLastPeer(self.swarm.host.ID().pid())
	if self.IsRunning() && !isLast && err == nil {

		//fetch leader and call him to leave
		leader, err := self.rep.GetLeader(ctx)
		if err == nil {
			err = self.swarm.host.EnsureConnection(ctx, PeerID(leader))
			if err != nil {
				return
			}

			var ret interface{}
			self.swarm.Rpc.CallContext(ctx, leader, "ReplicaReadAPI", "Leave", self.swarm.host.ID(), &ret)
		}
	}
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

		err = self.swarm.host.EnsureConnection(callctx, peer)
		if err != nil {
			continue
		}

		err = self.swarm.Rpc.CallContext(callctx, peer.pid(), "ReplicaReadAPI", "GetLeader", struct{}{}, &leader)
		if err == nil {
			break
		}

		//check if the context is still alive
		select {
		case <-ctx.Done():
			if err != nil {
				return utils.StackError(err, "Unable to inquery leader (asking peer %v)", peer)
			}
			return fmt.Errorf("Connect timed out: unable to find leader")
		default:
			break
		}
	}

	if err != nil {
		return utils.StackError(err, "Unable to find leader of swarm")
	}

	//call the leader to let us join
	var auth AUTH_STATE
	return self.swarm.Rpc.CallContext(callctx, leader, "ReplicaReadAPI", "Join", self.swarm.host.ID(), &auth)
}
