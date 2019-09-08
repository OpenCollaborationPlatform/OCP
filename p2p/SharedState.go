package p2p

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

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

func (self *ReplicaAPI) AddCommand(ctx context.Context,op replica.Operation, ret *interface{}) error {

	value, err := self.rep.AddCommand(ctx, op)
	*ret = value
	return err
}

type ReplicaReadAPI struct {
	rep *replica.Replica
}

func (self *ReplicaReadAPI) GetLeader(ctx context.Context, inp struct{}, ret *peer.ID) error {

	value, err := self.rep.GetLeader(ctx)
	*ret = value
	return err
}

func (self *ReplicaReadAPI) EnsureParticipation(ctx context.Context, id peer.ID, ret *struct{}) error {

	err := self.rep.EnsureParticipation(ctx, id)
	return err
}

/******************************************************************************
							shared state service
*******************************************************************************/

//implements peerprovider interface of replica
type swarmPeerProvider struct {
	swarm *Swarm
}

func (self swarmPeerProvider) HasPeer(id peer.ID) bool {
	return self.swarm.HasPeer(PeerID(id))
}

func (self swarmPeerProvider) CanWrite(id peer.ID) (bool, error) {
	if !self.swarm.HasPeer(PeerID(id)) {
		return false, fmt.Errorf("No such peer available")
	}
	auth := self.swarm.PeerAuth(PeerID(id))
	return auth == AUTH_READWRITE, nil
}

func (self swarmPeerProvider) GetReadPeers() []peer.ID {
	peers := self.swarm.GetPeers(AUTH_READONLY)
	ret := make([]peer.ID, len(peers))
	for i, p := range peers {
		ret[i] = p.pid()
	}
	return ret
}
func (self swarmPeerProvider) GetWritePeers() []peer.ID {
	peers := self.swarm.GetPeers(AUTH_READWRITE)
	ret := make([]peer.ID, len(peers))
	for i, p := range peers {
		ret[i] = p.pid()
	}
	return ret
}

type sharedStateService struct {
	swarm *Swarm
	rep   *replica.Replica
	api   ReplicaAPI
	rApi  ReplicaReadAPI
}

func newSharedStateService(swarm *Swarm) *sharedStateService {

	//setup replica
	path := filepath.Join(swarm.GetPath())
	rep, err := replica.NewReplica(string(swarm.ID), path, swarm.host.host, swarmPeerProvider{swarm})
	if err != nil {
		return nil
	}

	return &sharedStateService{swarm, rep, ReplicaAPI{}, ReplicaReadAPI{}}
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

func (self *sharedStateService) startup(bootstrap bool) error {

	//check if we need to bootstrap or join
	if bootstrap {
		err := self.rep.Bootstrap()
		if err != nil {
			return utils.StackError(err, "Unable to bootstrap replica")
		}

	}

	//setup API
	self.api = ReplicaAPI{self.rep}
	err := self.swarm.Rpc.Register(&self.api, AUTH_READWRITE)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica API")
	}
	self.rApi = ReplicaReadAPI{self.rep}
	err = self.swarm.Rpc.Register(&self.rApi, AUTH_READONLY)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica Read API")
	}

	return nil
}

func (self *sharedStateService) AddCommand(ctx context.Context, state string, cmd []byte) (interface{}, error) {

	if !self.IsRunning() {
		return nil, fmt.Errorf("State service not correctly setup: cannot add command")
	}
	
	//build the operation
	op := replica.NewOperation(state, cmd)

	//fetch leader and call him
	leader, err := self.rep.GetLeader(ctx)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get leader for state")
	}

	var ret interface{}
	err = self.swarm.Rpc.CallContext(ctx, leader, "ReplicaAPI", "AddCommand", op, &ret)
	return ret, err
}

//only callable by leader!
func (self *sharedStateService) AddPeer(ctx context.Context, id PeerID, auth AUTH_STATE) error {

	if self.rep == nil {
		return fmt.Errorf("No state available")
	}

	//fetch leader and check if we are
	leader, err := self.rep.GetLeader(ctx)
	if err != nil {
		return utils.StackError(err, "Unable to get leader for state")
	}
	if leader != self.swarm.host.ID().pid() {
		return fmt.Errorf("No is not leader, cannot add")
	}

	err = self.rep.AddPeer(ctx, id.pid(), auth == AUTH_READWRITE)
	if err != nil {
		return utils.StackError(err, "Unable to add peer to replica")
	}
	return nil
}

//only callable by leader!
func (self *sharedStateService) RemovePeer(ctx context.Context, id PeerID) error {

	if self.rep == nil {
		return fmt.Errorf("No state available")
	}

	//fetch leader and check if we are
	leader, err := self.rep.GetLeader(ctx)
	if err != nil {
		return utils.StackError(err, "Unable to get leader for state")
	}
	if leader != self.swarm.host.ID().pid() {
		return fmt.Errorf("No is not leader, cannot add")
	}

	err = self.rep.RemovePeer(ctx, id.pid())
	if err != nil {
		return utils.StackError(err, "Unable to remove peer from replica")
	}
	return nil
}

func (self *sharedStateService) Close() {

	if self.rep != nil {
		self.rep.Close()
	}
}
