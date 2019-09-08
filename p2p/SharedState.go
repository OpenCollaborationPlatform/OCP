package p2p

import (
	"context"
	"fmt"
	"path/filepath"
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

func (self *ReplicaAPI) AddCommand(ctx context.Context, cmd []byte, ret *interface{}) error {

	value, err := self.rep.AddCommand(ctx, cmd)
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
	path := filepath.Join(self.swarm.GetPath())
	rep, err := replica.NewReplica(string(self.swarm.ID), path, self.swarm.host.host, state, swarmPeerProvider{self.swarm})
	if err != nil {
		return utils.StackError(err, "unable to add state to swarm")
	}
	self.rep = rep


	return &sharedStateService{swarm, rep, ReplicaAPI{}, ReplicaReadAPI{}}
}

func (self *sharedStateService) Share(state replica.State) error {

	//get the name
	var name string
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
        name = t.Elem().Name()
    } else {
        name = t.Name()
    }
    
    //add to replica
    self.rep.AddState(name, state)
}

func (self *sharedStateService) Startup() error {
	
	//check if we need to bootstrap or join
	peers := self.swarm.GetPeers(AUTH_READWRITE)
	if len(peers) == 0 {
		err := self.rep.Bootstrap(self.swarm.host.host 
		if err != nil {
			return utils.StackError(err, "Unable to bootstrap replica")
		}

	} else {
		err := self.rep.Join()
		if err != nil {
			return utils.StackError(err, "Unable to startup replica")
		}
		
		peers = self.swarm.GetPeers(AUTH_NONE)
		go func() {
			//get leader and inform him that we want to participate
			var leader peer.ID
			for _, peer := range peers {
				ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
				err := self.swarm.Rpc.CallContext(ctx, peer.pid(), "ReplicaReadAPI", "GetLeader", struct{}{}, &leader)
				if err == nil {
					break
				}
			}
			if leader == peer.ID("") {
				return
			}

			//inform him
			ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
			var ret struct{}
			self.swarm.Rpc.CallContext(ctx, leader, "ReplicaReadAPI", "EnsureParticipation", self.swarm.host.ID(), &ret)
			return
		}()
	}

	//setup API
	self.api = ReplicaAPI{rep}
	err = self.swarm.Rpc.Register(&self.api, AUTH_READWRITE)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica API")
	}
	self.rApi = ReplicaReadAPI{rep}
	err = self.swarm.Rpc.Register(&self.rApi, AUTH_READONLY)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica Read API")
	}

	return nil
}

func (self *sharedStateService) AddCommand(ctx context.Context, cmd []byte) (interface{}, error) {

	if self.rep == nil {
		return nil, fmt.Errorf("No state available")
	}

	//fetch leader and call him
	leader, err := self.rep.GetLeader(ctx)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get leader for state")
	}

	var ret interface{}
	err = self.swarm.Rpc.CallContext(ctx, leader, "ReplicaAPI", "AddCommand", cmd, &ret)
	return ret, err

}

//only callable by leader!
func (self *sharedStateService) AddPeer(ctx context.Context, id PeerID, auth AUTH_STATE) (error) {

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
func (self *sharedStateService) RemovePeer(ctx context.Context, id PeerID) (error) {

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
