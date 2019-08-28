package p2p

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/ickby/CollaborationNode/p2p/replica"
	"github.com/ickby/CollaborationNode/utils"
)

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

/******************************************************************************
							shared state service
*******************************************************************************/

type sharedStateService struct {
	swarm *Swarm
	rep   *replica.Replica
	api   ReplicaAPI
}

func newSharedStateService(swarm *Swarm) *sharedStateService {

	return &sharedStateService{swarm, nil, ReplicaAPI{}}
}

func (self *sharedStateService) Share(state replica.State) error {

	if self.rep != nil {
		return fmt.Errorf("State already shared")
	}

	//setup replica
	path := filepath.Join(self.swarm.GetPath())
	rep, err := replica.NewReplica(string(self.swarm.ID), path, self.swarm.host.host, state)
	if err != nil {
		return utils.StackError(err, "unable to add state to swarm")
	}
	self.rep = rep

	//setup API
	self.api = ReplicaAPI{rep}
	err = self.swarm.Rpc.Register(&self.api, AUTH_READWRITE)
	if err != nil {
		return utils.StackError(err, "Unable to register Replica API")
	}

	return nil
}

func (self *sharedStateService) AddCommand(ctx context.Context, cmd []byte) (interface{}, error) {

	if self.rep == nil {
		return nil, fmt.Errorf("No state with the given name available")
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

func (self *sharedStateService) Close() {

	self.rep.Close()
}
