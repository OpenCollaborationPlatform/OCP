package p2p

import (
	"CollaborationNode/utils"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"time"

	raft "github.com/hashicorp/raft"
	boltstore "github.com/hashicorp/raft-boltdb"
)

type sharedState struct {
	raftServer *raft.Raft
}

func newSharedState(swarm *Swarm, fsm raft.FSM) (*sharedState, error) {

	//the name used to distuinguish is the class name of the fsm
	value := reflect.ValueOf(fsm)
	name := swarm.ID.Pretty() + `/` + reflect.Indirect(value).Type().Name()

	//the p2p transport
	transport, err := NewLibp2pTransport(swarm.host.host, name, time.Minute)
	if err != nil {
		return nil, utils.StackError(err, "Unable to load new transport")
	}

	//log and conf store
	path := filepath.Join(swarm.GetPath(), name, `-logstore.db`)
	logstore, err := boltstore.NewBoltStore(path)
	if err != nil {
		return nil, utils.StackError(err, "Unable to load logstore")
	}

	//snapshot store: for now the default file one (change to bolt, as we do not
	//store much data)
	path = filepath.Join(swarm.GetPath(), name, `-snapshots`)
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create director for snapshotstore")
	}
	snapstore, err := raft.NewFileSnapshotStore(path, 3, ioutil.Discard)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create snapshotstore")
	}

	//config
	config := raft.DefaultConfig()
	config.LogOutput = ioutil.Discard
	config.Logger = nil
	config.LocalID = raft.ServerID(swarm.host.ID().pid().Pretty())

	//start up
	raftserver, err := raft.NewRaft(config, fsm, logstore, logstore, snapstore, transport)
	if err != nil {
		return nil, utils.StackError(err, "Unable to startup raft server")
	}

	return &sharedState{raftserver}, nil
}
