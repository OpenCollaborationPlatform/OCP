package p2p

import (
	raft "github.com/hashicorp/raft"
)

type sharedState struct {
	raftServer *raft.Raft
}

type sharedStateService struct {
	swarm *Swarm
}

func newSharedStateService(swarm *Swarm) *sharedStateService {

	return &sharedStateService{swarm}
}

func (self *sharedStateService) Share(fsm raft.FSM, isRoot bool) (*sharedState, error) {
	/*
		//the name used to distuinguish is the class name of the fsm
		value := reflect.ValueOf(fsm)
		fsmname := reflect.Indirect(value).Type().Name()

		//the p2p transport
		transport, err := NewLibp2pTransport(self.swarm.host.host, self.swarm.ID.Pretty()+`/`+fsmname, time.Minute)
		if err != nil {
			return nil, utils.StackError(err, "Unable to load new transport")
		}

		//log and conf store
		path := filepath.Join(self.swarm.GetPath(), fsmname+`-logstore.db`)
		logstore, err := boltstore.NewBoltStore(path)
		if err != nil {
			return nil, utils.StackError(err, "Unable to load logstore")
		}

		//snapshot store: for now the default file one (change to bolt, as we do not
		//store much data)
		path = filepath.Join(self.swarm.GetPath(), fsmname+`-snapshots`)
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
		//config.LogOutput = ioutil.Discard
		config.Logger = nil
		config.LocalID = raft.ServerID(self.swarm.host.ID().pid().Pretty())
		config.StartAsLeader = false

		//Bootstrap if required
		var clusterConfig raft.Configuration
		if isRoot {
			servers := []raft.Server{
				raft.Server{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(self.swarm.host.ID().pid().Pretty()),
					Address:  raft.ServerAddress(self.swarm.host.ID().pid().Pretty()),
				},
			}
			clusterConfig = raft.Configuration{servers}

		} else {
			clusterConfig = raft.Configuration{}
		}

		raft.BootstrapCluster(config, logstore, logstore, snapstore, transport, clusterConfig.Clone())

		//start up
		raftserver, err := raft.NewRaft(config, fsm, logstore, logstore, snapstore, transport)
		if err != nil {
			return nil, utils.StackError(err, "Unable to startup raft server")
		}

		return &sharedState{raftserver}, nil*/
	return nil, nil
}
