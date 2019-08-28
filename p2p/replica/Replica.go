package replica

import (
	"context"
	"fmt"

	//"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

//States are just raft states in a new name
type State = raft.FSM
type Snapshot = raft.FSMSnapshot
type SnapshotSink = raft.SnapshotSink
type Log = raft.Log

type Replica struct {
	rep   *raft.Raft
	logs  raft.LogStore
	confs raft.StableStore
	snaps raft.SnapshotStore
}

func NewReplica(name string, path string, host p2phost.Host, state State) (*Replica, error) {

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(path, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	boltDB, err := raftbolt.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("new bolt store: %s", err)
	}
	logStore := raft.LogStore(boltDB)
	stableStore := raft.StableStore(boltDB)

	// -- Create LibP2P transports Raft
	transport, err := NewLibp2pTransport(host, 2*time.Second, name)
	if err != nil {
		return nil, err
	}

	//build up the config
	config := raft.DefaultConfig()
	//config.LogOutput = ioutil.Discard
	//config.Logger = nil
	config.LocalID = raft.ServerID(host.ID().Pretty())

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, state, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}
	serverconf := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(host.ID().Pretty()),
				Address:  raft.ServerAddress(host.ID().Pretty()),
			},
		},
	}
	ra.BootstrapCluster(serverconf)

	return &Replica{ra, logStore, stableStore, snapshots}, nil
}

func (self *Replica) Close() error {
	future := self.rep.Shutdown()
	return future.Error()
}

func (self *Replica) GetLeader(ctx context.Context) (peer.ID, error) {

	leader := self.rep.Leader()
	if leader == "" {

		obsCh := make(chan raft.Observation, 1)
		observer := raft.NewObserver(obsCh, false, nil)
		self.rep.RegisterObserver(observer)
		defer self.rep.DeregisterObserver(observer)

		for {
			select {
			case obs := <-obsCh:
				switch obs.Data.(type) {
				case raft.RaftState:
					if self.rep.Leader() != "" {
						return peer.IDB58Decode(string(self.rep.Leader()))
					}
				}
			case <-ctx.Done():
				return peer.ID(""), fmt.Errorf("Timed out while waiting for leader")
			}
		}
	}

	pid, err := peer.IDB58Decode(string(leader))
	if err != nil {
		return pid, err
	}
	return pid, nil
}

func (self *Replica) AddCommand(ctx context.Context, cmd []byte) (interface{}, error) {

	deadline, hasDeadline := ctx.Deadline()
	var duration time.Duration
	if hasDeadline {
		duration = deadline.Sub(time.Now())

	} else {
		duration = 2 * time.Second
	}

	future := self.rep.Apply(cmd, duration)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return future.Response(), nil
}
