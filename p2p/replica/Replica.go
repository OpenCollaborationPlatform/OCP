package replica

import (
	"io/ioutil"
	"context"
	"fmt"

	//"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb"
	"github.com/ickby/CollaborationNode/utils"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

//a interface that allows to query all available peers
//****************************************************
type PeerProvider interface {
	HasPeer(peer.ID) bool
	CanWrite(peer.ID) (bool, error)
	GetReadPeers() []peer.ID
	GetWritePeers() []peer.ID
}

type Replica struct {
	host      p2phost.Host
	state     multiState
	rep       *raft.Raft
	logs      raft.LogStore
	confs     raft.StableStore
	snaps     raft.SnapshotStore
	name      string
}

func NewReplica(name string, path string, host p2phost.Host) (*Replica, error) {

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

	//setup the state
	state := newMultiState()

	return &Replica{host, state, nil, logStore, stableStore, snapshots, name}, nil
}

//starts the replica and waits to get added by the leader
//(to be done after all states are added)
func (self *Replica) Join() error {

	if self.rep != nil {
		return fmt.Errorf("Replica already initialized")
	}

	// Create LibP2P transports Raft
	transport, err := NewLibp2pTransport(self.host, 2*time.Second, self.name)
	if err != nil {
		return err
	}

	//build up the config
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(self.host.ID().Pretty())
	config.LogOutput = ioutil.Discard
	config.Logger = nil

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, self.state, self.logs, self.confs, self.snaps, transport)
	if err != nil {
		return utils.StackError(err, "Unable to initialize replica")
	}

	self.rep = ra
	return nil
}

//setup the replica completely new (to be done after all states are added)
func (self *Replica) Bootstrap() error {

	//build the raft instance
	err := self.Join()
	if err != nil {
		return err
	}
	peerDec :=  peer.IDB58Encode(self.host.ID())
	serverconf := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(peerDec),
				Address:  raft.ServerAddress(peerDec),
			},
		},
	}
	future := self.rep.BootstrapCluster(serverconf)
	return future.Error()
}

func (self *Replica) IsRunning() bool {

	return self.rep != nil
}

//warning: only close after removing from the cluster!
func (self *Replica) Close() error {

	if self.rep != nil {
		future := self.rep.Shutdown()
		return future.Error()
	}
	return nil
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

func (self *Replica) ConnectPeer(ctx context.Context, pid peer.ID, writer bool) error {

	duration := durationFromContext(ctx)
	peerDec := peer.IDB58Encode(pid)

	//run till success or cancelation
	for {
		var future raft.IndexFuture
		if writer {
			future = self.rep.AddVoter(raft.ServerID(peerDec), raft.ServerAddress(peerDec), 0, duration)
		} else {
			future = self.rep.AddVoter(raft.ServerID(peerDec), raft.ServerAddress(peerDec), 0, duration)
		}
		err := future.Error()
		if err == nil {
			return nil
		}
		if err != raft.ErrEnqueueTimeout {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("Timeout while adding peer")
		default:
			break
		}
	}

	return fmt.Errorf("Something bad happend")
}

func (self *Replica) DisconnectPeer(ctx context.Context, pid peer.ID) error {

	duration := durationFromContext(ctx)
	peerDec := peer.IDB58Encode(pid)

	//run till success or cancelation
	for {
		future := self.rep.RemoveServer(raft.ServerID(peerDec), 0, duration)
		err := future.Error()
		if err == nil {
			return nil
		}
		if err != raft.ErrEnqueueTimeout {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("Timeout while adding peer")
		default:
			break
		}
	}

	return fmt.Errorf("Something bad happend")
}

func (self *Replica) AddCommand(ctx context.Context, op Operation) (interface{}, error) {

	duration := durationFromContext(ctx)

	//run till success or cancelation
	for {
		future := self.rep.Apply(op.ToBytes(), duration)
		err := future.Error()
		if err == nil {
			return future.Response(), nil
		}
		if err != raft.ErrEnqueueTimeout {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("Timeout on command processing")
		default:
			break
		}
	}

	return nil, fmt.Errorf("Something bad happend")
}

func (self *Replica) AddState(name string, state State) error {

	if self.rep != nil {
		return fmt.Errorf("Replica already initialized, cannot add state")
	}

	return self.state.Add(name, state)
}

func durationFromContext(ctx context.Context) time.Duration {

	deadline, hasDeadline := ctx.Deadline()
	var duration time.Duration
	if hasDeadline {
		duration = deadline.Sub(time.Now())

	} else {
		duration = 1000 * time.Millisecond
	}
	return duration
}
