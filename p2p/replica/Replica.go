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

//a interface that allows to query all available peers
//****************************************************
type PeerProvider interface {
	HasPeer(peer.ID) bool
	CanWrite(peer.ID) (bool, error)
	GetReadPeers() []peer.ID
	GetWritePeers() []peer.ID
}

//States are just raft states in a new name
type State = raft.FSM
type Snapshot = raft.FSMSnapshot
type SnapshotSink = raft.SnapshotSink
type Log = raft.Log

type Replica struct {
	rep       *raft.Raft
	logs      raft.LogStore
	confs     raft.StableStore
	snaps     raft.SnapshotStore
	pprovider PeerProvider
}

func NewReplica(name string, path string, host p2phost.Host, state State, provider PeerProvider) (*Replica, error) {

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

	return &Replica{ra, logStore, stableStore, snapshots, provider}, nil
}

func (self *Replica) Bootstrap(host p2phost.Host) {
	serverconf := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(host.ID().Pretty()),
				Address:  raft.ServerAddress(host.ID().Pretty()),
			},
		},
	}
	self.rep.BootstrapCluster(serverconf)
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

func (self *Replica) AddPeer(ctx context.Context, peer peer.ID, writer bool) error {

	duration := durationFromContext(ctx)

	//run till success or cancelation
	for {
		var future raft.IndexFuture
		if writer {
			future = self.rep.AddVoter(raft.ServerID(peer.Pretty()), raft.ServerAddress(peer.Pretty()), 0, duration)
		} else {
			future = self.rep.AddVoter(raft.ServerID(peer.Pretty()), raft.ServerAddress(peer.Pretty()), 0, duration)
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

func (self *Replica) RemovePeer(ctx context.Context, peer peer.ID) error {

	duration := durationFromContext(ctx)

	//run till success or cancelation
	for {
		future := self.rep.RemoveServer(raft.ServerID(peer.Pretty()), 0, duration)
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

func (self *Replica) EnsureParticipation(ctx context.Context, id peer.ID) error {

	duration := durationFromContext(ctx)

	if !self.pprovider.HasPeer(id) {
		return fmt.Errorf("Peer is not allowed to participate")
	}

	for {
		var future raft.IndexFuture
		if res, _ := self.pprovider.CanWrite(id); res {
			future = self.rep.AddVoter(raft.ServerID(id.Pretty()), raft.ServerAddress(id.Pretty()), 0, duration)
		} else {
			future = self.rep.AddNonvoter(raft.ServerID(id.Pretty()), raft.ServerAddress(id.Pretty()), 0, duration)
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
			return fmt.Errorf("Timeout on ensuring peer")
		default:
			break
		}
	}

	return nil
}

func (self *Replica) AddCommand(ctx context.Context, cmd []byte) (interface{}, error) {

	duration := durationFromContext(ctx)

	//run till success or cancelation
	for {
		future := self.rep.Apply(cmd, duration)
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

/****************************************************************************
						internal
****************************************************************************/

func (self *Replica) runConfLoop() {

	select {
	case isLeader := <-self.rep.LeaderCh():
		if isLeader {
			self.resolveConfDifferences()
		}
	}
}

//check if the configuration matches the PeerProvider and update if not
//must only be run when the replica is leader!
func (self *Replica) resolveConfDifferences() {

	//get the configuration
	future := self.rep.GetConfiguration()
	if future.Error() != nil {
		return
	}
	conf := future.Configuration()

	//create a map of peers: bool states if they are write or read
	serverMap := make(map[peer.ID]bool, len(conf.Servers))
	for _, server := range conf.Servers {

		pid, err := peer.IDB58Decode(string(server.Address))
		if err != nil {
			continue
		}
		write := server.Suffrage == raft.Voter
		serverMap[pid] = write
	}

	//check if we have all write and read peers in the conf and connect if not
	for _, writer := range self.pprovider.GetWritePeers() {

		auth, has := serverMap[writer]
		if !has || !auth {
			self.rep.AddVoter(raft.ServerID(writer.Pretty()), raft.ServerAddress(writer.Pretty()), 0, 0)
		}
	}
	for _, reader := range self.pprovider.GetReadPeers() {

		auth, has := serverMap[reader]
		if !has {
			self.rep.AddNonvoter(raft.ServerID(reader.Pretty()), raft.ServerAddress(reader.Pretty()), 0, 0)
			continue
		}
		if auth {
			self.rep.DemoteVoter(raft.ServerID(reader.Pretty()), 0, 0)
		}
	}

	//check if the conv has more entries than the peers
	allpeers := make([]peer.ID, len(self.pprovider.GetWritePeers()))
	copy(allpeers, self.pprovider.GetWritePeers())
	allpeers = append(allpeers, self.pprovider.GetReadPeers()...)
	if len(allpeers) != len(serverMap) {

		for server, _ := range serverMap {

			remove := true
			for _, peer := range allpeers {
				if peer == server {
					remove = false
					break
				}
			}

			if remove {
				self.rep.RemoveServer(raft.ServerID(server.Pretty()), 0, 0)
			}
		}
	}
}

func durationFromContext(ctx context.Context) time.Duration {

	deadline, hasDeadline := ctx.Deadline()
	var duration time.Duration
	if hasDeadline {
		duration = deadline.Sub(time.Now())

	} else {
		duration = 500 * time.Millisecond
	}
	return duration
}
