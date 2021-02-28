package replica

import (
	"context"
	"fmt"
	"io/ioutil"

	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb"
	"github.com/ickby/CollaborationNode/utils"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
)

type Event_Type int

const (
	EVENT_ADDED Event_Type = iota
	EVENT_REMOVED
	EVENT_LEADER
)

type ReplicaPeerEvent struct {
	Peer  peer.ID
	Event Event_Type
}

type Replica struct {
	host  p2phost.Host
	dht   *kaddht.IpfsDHT
	state *multiState
	rep   *raft.Raft
	obs   *raft.Observer
	obsCh chan raft.Observation
	evtCh chan ReplicaPeerEvent
	logs  raft.LogStore
	confs raft.StableStore
	snaps raft.SnapshotStore
	name  string
}

func NewReplica(name string, path string, host p2phost.Host, dht *kaddht.IpfsDHT) (*Replica, error) {

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(path, 3, os.Stderr)
	if err != nil {
		return nil, wrapInternalError(err, Error_Setup)
	}

	// Create the log store and stable store.
	opts := raftbolt.Options{
		NoSync: false,
		Path:   filepath.Join(path, "raft.db"),
		BoltOptions: &bolt.Options{
			Timeout:    1 * time.Second,
			NoGrowSync: false,
		},
	}
	boltDB, err := raftbolt.New(opts)
	if err != nil {
		return nil, wrapInternalError(err, Error_Setup)
	}
	logStore := raft.LogStore(boltDB)
	stableStore := raft.StableStore(boltDB)

	//setup the state
	state := newMultiState()

	return &Replica{host, dht, state, nil, nil, nil, nil,
		logStore, stableStore, snapshots, name}, nil
}

//starts the replica and waits to get added by the leader
//(to be done after all states are added)
func (self *Replica) Join() error {

	if self.rep != nil {
		return newInternalError(Error_Setup, "Replica already initialized")
	}

	// Create LibP2P transports Raft
	transport, err := NewLibp2pTransport(self.host, self.dht, 2*time.Second, self.name)
	if err != nil {
		return utils.StackError(err, "Unable to create transport")
	}

	//build up the config
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(self.host.ID().Pretty())
	config.LogOutput = ioutil.Discard
	config.Logger = nil

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, self.state, self.logs, self.confs, self.snaps, transport)
	if err != nil {
		return wrapInternalError(err, Error_Setup)
	}

	//setup the default observers
	self.evtCh = make(chan ReplicaPeerEvent, 10)
	self.obsCh = make(chan raft.Observation, 10)
	self.obs = raft.NewObserver(self.obsCh, true, nil)
	ra.RegisterObserver(self.obs)
	go self.observationLoop()

	self.rep = ra
	return nil
}

//setup the replica completely new (to be done after all states are added)
func (self *Replica) Bootstrap() error {

	//build the raft instance
	err := self.Join()
	if err != nil {
		return utils.StackError(err, "Unable to create basic replica")
	}
	peerDec := peer.IDB58Encode(self.host.ID())
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
	return wrapConnectionError(future.Error(), Error_Setup)
}

func (self *Replica) IsRunning() bool {

	if self.rep == nil {
		return false
	}

	return self.rep.State() != raft.Shutdown
}

//warning: only close after removing ourself from the cluster!
func (self *Replica) Close(ctx context.Context) error {

	if self.rep != nil {

		//close the stores after shutdown
		defer func() {
			store, _ := self.logs.(*raftbolt.BoltStore)
			store.Close()
			self.rep = nil
		}()

		//remove the observers
		self.rep.DeregisterObserver(self.obs)
		close(self.obsCh)
		close(self.evtCh)

		//We shut down automatically when we get removed from conf (due to default conf option)
		//all this function does is to wait till we are shutdown to make sure we are
		//stil lavailable for the remove conf change

		obsCh := make(chan raft.Observation, 1)
		observer := raft.NewObserver(obsCh, false, nil)
		self.rep.RegisterObserver(observer)
		defer self.rep.DeregisterObserver(observer)

		//check after register observer just to make sure we do not miss a change
		if self.rep.State() == raft.Shutdown {
			return nil
		}

		for {
			select {
			case obs := <-obsCh:
				switch obs.Data.(type) {
				case raft.RaftState:
					state := obs.Data.(raft.RaftState)
					if state == raft.Shutdown {
						return nil
					}
				}
			case <-ctx.Done():
				//We need to force the shutdown!
				self.rep.Shutdown()
			}
		}
	}

	return nil
}

//shutdown the cluster. Only use when we are the last peer! otherwise remove
//ourself from the cluster and call close!
func (self *Replica) Shutdown(ctx context.Context) error {

	if self.IsRunning() {
		shut := self.rep.Shutdown()
		err := shut.Error()
		if err != nil {
			return wrapInternalError(err, Error_Process)
		}
		return self.Close(ctx)
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
						p, err := peer.IDB58Decode(string(self.rep.Leader()))
						return p, wrapInternalError(err, Error_Invalid_Data)
					}
				}
			case <-ctx.Done():
				return peer.ID(""), newConnectionError(Error_Process, "Timed out while waiting for leader")
			}
		}
	}

	pid, err := peer.IDB58Decode(string(leader))
	if err != nil {
		return pid, wrapInternalError(err, Error_Invalid_Data)
	}
	return pid, nil
}

func (self *Replica) ConnectPeer(ctx context.Context, pid peer.ID, writer bool) error {

	duration := durationFromContext(ctx)
	peerDec := peer.IDB58Encode(pid)

	//run till success or cancelation
	for {
		var future raft.IndexFuture

		//currently we do not use nonvoter. Reason: If read only would be a non voter
		//than the shared state replication would rely on ReadWrite peers only. As we expect
		//small amount of nodes sharing a state this would lead to problems, as easily only
		//non voters could remain. That seems counter intuitive for the user.
		// - read peers cannot add commands, as they are not allowed to use ReplicaApi
		// - A maliscious READ node coud reprogram the code and add faulty commands when it the leader
		// - A real public sharing would benefit from non voter
		//if writer {
		future = self.rep.AddVoter(raft.ServerID(peerDec), raft.ServerAddress(peerDec), 0, duration)
		//} else {
		//	future = self.rep.AddNonvoter(raft.ServerID(peerDec), raft.ServerAddress(peerDec), 0, duration)
		//}

		err := future.Error()
		if err == nil {
			return nil
		}
		if err != raft.ErrEnqueueTimeout {
			return wrapInternalError(err, Error_Process)
		}

		select {
		case <-ctx.Done():
			return newConnectionError(Error_Process, "Timeout while adding peer")
		default:
			break
		}
	}

	return newConnectionError(Error_Process, "Something bad happend")
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
			return wrapInternalError(err, Error_Process)
		}

		select {
		case <-ctx.Done():
			return newConnectionError(Error_Process, "Timeout while adding peer")
		default:
			break
		}
	}

	return newConnectionError(Error_Process, "Something bad happend")
}

func (self *Replica) IsLastPeer(pid peer.ID) (bool, error) {

	future := self.rep.GetConfiguration()
	err := future.Error()
	if err != nil {
		return false, wrapInternalError(err, Error_Process)
	}

	conf := future.Configuration()

	if len(conf.Servers) != 1 {
		return false, nil
	}

	server := conf.Servers[0].ID
	serverpid, err := peer.IDB58Decode(string(server))
	if err != nil {
		return false, wrapInternalError(err, Error_Invalid_Data)
	}

	return pid == serverpid, nil
}

func (self *Replica) AddCommand(ctx context.Context, op Operation) (interface{}, error) {

	duration := durationFromContext(ctx)

	//track application of op
	doneC := make(chan struct{}, 1)
	self.state.SetDoneChan(op.OpID, doneC)

	//run till success or cancelation
	for {
		future := self.rep.Apply(op.ToBytes(), duration)
		err := future.Error()
		if err != nil && err != raft.ErrEnqueueTimeout {
			return nil, wrapInternalError(err, Error_Process)
		}

		select {
		case <-doneC:
			return future.Response(), nil

		case <-ctx.Done():
			return nil, newConnectionError(Error_Process, "Timeout on command processing")
		default:
			break
		}
	}

	return nil, newConnectionError(Error_Process, "Something bad happend")
}

func (self *Replica) AddState(name string, state State) error {

	if self.rep != nil {
		return newInternalError(Error_Setup, "Replica already initialized, cannot add state")
	}

	return self.state.Add(name, state)
}

func (self *Replica) PrintConf() {

	if self.rep == nil {
		fmt.Println("Replica not startet up")
		return
	}

	future := self.rep.GetConfiguration()
	err := future.Error()
	if err != nil {
		fmt.Println("Error in configuration")
		return
	}

	l := self.rep.Leader()
	if l != "" {
		l = l[len(l)-4:]
	}

	conf := future.Configuration()
	n := self.host.ID().Pretty()
	n = string(n[len(n)-4:])
	fmt.Printf("Configuration of node %v with leader %v: \n", n, l)
	for _, server := range conf.Servers {
		str := string(server.ID)
		if server.Suffrage == raft.Voter {
			str = str + ": Voter"

		} else {
			str = str + ": Nonvoter"
		}
		fmt.Println(str)
	}
}

func (self *Replica) ConnectedPeers() ([]peer.ID, error) {

	if self.rep == nil {
		return nil, newInternalError(Error_Setup, "Replica not startet up")
	}

	future := self.rep.GetConfiguration()
	err := future.Error()
	if err != nil {
		return nil, wrapInternalError(err, Error_Process)
	}

	conf := future.Configuration()
	result := make([]peer.ID, 0)
	for _, server := range conf.Servers {
		serverpid, err := peer.IDB58Decode(string(server.ID))
		if err != nil {
			continue
		}
		result = append(result, serverpid)
	}
	return result, nil
}

func (self *Replica) EventChannel() chan ReplicaPeerEvent {
	return self.evtCh
}

func (self *Replica) observationLoop() {

	//stops when obsCh is closed
	for obs := range self.obsCh {

		switch obs := obs.Data.(type) {

		case raft.LeaderObservation:
			pid, err := peer.IDB58Decode(string(obs.Leader))
			if err != nil {
				continue
			}
			self.evtCh <- ReplicaPeerEvent{pid, EVENT_LEADER}

		case raft.PeerObservation:
			pid, err := peer.IDB58Decode(string(obs.Peer.Address))
			if err != nil {
				continue
			}
			evtType := EVENT_ADDED
			if obs.Removed {
				evtType = EVENT_REMOVED
			}
			self.evtCh <- ReplicaPeerEvent{pid, evtType}
		}
	}
}

func durationFromContext(ctx context.Context) time.Duration {

	deadline, hasDeadline := ctx.Deadline()
	var duration time.Duration
	if hasDeadline {
		duration = deadline.Sub(time.Now())

	} else {
		duration = 2000 * time.Millisecond
	}
	return duration
}
