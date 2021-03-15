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

type Replica struct {
	host  p2phost.Host
	dht   *kaddht.IpfsDHT
	state *multiState
	rep   *raft.Raft
	obs   *raft.Observer
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

	return &Replica{host, dht, state, nil, nil,
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
	config.ShutdownOnRemove = true

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, self.state, self.logs, self.confs, self.snaps, transport)
	if err != nil {
		return wrapInternalError(err, Error_Setup)
	}

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

func (self *Replica) cleanup() {

	//close the stores
	store, _ := self.logs.(*raftbolt.BoltStore)
	store.Close()
	self.rep = nil
}

//warning: only close after removing ourself from the cluster!
func (self *Replica) Close(ctx context.Context) error {

	//Shutdown idea:
	//1. remove from large cluster so that we are a single peer cluster (user responsibility)
	//2. remove ourself from the one peer cluster (happens in this function)
	//3. wait till automatic shutdown occured

	if self.rep != nil {

		//internal states cleanup
		defer self.cleanup()

		//We shut down automatically when we get removed from conf (due to default conf option)
		//all this function does is to wait till we are shutdown to make sure we are
		//stil lavailable for the remove conf change
		obsCh := make(chan raft.Observation, 1)
		observer := raft.NewObserver(obsCh, false, nil)
		self.rep.RegisterObserver(observer)
		defer close(obsCh)
		defer self.rep.DeregisterObserver(observer)

		//remove ourself as last peer!
		peerDec := peer.IDB58Encode(self.host.ID())
		rem := self.rep.RemovePeer(raft.ServerAddress(peerDec))
		err := rem.Error()
		if err != nil {
			//failed, need hard shutdown
			shut := self.rep.Shutdown()
			return shut.Error()
		}

		//check after register observer just to make sure we do not miss a change
		if self.rep.State() == raft.Shutdown {
			return nil
		}

		fmt.Printf("\nwait for shutdown while being %v: %v\n", self.rep.State().String(), self.host.ID().Pretty())
		for {
			select {
			case obs := <-obsCh:
				switch state := obs.Data.(type) {
				case raft.RaftState:
					fmt.Printf("\nState changed: %v\n", state.String())
					if state == raft.Shutdown {
						return nil
					}
				}
			case <-ctx.Done():
				fmt.Printf("timeout\n")
				//We need to force the shutdown!
				shut := self.rep.Shutdown()
				err := shut.Error()
				if err != nil {
					return wrapInternalError(err, Error_Process)
				}
			}
		}
	}

	return nil
}

//shutdown the cluster. Only use when we are the last peer! otherwise remove
//ourself from the cluster and call close!
func (self *Replica) Shutdown(ctx context.Context) error {

	if self.IsRunning() {

		//internal states cleanup
		defer self.cleanup()

		shut := self.rep.Shutdown()
		err := shut.Error()
		if err != nil {
			return wrapInternalError(err, Error_Process)
		}
	}
	return nil
}

func (self *Replica) GetLeader(ctx context.Context) (peer.ID, error) {

	obsCh := make(chan raft.Observation, 1)
	observer := raft.NewObserver(obsCh, false, nil)
	self.rep.RegisterObserver(observer)
	defer close(obsCh)
	defer self.rep.DeregisterObserver(observer)

	leader := self.rep.Leader()
	if leader == "" {
		for {
			select {
			case obs := <-obsCh:
				switch o := obs.Data.(type) {
				case raft.LeaderObservation:
					pid, err := peer.IDB58Decode(string(o.Leader))
					if err != nil {
						return peer.ID(""), wrapInternalError(err, Error_Invalid_Data)
					}
					return pid, nil
				default:
					continue
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

func (self *Replica) TransferLeadership(ctx context.Context) (peer.ID, error) {

	obsCh := make(chan raft.Observation, 1)
	observer := raft.NewObserver(obsCh, false, nil)
	self.rep.RegisterObserver(observer)
	defer close(obsCh)
	defer self.rep.DeregisterObserver(observer)

	transfer := self.rep.LeadershipTransfer()
	err := transfer.Error()
	if err != nil {
		return peer.ID(""), newInternalError(Error_Process, "Unable to transfer leadership before removing self as leader")
	}
	//wait for new leader
	for {
		select {
		case obs := <-obsCh:
			switch o := obs.Data.(type) {
			case raft.LeaderObservation:
				pid, err := peer.IDB58Decode(string(o.Leader))
				if err != nil {
					continue
				}
				return pid, err
			default:
				continue
			}
		case <-ctx.Done():
			return peer.ID(""), newConnectionError(Error_Process, "Timeout while leadership transfer")
		}
	}
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

func (self *Replica) CreateResultChannel(opID string) (chan interface{}, error) {

	//track application of op
	return self.state.CreateResultChan(opID)
}

func (self *Replica) CloseResultChan(opID string) {

	//track application of op
	self.state.CloseResultChan(opID)
}

func (self *Replica) AddCommand(ctx context.Context, op Operation) error {

	duration := durationFromContext(ctx)

	//rerun the op till ctx closes, max 3 retries (to not stall forever)
	for i := 0; i < 3; i++ {
		future := self.rep.Apply(op.ToBytes(), duration)
		err := future.Error()
		//no error means success
		if err == nil {
			return nil
		}
		//error other than timeout is not recoverable
		if err != raft.ErrEnqueueTimeout {
			return wrapInternalError(err, Error_Process)
		}

		//in case the context has no timeout raft used the default one. This could
		//mean we are here without context being canceled yet... check for that
		select {
		case <-ctx.Done():
			return newConnectionError(Error_Process, "Command processing canceled")
		default:
			break
		}
	}

	return newConnectionError(Error_Process, "Unable to reach majority of nodes")
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

func (self *Replica) HasMajority() bool {
	state := self.rep.State()
	if state == raft.Candidate {
		return false
	}
	return true
}

func durationFromContext(ctx context.Context) time.Duration {

	deadline, hasDeadline := ctx.Deadline()
	var duration time.Duration
	if hasDeadline {
		duration = deadline.Sub(time.Now())

	} else {
		duration = 3000 * time.Millisecond
	}
	return duration
}
