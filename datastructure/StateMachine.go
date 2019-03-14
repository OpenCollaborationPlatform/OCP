package datastructure

import (
	"CollaborationNode/dml"
	"CollaborationNode/utils"
	"CollaborationNode/p2p"
	"bytes"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

//RAFT finite state machine
type stateMachine struct {
	//path which holds the datastores and dml files
	path  string
	
	//inmutable data
	swarm *p2p.Swarm
	
	//mutable runtime data
	dml   *dml.Runtime
	store *datastores.Datastore
	mutex sync.Mutex
}

func newStateMachine(path string, swarm *p2p.Swarm) (raft.FSM, error) {

	//create the datastore (autocreates the folder)
	store, err := datastores.NewDatastore(dir)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create datastore in statemaching")
	}

	//read in the file and create the runtime
	rntm := NewRuntime(store)
	file := filepath.Join(path, "Dml", "main.dml")
	filereader, err := os.Open(file)
	if err != nil {
		return nil, utils.StackError(err, "Unable to load dml file")
	}
	err = rntm.Parse(filereader)
	if err != nil {
		return nil, utils.StackError(err, "Unable to parse dml file")
	}
	
	return &stateMachine{path, swarm, rntm, store, sync.Mutex{}}
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the raft.FSM.
func (self *stateMachine) Apply(log *raft.Log) interface{} {

	//lock the state
	self.mutex.Lock()
	defer self.mutex.Unlock()

	//get the operation from the log entry
	op := operationFromData(log.Data)

	//apply to runtime
	return op.applyTo(self.dml)

}

// Snapshot is used to support log compaction. This call should
// return an raft.FSMSnapshot which can be used to save a point-in-time
// snapshot of the raft.FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the raft.FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (self *stateMachine) Snapshot() (raft.FSMSnapshot, error) {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//make sure the folder exists
	dir := filepath.Join(self.path, "Snapshot")
	err := os.MkdirAll(dir, os.ModePerm)

	//make the backup
	err := self.store.Backup(dir)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create snapshot from state-machine")
	}
	
	//generate the p2p descriptor
	file := filepath.Join(dir, "bolt.db")
	id, err := self.swarm.DistributeFile(file)
	if err != nil {
		return nil, utils.StackError(err, "Unable to make snapshot: cannot distribute file")
	}
	
	return stateSnapshot{id}, err
}

// Restore is used to restore an raft.FSM from a snapshot. It is not called
// concurrently with any other command. The raft.FSM must discard all previous
// state.
func (self *stateMachine) Restore(reader io.ReadCloser) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()
	
	//load the data
	var buffer bytes.Buffer
	_, err := buffer.ReadFrom(reader)
	reader.Close()

	if err != nil {
		return utils.StackError(err, "Uable to restore state: cannot read data")
	}

	//shutdown the runtime
	self.dml.

	//restore the state
	
}

type stateSnapshot struct {
	fileid id
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
func (self *stateSnapshot) Persist(sink raft.SnapshotSink) error {

	_, err := self.buffer.WriteTo(sink)

	if err != nil {
		sink.Cancel()
		return utils.StackError(err, "unable to persist snapshot")
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (self *stateSnapshot) Release() {
	self.buffer.Reset()
}
