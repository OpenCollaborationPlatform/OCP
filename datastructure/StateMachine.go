package datastructure

import (
	"CollaborationNode/datastores"
	"CollaborationNode/dml"
	"CollaborationNode/p2p"
	"CollaborationNode/utils"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
)

//shared state data structure
type state struct {
	//path which holds the datastores and dml files
	path string

	//store snapshots
	data p2p.DataService

	//runtime data
	dml             *dml.Runtime
	store           *datastore.Datastore
	operationNumber uint64
	mutex           sync.Mutex
}

type stateSnapshot struct {
	file            cid.Cid
	operationNumber uint64
}

func (self stateSnapshot) toByte() []byte {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(self)
	if err != nil {
		return nil
	}
	return b.Bytes()
}

func snapshotFromBytes(data []byte) (stateSnapshot, error) {
	var snap stateSnapshot
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&snap)
	return snap, err
}

/*
type State interface {

	//manipulation
	Apply([]byte) error //apply a command to change the state
	Reset() error       //reset state to initial value without any apply

	//snapshoting
	Snapshot() ([]byte, error)   //crete a snapshot from current state
	LoadSnapshot([]byte) error   //setup state according to snapshot
	EnsureSnapshot([]byte) error //make sure this snapshot represents the current state
}*/

func newState(path string, data p2p.DataService) (state, error) {

	//create the datastore (autocreates the folder)
	//path/Datastore
	store, err := datastore.NewDatastore(path)
	if err != nil {
		return state{}, utils.StackError(err, "Cannot create datastore for datastructure")
	}

	//read in the file and create the runtime
	//path/Dml/main.dml
	rntm := dml.NewRuntime(store)
	file := filepath.Join(path, "Dml", "main.dml")
	filereader, err := os.Open(file)
	if err != nil {
		return state{}, utils.StackError(err, "Unable to load dml file")
	}
	err = rntm.Parse(filereader)
	if err != nil {
		return state{}, utils.StackError(err, "Unable to parse dml file")
	}

	return state{path, data, rntm, store, 0, sync.Mutex{}}, nil
}

func (self *state) Apply(data []byte) error {

	//lock the state
	self.mutex.Lock()
	defer self.mutex.Unlock()

	//get the operation from the log entry
	op := operationFromData(data)
	self.operationNumber++

	//apply to runtime
	op.ApplyTo(self.dml)
	return nil

}

func (self *state) Reset() error {

	self.operationNumber = 0

	//clear the datastore and build a new one
	err := self.store.Delete()
	if err != nil {
		return utils.StackError(err, "Unable to reset state, cannot delete datastore")
	}
	self.store, err = datastore.NewDatastore(self.path)
	if err != nil {
		return utils.StackError(err, "Unable to reset state")
	}

	//reparse the dml file
	self.dml = dml.NewRuntime(self.store)
	file := filepath.Join(self.path, "Dml", "main.dml")
	filereader, err := os.Open(file)
	if err != nil {
		return utils.StackError(err, "Unable to load dml file")
	}
	return self.dml.Parse(filereader)
}

func (self *state) Snapshot() ([]byte, error) {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//make the backup
	err := self.store.PrepareFileBackup()
	defer self.store.FinishFileBackup()
	if err != nil {
		return nil, utils.StackError(err, "Unable to create snapshot from state-machine")
	}

	//generate the p2p descriptor
	id, err := self.data.AddAsync(self.store.Path())
	if err != nil {
		return nil, utils.StackError(err, "Unable to make snapshot: cannot distribute file")
	}

	return stateSnapshot{id, self.operationNumber}.toByte(), err
}

func (self *state) LoadSnapshot(data []byte) error {

	snap, err := snapshotFromBytes(data)
	if err != nil {
		return utils.StackError(err, "Provided data is not a snapshot, cannot load!")
	}

	//fetch the file
	dir := self.store.Path()
	self.store.Delete()
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Hour)
	_, err = self.data.Write(ctx, snap.file, dir)
	if err != nil {
		return utils.StackError(err, "Unable to fetch snapshot data")
	}
	return nil
}

func (self *state) EnsureSnapshot(data []byte) error {

	snap, err := snapshotFromBytes(data)
	if err != nil {
		return utils.StackError(err, "Provided data is not a snapshot, cannot load!")
	}

	if snap.operationNumber != self.operationNumber {
		return fmt.Errorf("Operation numbers not equal: snapshot does not represent current state")
	}

	return nil
}
