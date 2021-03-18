package replica

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/OpenCollaborationPlatform/OCP/utils"
	uuid "github.com/satori/go.uuid"
)

//a little herlper to ease multi state commands
type Operation struct {
	State string
	OpID  string
	Op    []byte
}

func NewOperation(state string, cmd []byte) Operation {
	opID := uuid.NewV4().String()
	return Operation{state, opID, cmd}
}

func operationFromBytes(data []byte) (Operation, error) {
	var op Operation
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&op)
	return op, wrapInternalError(err, Error_Invalid_Data)
}

func (self Operation) ToBytes() []byte {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(self)
	if err != nil {
		return nil
	}
	return b.Bytes()
}

//interface that must be implemented by replica states
type State interface {
	Apply([]byte) interface{}
	Snapshot() ([]byte, error)
	LoadSnapshot([]byte) error
}

//implements the raft FSM interface
type multiState struct {
	states     map[string]State
	resultChan map[string]chan interface{}
	mutex      sync.RWMutex
}

func newMultiState() *multiState {
	return &multiState{
		states:     make(map[string]State, 0),
		resultChan: make(map[string]chan interface{}, 0),
		mutex:      sync.RWMutex{},
	}
}

func (self *multiState) Add(name string, state State) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()
	if _, has := self.states[name]; has {
		return newInternalError(Error_Operation_Invalid, "State already exists, cannot set")
	}
	self.states[name] = state
	return nil
}

func (self *multiState) CreateResultChan(opid string) (chan interface{}, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	_, ok := self.resultChan[opid]
	if ok {
		return nil, newInternalError(Error_Operation_Invalid, "Operation ID has already registered channel")
	}

	c := make(chan interface{}, 1)
	self.resultChan[opid] = c
	return c, nil
}

func (self *multiState) CloseResultChan(opid string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	c, ok := self.resultChan[opid]
	if ok {
		close(c)
		delete(self.resultChan, opid)
	}
}

func (self *multiState) Apply(log *raft.Log) interface{} {

	op, err := operationFromBytes(log.Data)
	if err != nil {
		return newInternalError(Error_Invalid_Data, "Unable to decode command")
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	_, has := self.states[op.State]
	if !has {
		return newInternalError(Error_Invalid_Data, "No such state known, cannot apply", "state", op.State)
	}
	result := self.states[op.State].Apply(op.Op)

	//also return in case its an error, as this is a valid command result
	c, ok := self.resultChan[op.OpID]
	if ok {
		c <- result
		close(c) //buffered channel, the reader will receive the result
		delete(self.resultChan, op.OpID)
	}
	return nil
}

func (self *multiState) Snapshot() (raft.FSMSnapshot, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	snap := make(map[string][]byte, 0)
	fullerr := newInternalError(Error_Process, "Unable to create snapshot")
	for name, state := range self.states {
		var err error = nil
		snap[name], err = state.Snapshot()
		if err != nil {
			fullerr = utils.StackError(fullerr, err.Error()).(utils.OCPError)
		}
	}

	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(snap)
	if err != nil {
		return nil, wrapInternalError(err, Error_Invalid_Data)
	}
	if len(fullerr.Stack()) > 1 {
		return multiStateSnapshot{}, fullerr
	}
	return multiStateSnapshot{b.Bytes()}, nil
}

func (self *multiState) Restore(reader io.ReadCloser) error {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	snap := make(map[string][]byte, 0)
	data := make([]byte, 0)
	reader.Read(data)
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&snap)

	if err != nil {
		return wrapInternalError(err, Error_Invalid_Data)
	}

	fullerr := newInternalError(Error_Process, "Unable to create snapshot")
	for name, state := range self.states {
		err := state.LoadSnapshot(snap[name])
		if err != nil {
			fullerr = utils.StackError(fullerr, err.Error()).(utils.OCPError)
		}
	}

	if len(fullerr.Stack()) > 1 {
		return fullerr
	}
	return nil
}

//implements the raft FSMSnapshot interface
type multiStateSnapshot struct {
	data []byte
}

func (self multiStateSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(self.data)
	return wrapInternalError(err, Error_Process)
}

func (self multiStateSnapshot) Release() {}
