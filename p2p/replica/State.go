package replica

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	uuid "github.com/satori/go.uuid"
	"github.com/ickby/CollaborationNode/utils"
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
	return op, err
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
	states map[string]State
	doneChan map[string]chan struct{}
	mutex  sync.RWMutex	
}

func newMultiState() *multiState {
	return &multiState{
		states: make(map[string]State, 0),
		doneChan: make(map[string]chan struct{}, 0),
		mutex:  sync.RWMutex{},
	}
}

func (self *multiState) Add(name string, state State) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()
	if _, has := self.states[name]; has {
		return fmt.Errorf("State already exists, cannot set")
	}
	self.states[name] = state
	return nil
}

func (self *multiState) SetDoneChan(opid string, c chan struct{}) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.doneChan[opid] = c
}

func (self *multiState) Apply(log *raft.Log) interface{} {

	op, err := operationFromBytes(log.Data)
	if err != nil {
		return fmt.Errorf("Unable to decode command")
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	_, has := self.states[op.State]
	if !has {
		return fmt.Errorf("No such state known, cannot apply")
	}
	result := self.states[op.State].Apply(op.Op)
	
	c, ok := self.doneChan[op.OpID]
	if ok {
		c <- struct{}{}
		delete(self.doneChan, op.OpID)
	}
	return result
}

func (self *multiState) Snapshot() (raft.FSMSnapshot, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	snap := make(map[string][]byte, 0)
	fullerr := fmt.Errorf("")
	for name, state := range self.states {
		var err error = nil
		snap[name], err = state.Snapshot()
		if err != nil {
			fullerr = utils.StackError(fullerr, err.Error())
		}
	}

	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(snap)
	if err != nil {
		return nil, err
	}
	if fullerr.Error() != "" {
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
		utils.StackError(err, "Unable to load snapshot")
	}

	fullerr := fmt.Errorf("")
	for name, state := range self.states {
		err := state.LoadSnapshot(snap[name])
		if err != nil {
			fullerr = utils.StackError(fullerr, err.Error())
		}
	}

	if fullerr.Error() != "" {
		return fullerr
	}
	return nil
}

//implements the raft FSMSnapshot interface
type multiStateSnapshot struct {
	data []byte
}

func (self multiStateSnapshot) Persist(sink raft.SnapshotSink) error {
	sink.Write(self.data)
	return nil
}

func (self multiStateSnapshot) Release() {}
