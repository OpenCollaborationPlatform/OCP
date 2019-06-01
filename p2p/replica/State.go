package replica

import (
	"CollaborationNode/utils"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sync"
)

/* State to be replicated
 * - Will not be called from concurrent threads
 * - Execution haltet till return
 */
type State interface {

	//applying commands
	Apply([]byte)

	//snapshoting
	Snapshot() []byte            //crete a snapshot from current state
	LoadSnapshot([]byte) error   //setup state according to snapshot
	EnsureSnapshot([]byte) error //make sure this snapshot can be loaded later on
}

//Internal state store to make multiple state handling easier
//  - states will be added asynchron, hence the state list should be protected
//  - states are accessd synchron, so operations on states do not need to be protected
type stateStore struct {
	states []State
	mutex  sync.RWMutex
}

func newStateStore() stateStore {
	return stateStore{
		states: make([]State, 0),
		mutex:  sync.RWMutex{},
	}
}

func (self *stateStore) Add(state State) uint8 {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.states = append(self.states, state)
	return uint8(len(self.states) - 1)
}

func (self *stateStore) Get(state uint8) State {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	if int(state) >= len(self.states) {
		//logger.Errorf("Want access state %v, but is not available", state)
		return nil
	}

	return self.states[state]
}

func (self *stateStore) StateCount() uint8 {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return uint8(len(self.states))
}

func (self *stateStore) Snaphot() ([]byte, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	snap := make(map[uint8][]byte, 0)
	for i, state := range self.states {
		snap[uint8(i)] = state.Snapshot()
	}

	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(snap)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (self *stateStore) LoadSnaphot(data []byte) error {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	snap := make(map[uint8][]byte, 0)
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&snap)

	if err != nil {
		utils.StackError(err, "Unable to load snapshot")
	}

	errs := make([]error, 0)
	for i, state := range self.states {
		err := state.LoadSnapshot(snap[uint8(i)])
		if err != nil {
			errs = append(errs, utils.StackError(err, "Unable to load snapshot for state %v"))
		}
	}

	if len(errs) != 0 {
		return errs[0]
	}
	return nil
}

func (self *stateStore) EnsureSnapshot(data []byte) error {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	snap := make(map[uint8][]byte, 0)
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&snap)

	if err != nil {
		utils.StackError(err, "Unable to load snapshot")
	}

	errs := make([]error, 0)
	for i, state := range self.states {
		err := state.EnsureSnapshot(snap[uint8(i)])
		if err != nil {
			errs = append(errs, utils.StackError(err, "Unable to ensure snapshot for state %v"))
		}
	}

	if len(errs) != 0 {
		return errs[0]
	}
	return nil
}

//a simple state for testing
func newTestState() *testState {
	return &testState{make([]uint64, 0)}
}

//simple state that stores each uint given to apply
type testState struct {
	Value []uint64
}

//cmd must be a binary uint which is added to the current list of values
func (self *testState) Apply(cmd []byte) {

	buf := bytes.NewBuffer(cmd)
	val, err := binary.ReadUvarint(buf)
	if err == nil {
		self.Value = append(self.Value, val)

	}
}

func (self *testState) Snaphot() []byte {

	res, _ := json.Marshal(self.Value)
	return res
}

func (self *testState) LoadSnaphot(cmd []byte) error {

	self.Value = make([]uint64, 0)
	err := json.Unmarshal(cmd, &self.Value)
	if err != nil {
		return utils.StackError(err, "Unable to load snapshot")
	}

	return nil
}

func (self *testState) Equals(other *testState) bool {

	if len(self.Value) != len(other.Value) {
		return false
	}

	for i, val := range self.Value {
		if other.Value[i] != val {
			return false
		}
	}

	return true
}

func (self *testState) Print() {

	fmt.Printf("Test state with %v entries:\n", len(self.Value))
	for i, val := range self.Value {
		fmt.Printf("%v: %v\n", i, val)
	}
}

func intToByte(val uint64) []byte {

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, val)
	return buf
}
