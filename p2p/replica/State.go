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

	//manipulation
	Apply([]byte) error //apply a command to change the state
	Reset() error       //reset state to initial value without any apply

	//snapshoting
	Snapshot() ([]byte, error)   //crete a snapshot from current state
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
		snap[uint8(i)], _ = state.Snapshot()
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

func (self *stateStore) Reset() error {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	errs := make([]error, 0)
	for _, state := range self.states {
		err := state.Reset()
		if err != nil {
			errs = append(errs, utils.StackError(err, "Unable to restore snapshot for state %v"))
		}
	}

	if len(errs) != 0 {
		return errs[0]
	}
	return nil
}

//a simple state for testing. This one must be thread safe (in contrast to real states) as
//it is also accessed by the test functions
func newTestState() *testState {
	return &testState{make([]uint64, 0), sync.RWMutex{}}
}

//simple state that stores each uint given to apply
type testState struct {
	value []uint64
	mutex sync.RWMutex
}

//cmd must be a binary uint which is added to the current list of values
func (self *testState) Apply(cmd []byte) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	buf := bytes.NewBuffer(cmd)
	val, err := binary.ReadUvarint(buf)
	if err == nil {
		self.value = append(self.value, val)
		return nil
	}
	return fmt.Errorf("Unable to apply command: %v", err)
}

func (self *testState) Reset() error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.value = make([]uint64, 0)
	return nil
}

func (self *testState) Snapshot() ([]byte, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	res, err := json.Marshal(self.value)
	return res, err
}

func (self *testState) LoadSnapshot(cmd []byte) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.value = make([]uint64, 0)
	err := json.Unmarshal(cmd, &self.value)
	if err != nil {
		return utils.StackError(err, "Unable to load snapshot")
	}

	return nil
}

func (self *testState) EnsureSnapshot(snap []byte) error {
	//snapshot holds all required data, hence yes, we can apply it
	return nil
}

func (self *testState) Equals(other *testState) bool {

	self.mutex.RLock()
	other.mutex.RLock()
	defer self.mutex.RUnlock()
	defer other.mutex.RUnlock()

	if len(self.value) != len(other.value) {
		fmt.Printf("Length wrong: %v vs %v\n", len(self.value), len(other.value))
		return false
	}

	for i, val := range self.value {
		if other.value[i] != val {
			fmt.Printf("Entry %v wrong: %v vs %v\n", i, val, other.value[i])
			return false
		}
	}

	return true
}

func (self *testState) EntryCount() int {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return len(self.value)
}

func (self *testState) Print() {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	fmt.Printf("Test state with %v entries:\n", len(self.value))
	for i, val := range self.value {
		fmt.Printf("%v: %v\n", i, val)
	}
}

func intToByte(val uint64) []byte {

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, val)
	return buf
}
