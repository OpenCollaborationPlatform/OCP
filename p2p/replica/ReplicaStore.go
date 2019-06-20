package replica

import (
	"CollaborationNode/utils"
	"fmt"
	"math"
	"sync"
)

//a store that combines state and logs so that they never go out of sync
type replicaStore struct {
	logs  logStore
	state stateStore
	mutex sync.RWMutex
}

//creates a replica store. If path is empty a memory store is created, otherwise
//a persistent store
func newReplicaStore(path, name string) (replicaStore, error) {

	var log logStore
	if path == "" {
		log = newMemoryLogStore()
	} else {
		var err error
		log, err = newPersistentLogStore(path, name)
		if err != nil {
			return replicaStore{}, utils.StackError(err, "Unable to create replica store")
		}
	}
	state := newStateStore()

	return replicaStore{log, state, sync.RWMutex{}}, nil
}

func (self *replicaStore) Close() error {
	return self.logs.Close()
}

func (self *replicaStore) FirstIndex() (uint64, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return self.logs.FirstIndex()
}

func (self *replicaStore) LastIndex() (uint64, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return self.logs.LastIndex()
}

func (self *replicaStore) GetLog(idx uint64) (Log, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return self.logs.GetLog(idx)
}

func (self *replicaStore) GetLatestLog() (Log, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return self.logs.GetLatestLog()
}

func (self *replicaStore) Add(state State) uint8 {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.state.Add(state)
}

func (self *replicaStore) GetState(state uint8) State {
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	return self.state.Get(state)
}

func (self *replicaStore) StateCount() uint8 {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return self.state.StateCount()
}

func (self *replicaStore) Snaphot() ([]byte, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.state.Snaphot()
}

func (self *replicaStore) Reset() error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.logs.Clear()
	self.state.Reset()
	return nil
}

//stores the log and applies it to the state. Return the index on the channel when done.
func (self *replicaStore) ApplyLog(log Log, ret chan uint64) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//check if it can be applied
	last, err := self.logs.LastIndex()
	if err == nil {
		if log.Index != last+1 {
			return fmt.Errorf("Expected log %v, got %v", last+1, log.Index)
		}
	} else if IsNoEntryError(err) {
		if log.Index != 0 {
			return utils.StackError(err, "Expected log 0, got %v", log.Index)
		}
	} else {
		return utils.StackError(err, "Cannot access logstore")
	}

	//handle the log logic
	switch log.Type {

	case logType_Snapshot:
		//make sure the snapshot is the current state...
		err := self.state.EnsureSnapshot(log.Data)
		if err != nil {
			err := self.state.LoadSnapshot(log.Data)
			if err != nil {
				self.alignStatesToLog() //make sure everything is still aligned
				return utils.StackError(err, "Unable to apply snapshot log")
			}
		}

		//start log compaction. Everything except the snapshot must go
		err = self.logs.Clear()
		if err != nil {
			return utils.StackError(err, "Deleting log range for snapshot failed")
		}

	default:
		state := self.state.Get(log.Type)
		if state != nil {
			err := state.Apply(log.Data)
			if err != nil {
				self.alignStatesToLog() //make sure everything is still aligned
				return utils.StackError(err, "Cannot apply log")
			}

		} else {
			return fmt.Errorf("Cannot apply log: Unknown state")
		}
	}

	//store the log
	err = self.logs.StoreLog(log)
	if err != nil {
		self.alignStatesToLog() //make sure everything is still aligned
		return utils.StackError(err, "Unable to store log")
	}

	//inform (but do not block if no-one listens)
	select {
	case ret <- log.Index:
		break
	default:
		break
	}

	return nil
}

func (self *replicaStore) ForwardToSnapshot(log Log, ret chan uint64) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//check if it is a snapshot
	if log.Type != logType_Snapshot {
		return fmt.Errorf("Log is not a snapshot, cannot forward")
	}

	//check if it is forwarding or if we have something newer already
	//check if it can be applied
	last, err := self.logs.LastIndex()
	if err == nil {
		if log.Index <= last {
			return fmt.Errorf("Cannot forward, as newer logs are already commited: %v vs %v", log.Index, last)
		}
	} else if !IsNoEntryError(err) {

		return utils.StackError(err, "Cannot access logstore")
	}

	//load the snapshot
	err = self.state.LoadSnapshot(log.Data)
	if err != nil {
		self.alignStatesToLog() //make sure store is consistent
		return utils.StackError(err, "Unable to apply snapshot log")
	}

	//start log compaction. Everything except the snapshot must go
	err = self.logs.Clear()
	if err != nil {
		self.alignStatesToLog() //make sure store is consistent
		return utils.StackError(err, "Deleting log range for snapshot failed")
	}

	//store the log
	err = self.logs.StoreLog(log)
	if err != nil {
		self.alignStatesToLog() //make sure store is consistent
		return utils.StackError(err, "Unable to store snapshot log")
	}

	//inform (but do not block if no-one listens)
	select {
	case ret <- log.Index:
		break
	default:
		break
	}

	return nil
}

func (self *replicaStore) AlignToLeaders(leader *leaderStore) (bool, error) {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//make sure noone changes the leader state during this function
	leader.RLock()
	defer leader.RUnlock()

	//find the last valid log. We only check epoch start and end index, as all other
	//verifications have been done in the commit already
	last, err := self.logs.LastIndex()
	if IsNoEntryError(err) {
		//nothing to recover for zero entries
		return false, nil
	} else if err != nil {
		return false, utils.StackError(err, "Unable to access logstore")
	}

	valid := true
	first, _ := self.logs.FirstIndex()
	for i := last; (i >= first) && (i != math.MaxUint64); i-- {
		log, _ := self.logs.GetLog(i)

		//if we are below the epoch start it must be deleted (unlikely, as this would have
		//been captured during verify in commit. But better double check)
		start, _ := leader.LockedGetLeaderStartIdxForEpoch(log.Epoch)
		if log.Index < start {
			valid = false
			break
		}

		//check if threre is a newer epoch which was made known to us after commiting
		//the log (the realistic scenario for a recover)
		if leader.LockedHasEpoch(log.Epoch + 1) {
			end, _ := leader.LockedGetLeaderStartIdxForEpoch(log.Epoch + 1)
			if log.Index >= end {
				valid = false
				break
			}
		}
	}

	//maybe we do not need to change anything?
	if valid {
		return false, nil
	}

	//check if we have a snapshot in the beginning of our history
	log, _ := self.logs.GetLog(first)
	var snap Log
	if log.Type == logType_Snapshot {
		snap = log
	}

	//if we do not have a snapshot to recover from we need to start all over
	if !snap.IsValid() {
		self.logs.Clear()
		self.state.Reset()

	} else {
		//otherwise we recover from snapshot
		err := self.state.LoadSnapshot(snap.Data)
		if err != nil {
			return false, err
		}

		err = self.logs.DeleteUpFrom(snap.Index)
		if err != nil {
			return true, err
		}
	}

	return true, err
}

//this is a recovery function: it resets the states to the logs
//should only be called when something went wrong with applying logs to
//states or storing logs... Pure internal function! Hence no locking!
//--> Do not call from outside the store!
func (self *replicaStore) alignStatesToLog() {

	first, err := self.logs.FirstIndex()
	if IsNoEntryError(err) {
		//nothing to align for zero logs
		return
	} else if err != nil {
		//TODO: replcae panic with some serious error handling
		panic("Unable to align states to log")
	}

	//reset state if there is no snapshot
	if log, err := self.logs.GetLog(first); err == nil {
		if log.Type != logType_Snapshot {
			self.state.Reset()
		}
	} else {
		//TODO: replcae panic with some serious error handling
		panic("Unable to align states to log")
	}

	//apply all logs
	last, _ := self.logs.LastIndex()
	for i := first; i <= last; i-- {
		log, err := self.logs.GetLog(i)
		if err != nil {
			//TODO: replcae panic with some serious error handling
			panic("Unable to align states to log")
		}

		if log.Type == logType_Snapshot {

			self.state.LoadSnapshot(log.Data)

		} else {
			state := self.state.Get(log.Type)
			if state == nil {
				//TODO: replcae panic with some serious error handling
				panic("Unable to align states to log")
			}
			state.Apply(log.Data)
		}
	}
}

func (self *replicaStore) Equals(others []*replicaStore) bool {

	//lock all for comparison
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	for _, store := range others {
		store.mutex.RLock()
		defer store.mutex.RUnlock()
	}

	//compare
	for _, rep := range others {

		if !self.logs.Equals(rep.logs) {
			return false
		}
		if !self.state.Equals(&rep.state) {
			return false
		}
	}

	return true
}
