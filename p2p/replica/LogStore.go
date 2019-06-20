package replica

import (
	"CollaborationNode/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	"github.com/boltdb/bolt"
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
)

type NoEntryError struct{}

func (self *NoEntryError) Error() string {
	return "No entry availbale in log"
}

func IsNoEntryError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*NoEntryError)
	return ok
}

type logStore interface {
	Close() error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(idx uint64) (Log, error)
	GetLatestLog() (Log, error)
	StoreLog(log Log) error
	StoreLogs(logs []Log) error
	DeleteUpTo(idx uint64) error
	DeleteUpFrom(idx uint64) error
	Clear() error
	Equals(logStore) bool
}

type memoryLogStore struct {
	memory map[uint64]Log
	min    uint64
	max    uint64
	mutex  sync.RWMutex
}

func newMemoryLogStore() logStore {

	return &memoryLogStore{make(map[uint64]Log, 0), 0, 0, sync.RWMutex{}}
}

func (self *memoryLogStore) Close() error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.memory = make(map[uint64]Log, 0)
	return nil
}

func (self *memoryLogStore) FirstIndex() (uint64, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()
	if len(self.memory) == 0 {
		return 0, &NoEntryError{}
	}

	return self.min, nil
}

func (self *memoryLogStore) LastIndex() (uint64, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	if len(self.memory) == 0 {
		return 0, &NoEntryError{}
	}

	return self.max, nil
}

func (self *memoryLogStore) GetLog(idx uint64) (Log, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	log, ok := self.memory[idx]
	if !ok {
		return Log{}, fmt.Errorf("Log is not available")
	}

	return log, nil
}

func (self *memoryLogStore) GetLatestLog() (Log, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	if len(self.memory) == 0 {
		return Log{}, &NoEntryError{}
	}

	log, ok := self.memory[self.max]
	if !ok {
		return Log{}, fmt.Errorf("Error in internal store state")
	}

	return log, nil
}

func (self *memoryLogStore) StoreLog(log Log) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if len(self.memory) == 0 {
		self.max = log.Index
		self.min = log.Index
	}

	self.memory[log.Index] = log
	if log.Index < self.min {
		self.min = log.Index

	} else if log.Index > self.max {
		self.max = log.Index
	}
	return nil
}

func (self *memoryLogStore) StoreLogs(logs []Log) error {

	for _, log := range logs {
		self.StoreLog(log)
	}

	return nil
}

//deletes all entries up to (but not including) the given idx
func (self *memoryLogStore) DeleteUpTo(idx uint64) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if idx > self.max || idx < self.min {
		return fmt.Errorf("TThe provided idx does not exist, cannot delete")
	}

	for i := self.min; i < idx; i++ {
		delete(self.memory, i)
	}
	self.min = idx
	return nil
}

//deletes all entries from (but not including) the given idx
func (self *memoryLogStore) DeleteUpFrom(idx uint64) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if idx > self.max || idx < self.min {
		return fmt.Errorf("TThe provided idx does not exist, cannot delete")
	}

	for i := idx + 1; i <= self.max; i++ {
		delete(self.memory, i)
	}
	self.max = idx
	return nil
}

func (self *memoryLogStore) Clear() error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.memory = make(map[uint64]Log)
	self.max = 0
	self.min = 0

	return nil
}

func (self *memoryLogStore) Equals(store logStore) bool {

	other, ok := store.(*memoryLogStore)
	if !ok {
		//cannot compare to non-memory logstores
		return false
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	other.mutex.Lock()
	defer other.mutex.Unlock()

	if len(self.memory) != len(other.memory) {
		fmt.Printf("Lengths of logstores are different: %v vs %v\n", len(self.memory), len(other.memory))
		return false
	}

	if self.min != other.min {
		fmt.Printf("First indexes of logstores are different: %v vs %v\n", self.min, other.min)
		return false
	}

	if self.max != other.max {
		fmt.Printf("Last indexes of logstores are different: %v vs %v\n", self.max, other.max)
		return false
	}

	for i := self.min; i <= self.max; i++ {

		val, ok := self.memory[i]
		oval, ook := self.memory[i]

		if ok != ook {
			fmt.Printf("Indexes %v available in 1 store, but not the otehr\n", i)
			return false
		}

		if ok {
			//compare the log
			if val.Index != oval.Index {
				fmt.Printf("Logs %v have different indexes: %v vs %v\n", i, val.Index, oval.Index)
				return false
			}
			if val.Epoch != oval.Epoch {
				fmt.Printf("Logs %v have different epochs: %v vs %v\n", i, val.Epoch, oval.Epoch)
				return false
			}
			if val.Type != oval.Type {
				fmt.Printf("Logs %v have different types: %v vs %v\n", i, val.Type, oval.Type)
				return false
			}
			if !bytes.Equal(val.Signature, oval.Signature) {
				fmt.Printf("Logs %v have different signatures\n", i)
				return false
			}
			if !bytes.Equal(val.Data, oval.Data) {
				fmt.Printf("Logs %v have different data\n", i)
				return false
			}
		}
	}
	return true
}

type persistentLogStore struct {
	db *bolt.DB
}

func newPersistentLogStore(path string, name string) (logStore, error) {

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, utils.StackError(err, "Cannot open path %s for replica store", path)
	}
	dbpath := filepath.Join(path, name+"-log.db")
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, utils.StackError(err, "Unable to open bolt db: %s", dbpath)
	}

	//make sure the basic structure exists
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dbLogs)
		return err
	})

	return &persistentLogStore{db}, err
}

// Close is used to gracefully close the DB connection.
func (self *persistentLogStore) Close() error {
	return self.db.Close()
}

func (self *persistentLogStore) FirstIndex() (uint64, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, &NoEntryError{}
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (self *persistentLogStore) LastIndex() (uint64, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, &NoEntryError{}
	} else {
		return bytesToUint64(last), nil
	}
}

func (self *persistentLogStore) GetLog(idx uint64) (Log, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return Log{}, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return Log{}, fmt.Errorf("Entry not found")
	}

	log, err := LogFromBytes(val)
	if val == nil {
		return Log{}, utils.StackError(err, "Entry badly formatted")
	}

	return log, nil
}

func (self *persistentLogStore) GetLatestLog() (Log, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return Log{}, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbLogs)
	_, val := bucket.Cursor().Last()

	if val == nil {
		return Log{}, &NoEntryError{}
	}

	log, err := LogFromBytes(val)
	if val == nil {
		return Log{}, utils.StackError(err, "Entry badly formatted")
	}

	return log, nil
}

// persistentLogStoreLog is used to store a single raft log
func (self *persistentLogStore) StoreLog(log Log) error {
	return self.StoreLogs([]Log{log})
}

// persistentLogStoreLogs is used to store a set of raft logs
func (self *persistentLogStore) StoreLogs(logs []Log) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := log.ToBytes()
		if err != nil {
			return err
		}
		bucket := tx.Bucket(dbLogs)
		if err := bucket.Put(key, val); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Deletes all logs smaller than the given index (not including the index itself)
func (self *persistentLogStore) DeleteUpTo(idx uint64) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.First(); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) >= idx {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Deletes all logs larger than the given index (not including the index itself)
func (self *persistentLogStore) DeleteUpFrom(idx uint64) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.First(); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) <= idx {
			continue
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (self *persistentLogStore) Clear() error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.First(); k != nil; k, _ = curs.Next() {
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (self *persistentLogStore) Equals(store logStore) bool {

	//not implemented (not needed for tests)
	return false
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
