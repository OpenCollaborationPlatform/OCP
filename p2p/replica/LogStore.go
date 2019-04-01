package replica

import (
	"CollaborationNode/utils"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")
)

type Log struct {
	Index uint64
	Data  []byte
}

func (self *Log) ToBytes() ([]byte, error) {
	return json.Marshal(self)
}

func LogFromBytes(data []byte) (Log, error) {
	var log Log
	err := json.Unmarshal(data, &log)
	return log, err
}

type LogStore struct {
	db *bolt.DB
}

func NewLogStore(path string, name string) (LogStore, error) {

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return LogStore{}, utils.StackError(err, "Cannot open path %s for replica store", path)
	}
	dbpath := filepath.Join(path, name+"-log.db")
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return LogStore{}, utils.StackError(err, "Unable to open bolt db: %s", dbpath)
	}

	//make sure the basic structure exists
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(dbLogs)
		tx.CreateBucketIfNotExists(dbConf)
		return nil
	})

	return LogStore{db}, nil
}

// Close is used to gracefully close the DB connection.
func (self *LogStore) Close() error {
	return self.db.Close()
}

func (self *LogStore) FirstIndex() (uint64, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (self *LogStore) LastIndex() (uint64, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

func (self *LogStore) GetLog(idx uint64) (Log, error) {
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

// LogStoreLog is used to store a single raft log
func (self *LogStore) StoreLog(log Log) error {
	return self.StoreLogs([]Log{log})
}

// LogStoreLogs is used to store a set of raft logs
func (self *LogStore) StoreLogs(logs []Log) error {

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

// DeleteRange is used to delete logs within a given range inclusively.
func (self *LogStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

/*
// Set is used to set a key/value set outside of the raft log
func (self *LogStore) Set(k, v []byte) error {
	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key
func (self *LogStore) Get(k []byte) ([]byte, error) {
	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	val := bucket.Get(k)

	if val == nil {
		return nil, fmt.Errorf("Unable to find entry")
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 is like Set, but handles uint64 values
func (self *LogStore) SetUint64(key []byte, val uint64) error {
	return self.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (self *LogStore) GetUint64(key []byte) (uint64, error) {
	val, err := self.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}*/

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
