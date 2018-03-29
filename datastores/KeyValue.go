// KeyValue datastore: The key value database saves the multople stores within a
// single bolt db, one bucket for each store.
package datastore

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

func NewKeyValueDatabase(path string, name string) (*keyValueDatabase, error) {

	//make sure the path exist...
	os.MkdirAll(path, os.ModePerm)
	path = filepath.Join(path, name+".db")

	fmt.Println(path)
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	stores := make(map[string]keyValueStore, 0)

	return &keyValueDatabase{db, stores}, nil
}

//implements the database interface
type keyValueDatabase struct {
	db     *bolt.DB
	stores map[string]keyValueStore
}

func (self *keyValueDatabase) HasStore(name string) bool {

	_, ok := self.stores[name]
	return ok
}

func (self *keyValueDatabase) GetOrCreateStore(name string) Store {

	if !self.HasStore(name) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucketIfNotExists([]byte(name))
			return nil
		})
		self.stores[name] = keyValueStore{self.db, name,
			make(map[string]keyValueEntry, 0)}
	}

	store := self.stores[name]
	return &store
}

func (self *keyValueDatabase) Close() {
	self.db.Close()
}

//The store itself is very simple, as all the access logic will be in the entry type
//this is only to manage the existing entries
type keyValueStore struct {
	db      *bolt.DB
	name    string
	entries map[string]keyValueEntry
}

func (self *keyValueStore) GetOrCreateEntry(name string) Entry {

	if !self.HasEntry(name) {

		//make sure the entry exists in the db with null value
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(self.name))
			bucket.Put([]byte(name), make([]byte, 0))
			return nil
		})

		//build the entry
		entry := keyValueEntry{self.db, self.name, name}
		self.entries[name] = entry
	}

	entry := self.entries[name]
	return &entry
}

func (self *keyValueStore) HasEntry(name string) bool {

	_, ok := self.entries[name]
	return ok
}

func (self *keyValueStore) RemoveEntry(name string) {

	if !self.HasEntry(name) {
		return
	}

	entry := self.entries[name]
	entry.Remove()
	delete(self.entries, name)
}

type keyValueEntry struct {
	db     *bolt.DB
	bucket string
	key    string
}

func (self *keyValueEntry) Write(value interface{}) error {

	bts, err := getBytes(value)
	if err != nil {
		return err
	}

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(self.bucket))
		return bucket.Put([]byte(self.key), bts)
	})
}

func (self *keyValueEntry) IsValid() bool {

	if self.bucket == "" || self.key == "" {
		return false
	}

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(self.bucket))
		result = bucket.Get([]byte(self.key)) != nil
		return nil
	})

	return result
}

func (self *keyValueEntry) Read() (interface{}, error) {

	var result interface{}
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(self.bucket))
		data := bucket.Get([]byte(self.key))
		if len(data) == 0 {
			return fmt.Errorf("Value was not set before read")
		}
		res, err := getInterface(data)
		result = res
		return err
	})

	if err != nil {
		return nil, err
	}

	return result, err
}

func (self *keyValueEntry) Remove() bool {

	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(self.bucket))
		return bucket.Delete([]byte(self.key))
	})
	return err == nil
}

//helper functions
func getBytes(data interface{}) ([]byte, error) {

	return json.Marshal(data)
}

func getInterface(bts []byte) (interface{}, error) {

	var res interface{}
	err := json.Unmarshal(bts, &res)

	//json does not distuinguish between float and int
	num, ok := res.(float64)
	if ok && num == math.Trunc(num) {
		return int64(num), nil
	}

	return res, err
}
