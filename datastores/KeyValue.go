// KeyValue database: The key value database saves the multiple entries within a
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

func NewKeyValueDatabase(path string, name string) (*KeyValueDatabase, error) {

	//make sure the path exist...
	os.MkdirAll(path, os.ModePerm)
	path = filepath.Join(path, name+".db")

	fmt.Println(path)
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	return &KeyValueDatabase{db}, nil
}

//implements the database interface
type KeyValueDatabase struct {
	db *bolt.DB
}

func (self *KeyValueDatabase) HasEntry(entry [32]byte) bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		result = tx.Bucket(entry[:]) != nil
		return nil
	})

	return result
}

func (self *KeyValueDatabase) GetOrCreateEntry(entry [32]byte) Entry {

	if !self.HasEntry(entry) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucketIfNotExists(entry[:])
			return nil
		})
	}

	return KeyValueEntry{self.db, entry[:], make([][]byte, 0)}
}

func (self *KeyValueDatabase) RemoveEntry(entry [32]byte) error {

	if self.HasEntry(entry) {

		var result error
		self.db.View(func(tx *bolt.Tx) error {
			result = tx.DeleteBucket(entry[:])
			return nil
		})

		return result
	}

	return nil
}

func (self *KeyValueDatabase) Close() {
	self.db.Close()
}

//The store itself is very simple, as all the access logic will be in the entry type
//this is only to manage the existing entries
type KeyValueEntry struct {
	db         *bolt.DB
	mainbucket []byte
	subbuckets [][]byte
}

func (self KeyValueEntry) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.mainbucket)
		if bucket == nil {
			result = false
			return nil
		}
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
			if bucket == nil {
				result = false
				return nil
			}
		}
		result = true
		return nil
	})

	return result
}

func (self *KeyValueEntry) HasKey(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		result = bucket.Get(key) != nil
		return nil
	})

	return result
}

func (self *KeyValueEntry) GetOrCreateKey(key []byte) KeyValuePair {

	if !self.HasKey(key) {

		//make sure the entry exists in the db with null value
		self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket(self.mainbucket)
			for _, bkey := range self.subbuckets {
				bucket = bucket.Bucket(bkey)
			}
			bucket.Put(key, make([]byte, 0))
			return nil
		})
	}

	return KeyValuePair{self.db, self.mainbucket, self.subbuckets, key}
}

func (self *KeyValueEntry) RemoveKey(key string) error {

	err := self.db.View(func(tx *bolt.Tx) error {

		//get correct bucket
		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		//and delete
		return bucket.Delete([]byte(key))
	})
	return err
}

func (self *KeyValueEntry) HasSubEntry(entry []byte) bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		result = bucket.Bucket(entry[:]) != nil
		return nil
	})

	return result
}

func (self *KeyValueEntry) GetOrCreateSubEntry(entry []byte) KeyValueEntry {

	if !self.HasSubEntry(entry) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.mainbucket)
			for _, bkey := range self.subbuckets {
				bucket = bucket.Bucket(bkey)
			}
			bucket.CreateBucketIfNotExists(entry[:])
			return nil
		})
	}

	//build the entry
	subs := append(self.subbuckets, entry)
	return KeyValueEntry{self.db, self.mainbucket, subs}
}

func (self *KeyValueEntry) RemoveSubEntry(entry []byte) error {

	if self.HasSubEntry(entry) {

		var result error
		self.db.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.mainbucket)
			for _, bkey := range self.subbuckets {
				bucket = bucket.Bucket(bkey)
			}
			result = bucket.DeleteBucket(entry[:])
			return nil
		})

		return result
	}

	return nil
}

type KeyValuePair struct {
	db         *bolt.DB
	mainbucket []byte
	subbuckets [][]byte
	key        []byte
}

func (self *KeyValuePair) Write(value interface{}) error {

	bts, err := getBytes(value)
	if err != nil {
		return err
	}

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Put(self.key, bts)
	})
}

func (self *KeyValuePair) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		result = bucket.Get(self.key) != nil
		return nil
	})

	return result
}

func (self *KeyValuePair) Read() (interface{}, error) {

	var result interface{}
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(self.key)
		if data == nil || len(data) == 0 {
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

func (self *KeyValuePair) Remove() bool {

	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Delete(self.key)
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
