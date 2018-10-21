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

func (self KeyValueDatabase) HasSet(set [32]byte) bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		result = tx.Bucket(set[:]) != nil
		return nil
	})

	return result
}

func (self KeyValueDatabase) GetOrCreateSet(set [32]byte) Set {

	if !self.HasSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucketIfNotExists(set[:])
			return nil
		})
	}

	return KeyValueSet{self.db, set[:], make([][]byte, 0)}
}

func (self KeyValueDatabase) RemoveSet(set [32]byte) error {

	if self.HasSet(set) {

		var result error
		self.db.Update(func(tx *bolt.Tx) error {
			result = tx.DeleteBucket(set[:])
			return nil
		})

		return result
	}

	return nil
}

func (self KeyValueDatabase) Close() {
	self.db.Close()
}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type KeyValueSet struct {
	db         *bolt.DB
	mainbucket []byte
	subbuckets [][]byte
}

func (self KeyValueSet) IsValid() bool {

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

func (self *KeyValueSet) HasKey(key []byte) bool {

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

func (self *KeyValueSet) GetOrCreateKey(key []byte) KeyValuePair {

	if !self.HasKey(key) {

		//make sure the set exists in the db with null value
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

func (self *KeyValueSet) RemoveKey(key string) error {

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

func (self *KeyValueSet) HasSubSet(set []byte) bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.mainbucket)
		for _, bkey := range self.subbuckets {
			bucket = bucket.Bucket(bkey)
		}
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result
}

func (self *KeyValueSet) GetOrCreateSubSet(set []byte) KeyValueSet {

	if !self.HasSubSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.mainbucket)
			for _, bkey := range self.subbuckets {
				bucket = bucket.Bucket(bkey)
			}
			bucket.CreateBucketIfNotExists(set[:])
			return nil
		})
	}

	//build the set
	subs := append(self.subbuckets, set)
	return KeyValueSet{self.db, self.mainbucket, subs}
}

func (self *KeyValueSet) RemoveSubSet(set []byte) error {

	if self.HasSubSet(set) {

		var result error
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.mainbucket)
			for _, bkey := range self.subbuckets {
				bucket = bucket.Bucket(bkey)
			}
			result = bucket.DeleteBucket(set[:])
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
