// KeyValue database: The key value database saves the multiple entries within a
// single bolt db, one bucket for each store.
package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/boltdb/bolt"
	"github.com/mr-tron/base58/base58"
)

func NewKeyValueDatabase(db *bolt.DB) (*KeyValueDatabase, error) {

	//make sure key value store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("keyvalue"))
		return nil
	})

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
		bucket := tx.Bucket([]byte("keyvalue"))
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result
}

func (self KeyValueDatabase) GetOrCreateSet(set [32]byte) Set {

	if !self.HasSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("keyvalue"))
			newbucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			//set default values
			newbucket.Put([]byte("currentversion"), itob(0))
			newbucket.CreateBucketIfNotExists([]byte("versions"))

			return nil
		})
	}

	return &KeyValueSet{self.db, set[:]}
}

func (self KeyValueDatabase) RemoveSet(set [32]byte) error {

	if self.HasSet(set) {

		var result error
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("keyvalue"))
			result = bucket.DeleteBucket(set[:])
			return nil
		})

		return result
	}

	return nil
}

func (self KeyValueDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type KeyValueSet struct {
	db     *bolt.DB
	setkey []byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *KeyValueSet) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		if bucket == nil {
			result = false
			return nil
		}
		result = bucket.Bucket(self.setkey) == nil
		return nil
	})

	return result
}

func (self *KeyValueSet) FixStateAsVersion() (VersionID, error) {

	//we iterate over all entries and get the sequence number to store as current
	//state
	versionkey := []byte("versions")
	version := make(map[string]uint64)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)

		c := bucket.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {

			if !bytes.Equal(key, versionkey) {
				subbucket := bucket.Bucket(key)
				strkey := base58.Encode(key)
				version[strkey] = subbucket.Sequence()
			}
		}
		return nil
	})

	//sage the new version as entry in the "version" keyvalue entry
	vp := self.GetOrCreateKey(versionkey)
	err := vp.Write(version)

	//save as current version
	self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)
		bucket.Put([]byte("currentversion"), itob(uint64(vp.LatestVersion())))
		return nil
	})

	//Todo: it is quite likely that individual key value pairs have multiple sub-versions
	//      between the last and the new global version. They can be cleaned up.

	return VersionID(vp.CurrentVersion()), err
}

func (self *KeyValueSet) LoadVersion(id VersionID) error {

	//we write the current version
	self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)
		bucket.Put([]byte("currentversion"), itob(uint64(id)))
		return nil
	})

	//we grab the version data
	version := make(map[string]uint64)
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)
		bucket = bucket.Bucket([]byte("versions"))
		data := bucket.Get(itob(uint64(id)))
		if data == nil || len(data) == 0 {
			return fmt.Errorf("Version does not exist")
		}
		res, err := getInterface(data)
		if err != nil {
			return err
		}
		resmap, ok := res.(map[string]uint64)
		if !ok {
			return fmt.Errorf("Problem with parsing the saved data")
		}
		version = resmap
		return nil
	})
	if err != nil {
		return err
	}

	//make sure all subentries have loaded the correct subversion
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)

		for key, vers := range version {
			data, err := base58.Decode(string(key))
			if err != nil {
				return fmt.Errorf("Could not load the correct key from version data")
			}

			subbucket := bucket.Bucket(data)
			err = subbucket.SetSequence(vers)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (self *KeyValueSet) GetLatestVersion() (VersionID, error) {

	//read the last version we created
	vp := self.GetOrCreateKey([]byte("versions"))
	return vp.LatestVersion(), nil
}

func (self *KeyValueSet) RemoveVersionsUpTo(VersionID) error {
	return nil
}

func (self *KeyValueSet) RemoveVersionsUpFrom(VersionID) error {
	return nil
}

/*
 * Key-Value functions
 * ********************************************************************************
 */
func (self *KeyValueSet) HasKey(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)
		result = bucket.Bucket(key) != nil
		return nil
	})

	return result
}

func (self *KeyValueSet) GetOrCreateKey(key []byte) *KeyValuePair {

	if !self.HasKey(key) {

		//make sure the set exists in the db with null value
		self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket([]byte("keyvalue"))
			bucket = bucket.Bucket(self.setkey)
			_, err := bucket.CreateBucketIfNotExists(key)
			return err
		})
	}

	return &KeyValuePair{self.db, self.setkey, key}
}

func (self *KeyValueSet) RemoveKey(key []byte) error {

	err := self.db.View(func(tx *bolt.Tx) error {

		//get correct bucket
		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)
		//and delete
		return bucket.DeleteBucket(key)
	})
	return err
}

/*
 * Pair functions
 * ********************************************************************************
 */
type KeyValuePair struct {
	db     *bolt.DB
	setkey []byte
	key    []byte
}

func (self *KeyValuePair) Write(value interface{}) error {

	//check if we are allowed to write: is the set current version the maximum version?
	var currentVersion, latestVersion uint64
	latestVersion = 0
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		bucket = bucket.Bucket(self.setkey)
		versdata := bucket.Get([]byte("currentversion"))
		if versdata == nil {
			return fmt.Errorf("Could not access sets version data")
		}
		currentVersion = btoi(versdata)

		bucket = bucket.Bucket([]byte("versions"))
		if bucket == nil {
			return fmt.Errorf("Versions bucket does not exist")
		}
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val > latestVersion {
				latestVersion = val
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return err
	}
	if currentVersion != latestVersion {
		return fmt.Errorf("Cannot write, older version is loaded")
	}

	bts, err := getBytes(value)
	if err != nil {
		return err
	}

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		for _, bkey := range [][]byte{self.setkey, self.key} {
			bucket = bucket.Bucket(bkey)
		}
		id, _ := bucket.NextSequence()
		return bucket.Put(itob(id), bts)
	})
}

func (self *KeyValuePair) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		for _, bkey := range [][]byte{self.setkey, self.key} {
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

		bucket := tx.Bucket([]byte("keyvalue"))
		for _, bkey := range [][]byte{self.setkey, self.key} {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(itob(bucket.Sequence()))
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

		bucket := tx.Bucket([]byte("keyvalue"))
		for _, bkey := range [][]byte{self.setkey, self.key} {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Delete(itob(bucket.Sequence()))
	})
	return err == nil
}

func (self *KeyValuePair) CurrentVersion() VersionID {

	var version uint64
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		for _, bkey := range [][]byte{self.setkey, self.key} {
			bucket = bucket.Bucket(bkey)
		}
		version = bucket.Sequence()
		return nil
	})

	return VersionID(version)
}

func (self *KeyValuePair) LatestVersion() VersionID {

	var version uint64 = 0
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("keyvalue"))
		for _, bkey := range [][]byte{self.setkey, self.key} {
			bucket = bucket.Bucket(bkey)
		}
		//could be that we have the sequence set to any value, must not be the latest
		//hence we iterate over all and search for the biggest
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val > version {
				version = val
			}
			return nil
		})
		return nil
	})

	return VersionID(version)
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

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
