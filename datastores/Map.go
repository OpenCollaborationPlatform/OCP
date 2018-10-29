// Map database: The map database saves multiple maps per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
)

/*
Map database uses a keyvalue database underneath, just one level deeper in the
hirarchy.  Each ValueSet is a single map.

Data layout of versioned map store:

bucket(SetKey) [
	entry(CURRENT) = HEAD
	bucket(VERSIONS) [
		entry(1) = Versionmap(key1->1, key2->1)
		entry(2) = Versionmap(key1->2, key2->1)
	]
	ValueSet(key1)
	ValueSet(key2)
]
*/

func NewMapDatabase(db *bolt.DB) (*MapDatabase, error) {

	//make sure key value store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("map"))
		return nil
	})

	return &MapDatabase{db, []byte("map")}, nil
}

//implements the database interface
type MapDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self MapDatabase) HasSet(set [32]byte) bool {

	if self.db == nil {
		return false
	}

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result
}

func (self MapDatabase) GetOrCreateSet(set [32]byte) Set {

	if !self.HasSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			bucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			//setup the basic structure
			err = bucket.Put(itob(CURRENT), itob(HEAD))
			if err != nil {
				return err
			}
			_, err = bucket.CreateBucketIfNotExists(itob(VERSIONS))

			return nil
		})
	}

	return &MapSet{self.db, self.dbkey, set[:]}
}

func (self MapDatabase) RemoveSet(set [32]byte) error {

	if self.HasSet(set) {

		var result error
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			result = bucket.DeleteBucket(set[:])
			return nil
		})

		return result
	}

	return nil
}

func (self MapDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type MapSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey []byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *MapSet) IsValid() bool {

	return true
}

func (self *MapSet) Print() {

	if !self.IsValid() {
		fmt.Println("Invalid set")
		return
	}

	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			if bytes.Equal(k, itob(CURRENT)) {
				if btoi(v) == HEAD {
					fmt.Printf("CURRENT: HEAD\n")
				} else {
					fmt.Printf("CURRENT: %v\n", btoi(v))
				}

			} else if bytes.Equal(k, itob(VERSIONS)) {

				fmt.Println("VERSIONS:")
				//print the versions out
				subbucket := bucket.Bucket(k)
				subbucket.ForEach(func(sk []byte, sv []byte) error {
					inter, _ := getInterface(sv)
					data := inter.(map[string]interface{})
					//build the versioning string
					str := ""
					for mk, mv := range data {
						str = str + string(stob(mk)) + ": %v,  "
						mvid := stoi(mv.(string))
						if mvid == INVALID {
							str = fmt.Sprintf(str, "INVALID")
						} else {
							str = fmt.Sprintf(str, mvid)
						}
					}

					fmt.Printf("    %v: %v\n", btoi(sk), str)
					return nil
				})

			} else {

				kvset := ValueSet{self.db, self.dbkey, [][]byte{self.setkey, k}}
				kvset.Print()
			}
			return nil
		})
		return nil
	})

}

func (self *MapSet) FixStateAsVersion() (VersionID, error) {

	return VersionID(INVALID), nil
}

func (self *MapSet) getVersionInfo(id VersionID) (map[string]interface{}, error) {

	version := make(map[string]interface{})
	return version, nil
}

func (self *MapSet) LoadVersion(id VersionID) error {

	return nil
}

func (self *MapSet) GetLatestVersion() (VersionID, error) {

	return VersionID(INVALID), nil
}

func (self *MapSet) GetCurrentVersion() (VersionID, error) {

	var version uint64
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		val := bucket.Get(itob(CURRENT))
		if val == nil {
			return fmt.Errorf("No current version set")
		}
		version = btoi(val)
		return nil
	})

	return VersionID(version), err
}

func (self *MapSet) RemoveVersionsUpTo(ID VersionID) error {

	return nil
}

func (self *MapSet) RemoveVersionsUpFrom(ID VersionID) error {

	return nil
}

/*
 * Map functions
 * ********************************************************************************
 */
func (self *MapSet) HasMap(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		result = bucket.Bucket(key) != nil
		return nil
	})

	return result
}

func (self *MapSet) GetOrCreateMap(key []byte) (*Map, error) {

	if !self.HasMap(key) {

		curr, err := self.GetCurrentVersion()
		if err != nil {
			return nil, err
		}
		if !curr.IsHead() {
			return nil, fmt.Errorf("Key does not exist and cannot be created when version is loaded")
		}

		//make sure the set exists in the db with null value
		err = self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket(self.dbkey)
			bucket = bucket.Bucket(self.setkey)
			bucket, err = bucket.CreateBucketIfNotExists(key)
			if err != nil {
				return err
			}

			//setup the basic structure
			err = bucket.Put(itob(CURRENT), itob(HEAD))
			if err != nil {
				return err
			}
			_, err = bucket.CreateBucketIfNotExists(itob(VERSIONS))

			return err
		})

		if err != nil {
			return nil, err
		}
	}

	return newMap(self.db, self.dbkey, [][]byte{self.setkey, key}), nil
}

func (self *MapSet) RemoveMap(key []byte) error {

	return nil
}

/*
 * Map functions
 * ********************************************************************************
 */

type Map struct {
	kvset ValueSet
}

func newMap(db *bolt.DB, dbkey []byte, mapkeys [][]byte) *Map {

	kv := ValueSet{db, dbkey, mapkeys}
	return &Map{kv}
}

func (self *Map) Write(key []byte, value interface{}) error {

	pair, err := self.kvset.GetOrCreateKey(key)
	if err != nil {
		return err
	}
	return pair.Write(value)
}

func (self *Map) IsValid() bool {

	return self.kvset.IsValid()
}

func (self *Map) HasKey(key []byte) bool {

	return self.kvset.HasKey(key)
}

func (self *Map) Read(key []byte) (interface{}, error) {

	pair, err := self.kvset.GetOrCreateKey(key)
	if err != nil {
		return nil, err
	}
	return pair.Read()
}

func (self *Map) Remove(key []byte) bool {

	return self.kvset.RemoveKey(key) == nil
}

func (self *Map) CurrentVersion() VersionID {

	v, _ := self.kvset.GetCurrentVersion()
	return v
}

func (self *Map) LatestVersion() VersionID {

	v, _ := self.kvset.GetLatestVersion()
	return v
}
