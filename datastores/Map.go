// Map database: The map database saves multiple maps per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
)

/*
Map database uses a value database underneath, just one level deeper in the
hirarchy.  Each ValueSet is a single map.

Data layout of map store:

bucket(SetKey) [
	ValueSet(key1)
	ValueSet(key2)
]
*/

func NewMapDatabase(db *bolt.DB) (*MapDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("Map"))
		return nil
	})

	return &MapDatabase{db, []byte("Map")}, nil
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

func (self *MapSet) Print(params ...int) {

	if !self.IsValid() {
		fmt.Println("Invalid set")
		return
	}

	indent := ""
	if len(params) > 0 {
		for i := 0; i < params[0]; i++ {
			indent = indent + "\t"
		}
	}

	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			fmt.Println(string(k))
			kvset := ValueVersionedSet{self.db, self.dbkey, [][]byte{self.setkey, k}}
			if len(params) > 0 {
				kvset.Print(1 + params[0])
			} else {
				kvset.Print(1)
			}
			return nil
		})
		return nil
	})

}

func (self *MapSet) collectMaps() []Map {

	maps := make([]Map, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				//key must be copied, as it gets invalid outside of ForEach
				var key = make([]byte, len(k))
				copy(key, k)
				mp := newMap(self.db, self.dbkey, [][]byte{self.setkey, key})
				maps = append(maps, mp)
			}
			return nil
		})
		return nil
	})
	return maps
}

/*
 * Map functions
 * ********************************************************************************
 */
func (self MapSet) GetType() StorageType {
	return MapType
}

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

		//make sure the set exists in the db
		err := self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket(self.dbkey)
			bucket = bucket.Bucket(self.setkey)
			_, err := bucket.CreateBucketIfNotExists(key)
			return err
		})

		if err != nil {
			return nil, err
		}
	}

	mp := newMap(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &mp, nil
}

/*
 * Map functions
 * ********************************************************************************
 */

type Map struct {
	kvset ValueSet
}

func newMap(db *bolt.DB, dbkey []byte, mapkeys [][]byte) Map {

	kv := ValueSet{db, dbkey, mapkeys}
	return Map{kv}
}

func (self *Map) Write(key interface{}, value interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	entry, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return err
	}
	return entry.Write(value)
}

func (self *Map) IsValid() bool {

	return self.kvset.IsValid()
}

func (self *Map) HasKey(key interface{}) bool {

	k, err := getBytes(key)
	if err != nil {
		return false
	}
	return self.kvset.HasKey(k)
}

func (self *Map) Read(key interface{}) (interface{}, error) {

	k, err := getBytes(key)
	if err != nil {
		return nil, err
	}
	entry, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return nil, err
	}
	return entry.Read()
}

func (self *Map) ReadType(key interface{}, value interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	entry, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return err
	}
	return entry.ReadType(value)
}

func (self *Map) Remove(key interface{}) bool {

	k, err := getBytes(key)
	if err != nil {
		return false
	}
	return self.kvset.removeKey(k) == nil
}

func (self *Map) getMapKey() []byte {
	return self.kvset.getSetKey()
}
