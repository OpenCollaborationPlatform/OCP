// Map database: The map database saves multiple maps per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/utils"

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

func NewMapDatabase(db *boltWrapper) (*MapDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("Map"))
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})

	if err != nil {
		return nil, err
	}

	return &MapDatabase{db, []byte("Map")}, nil
}

//implements the database interface
type MapDatabase struct {
	db    *boltWrapper
	dbkey []byte
}

func (self MapDatabase) HasSet(set [32]byte) (bool, error) {

	var result bool = false
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result, nil
}

func (self MapDatabase) GetOrCreateSet(set [32]byte) (Set, error) {

	if !self.db.CanAccess() {
		return nil, NewDSError(Error_Transaction_Invalid, "No transaction open")
	}

	if has, _ := self.HasSet(set); !has {
		//make sure the bucket exists
		err := self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			bucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	return &MapSet{self.db, self.dbkey, set[:]}, nil
}

func (self MapDatabase) RemoveSet(set [32]byte) error {

	if has, _ := self.HasSet(set); has {

		return self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			err := bucket.DeleteBucket(set[:])
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
	}

	return nil
}

func (self MapDatabase) Close() {

}

type MapSet struct {
	db     *boltWrapper
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
			kvset := ValueSet{self.db, self.dbkey, [][]byte{self.setkey, k}}
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
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})

		if err != nil {
			return nil, err
		}
	}

	mp := newMap(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &mp, nil
}

func (self *MapSet) GetEntry(key []byte) (Entry, error) {

	if !self.HasMap(key) {
		return nil, NewDSError(Error_Key_Not_Existant, "Map does not exist in Set", "Map", key)
	}

	return self.GetOrCreateMap(key)
}

/*
 * Map functions
 * ********************************************************************************
 */

type Map struct {
	kvset ValueSet
}

func newMap(db *boltWrapper, dbkey []byte, mapkeys [][]byte) Map {

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
		return utils.StackError(err, "Unable to access key in value set")
	}
	return utils.StackError(entry.Write(value), "Unable to write ds value")
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

	if !self.kvset.HasKey(k) {
		return nil, NewDSError(Error_Key_Not_Existant, "Key not available in map")
	}

	entry, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return nil, utils.StackError(err, "Unable to acces key in value set")
	}
	res, err := entry.Read()
	if err != nil {
		return nil, utils.StackError(err, "Unable to read ds value")
	}
	return res, err
}

func (self *Map) Remove(key interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	return utils.StackError(self.kvset.removeKey(k), "Unable to remove in ds value set")
}

func (self *Map) GetKeys() ([]interface{}, error) {

	bytekeys, err := self.kvset.getKeys()
	if err != nil {
		return nil, utils.StackError(err, "Unable to get keys from ds value set")
	}

	//convert from byte keys to user type keys
	keys := make([]interface{}, len(bytekeys))
	for i, bytekey := range bytekeys {
		key, err := getInterface(bytekey)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}

	return keys, nil
}

func (self *Map) SupportsSubentries() bool {
	return true
}

func (self *Map) GetSubentry(key interface{}) (Entry, error) {

	k, err := getBytes(key)
	if err != nil {
		return nil, err
	}

	if !self.kvset.HasKey(k) {
		return nil, NewDSError(Error_Key_Not_Existant, "Key not available in map")
	}

	return self.kvset.GetOrCreateValue(k)
}

func (self *Map) getMapKey() []byte {
	return self.kvset.getSetKey()
}

func (self *Map) Print(params ...int) {

	if len(params) > 0 {
		self.kvset.Print(params[0])
	} else {
		self.kvset.Print()
	}
}
