// List database: The list database saves multiple lists per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"
	"github.com/ickby/CollaborationNode/utils"

	"github.com/boltdb/bolt"
)

/*
List database uses a value database underneath, just one level deeper in the
hirarchy.  Each ValueSet is a single list.

Data layout of list store:

bucket(SetKey) [
	ValueSet(key1)
	ValueSet(key2)
]
*/

func NewListDatabase(db *boltWrapper) (*ListDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("List"))
		return err
	})

	if err != nil {
		return nil, utils.StackError(err, "Cannot create bucket \"List\"")
	}

	return &ListDatabase{db, []byte("List")}, nil
}

//implements the database interface
type ListDatabase struct {
	db    *boltWrapper
	dbkey []byte
}

func (self ListDatabase) HasSet(set [32]byte) (bool, error) {

	var result bool = false
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result, err
}

func (self ListDatabase) GetOrCreateSet(set [32]byte) (Set, error) {

	if !self.db.CanAccess() {
		return nil, fmt.Errorf("No transaction open")
	}

	if has, _ := self.HasSet(set); !has {
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

	return &ListSet{self.db, self.dbkey, set[:]}, nil
}

func (self ListDatabase) RemoveSet(set [32]byte) error {

	if has, _ := self.HasSet(set); has {

		return self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			return bucket.DeleteBucket(set[:])
		})
	}

	return fmt.Errorf("no such set available")
}

func (self ListDatabase) Close() {

}

type ListSet struct {
	db     *boltWrapper
	dbkey  []byte
	setkey []byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *ListSet) IsValid() bool {

	return true
}

func (self *ListSet) Print(params ...int) {

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

func (self *ListSet) collectLists() []List {

	lists := make([]List, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				//key must be copied, as it gets invalid outside of ForEach
				var key = make([]byte, len(k))
				copy(key, k)
				mp := newList(self.db, self.dbkey, [][]byte{self.setkey, key})
				lists = append(lists, mp)
			}
			return nil
		})
		return nil
	})
	return lists
}

/*
 * List functions
 * ********************************************************************************
 */
func (self ListSet) GetType() StorageType {
	return ListType
}

func (self *ListSet) HasList(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		result = bucket.Bucket(key) != nil
		return nil
	})

	return result
}

func (self *ListSet) GetOrCreateList(key []byte) (*List, error) {

	if !self.HasList(key) {

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

	mp := newList(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &mp, nil
}

/*
 * List functions
 * ********************************************************************************
 */

type List struct {
	kvset ValueSet
}

func newList(db *boltWrapper, dbkey []byte, listkeys [][]byte) List {

	kv := ValueSet{db, dbkey, listkeys}
	return List{kv}
}

func (self *List) Add(value interface{}) (ListEntry, error) {

	var id uint64
	err := self.kvset.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}
		val, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		id = val
		return nil
	})
	if err != nil {
		return nil, err
	}

	entry, err := self.kvset.GetOrCreateValue(itob(id))
	if err != nil {
		return nil, err
	}
	return &listEntry{*entry}, entry.Write(value)
}

func (self *List) GetEntries() ([]ListEntry, error) {

	entries := make([]ListEntry, 0)

	//iterate over all entries...
	err := self.kvset.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}

		//collect the entries
		err := bucket.ForEach(func(k []byte, v []byte) error {

			//ceck if it is a valid key or if it was removed
			value := Value{self.kvset.db, self.kvset.dbkey, self.kvset.setkey, k}
			if !value.IsValid() {
				return nil
			}

			//copy the key as it is not valid outside for each
			var key = make([]byte, len(k))
			copy(key, k)
			value.key = key

			//add to the list
			entries = append(entries, &listEntry{value})
			return nil
		})
		return err
	})

	return entries, err
}

func (self *List) getListKey() []byte {
	return self.kvset.getSetKey()
}

/*
 * List entries functions
 * ********************************************************************************
 */
type ListEntry interface {
	Write(value interface{}) error
	Read() (interface{}, error)
	IsValid() bool
	Remove() error
	Id() uint64
}

type listEntry struct {
	value Value
}

func (self *listEntry) Write(value interface{}) error {
	return self.value.Write(value)
}

func (self *listEntry) Read() (interface{}, error) {
	return self.value.Read()
}

func (self *listEntry) IsValid() bool {
	return self.value.IsValid()
}

func (self *listEntry) Remove() error {
	return self.value.remove()
}

func (self *listEntry) Id() uint64 {
	return btoi(self.value.key)
}
