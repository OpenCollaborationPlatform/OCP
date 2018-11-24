// List database: The list database saves multiple lists per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

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

func NewListDatabase(db *bolt.DB) (*ListDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("List"))
		return nil
	})

	return &ListDatabase{db, []byte("List")}, nil
}

//implements the database interface
type ListDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self ListDatabase) HasSet(set [32]byte) bool {

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

func (self ListDatabase) GetOrCreateSet(set [32]byte) Set {

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

	return &ListSet{self.db, self.dbkey, set[:]}
}

func (self ListDatabase) RemoveSet(set [32]byte) error {

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

func (self ListDatabase) Close() {

}

type ListSet struct {
	db     *bolt.DB
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

func newList(db *bolt.DB, dbkey []byte, listkeys [][]byte) List {

	kv := ValueSet{db, dbkey, listkeys}
	return List{kv}
}

func (self *List) Add(value interface{}) (uint64, error) {

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
		return 0, err
	}

	entry, err := self.kvset.GetOrCreateKey(itob(id))
	if err != nil {
		return 0, err
	}
	return id, entry.Write(value)
}

func (self *List) Set(id uint64, value interface{}) error {

	if !self.kvset.HasKey(itob(id)) {
		return fmt.Errorf("No such item available")
	}
	entry, err := self.kvset.GetOrCreateKey(itob(id))
	if err != nil {
		return err
	}
	return entry.Write(value)
}

func (self *List) IsValid() bool {

	return self.kvset.IsValid()
}

func (self *List) Read(id uint64) (interface{}, error) {

	if !self.kvset.HasKey(itob(id)) {
		return nil, fmt.Errorf("Item not avilable in List")
	}

	entry, err := self.kvset.GetOrCreateKey(itob(id))
	if err != nil {
		return nil, err
	}
	return entry.Read()
}

func (self *List) Remove(id uint64) error {

	if !self.kvset.HasKey(itob(id)) {
		return fmt.Errorf("Item not avilable in List")
	}
	return self.kvset.removeKey(itob(id))
}

func (self *List) getListKey() []byte {
	return self.kvset.getSetKey()
}
