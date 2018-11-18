// Index database: Stores entries by index (zero based) and keeps reorders on
// delete and entry to ensure index is always continuous 0 to length
package datastore

/*
To not rewrite all keys on insert/delete, we do not use the index as key for data.
We use normal iterate keys to store the data and have a INDEX Index that gives us the
ordering of those indexes to access the correct data.

bucket(SetKey) [
	entry(INDEX) = [2 0 1]
	entry(0) = "first Index"
	entry(1) = "second vale"
	entry(2) = "second vale"
]
*/

import (
	"fmt"
	"math"

	"github.com/boltdb/bolt"
)

const (
	INDEX uint64 = math.MaxUint64 - 11
)

func NewIndexDatabase(db *bolt.DB) (*IndexDatabase, error) {

	//make sure key Index store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("Index"))
		return nil
	})

	return &IndexDatabase{db, []byte("Index")}, nil
}

//implements the database interface
type IndexDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self IndexDatabase) HasSet(set [32]byte) bool {

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

func (self IndexDatabase) GetOrCreateSet(set [32]byte) Set {

	if !self.HasSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			_, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			return nil
		})
	}

	return &IndexSet{self.db, self.dbkey, [][]byte{set[:]}}
}

func (self IndexDatabase) RemoveSet(set [32]byte) error {

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

func (self IndexDatabase) Close() {

}

type IndexSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *IndexSet) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool = true
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		if bucket == nil {
			result = false
			return nil
		}
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
			if bucket == nil {
				result = false
				return nil
			}
		}
		return nil
	})

	return result
}

func (self *IndexSet) Print(params ...int) {

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
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}

		bucket.ForEach(func(k []byte, v []byte) error {
			inter, _ := getInterface(v)
			fmt.Printf("%s\t%v: %v\n", indent, k, inter)
			return nil
		})
		return nil
	})
}

/*
 * Index functions
 * ********************************************************************************
 */
func (self IndexSet) GetType() StorageType {
	return IndexType
}

func (self *IndexSet) HasKey(key []byte) bool {

	Index := Index{self.db, self.dbkey, self.setkey, key}
	return Index.IsValid()
}

func (self *IndexSet) GetOrCreateKey(key []byte) (*Index, error) {

	//we create it by creating the relevant bucket
	if !self.HasKey(key) {
		err := self.db.Update(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(self.dbkey)
			for _, bkey := range self.setkey {
				bucket = bucket.Bucket(bkey)
			}
			newbucket, err := bucket.CreateBucketIfNotExists(key)
			if err != nil {
				return err
			}

			//setup default structure
			data, err := getBytes(make([]uint64, 0))
			if err != nil {
				return err
			}
			return newbucket.Put(itob(INDEX), data)

		})
		if err != nil {
			return nil, err
		}
	}

	return &Index{self.db, self.dbkey, self.setkey, key}, nil
}

func (self *IndexSet) removeKey(key []byte) error {

	if !self.HasKey(key) {
		return fmt.Errorf("key does not exists, cannot be removed")
	}
	Index, err := self.GetOrCreateKey(key)
	if err != nil {
		return err
	}
	if Index.remove() != nil {
		return fmt.Errorf("Unable to remove key")
	}
	return nil
}

func (self *IndexSet) getSetKey() []byte {
	return self.setkey[len(self.setkey)-1]
}

/*
 * Index functions
 * ********************************************************************************
 */
type Index struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
	key    []byte
}

func (self *Index) Length() (int, error) {

	index, err := self.getIndex()
	return len(index), err
}

func (self *Index) Write(idx int, value interface{}) error {

	index, err := self.getIndex()
	if err != nil {
		return err
	}

	if idx > (len(index) - 1) {
		return fmt.Errorf("Index out of bounds")
	}

	//store the data first (and remove the old value)
	seq := index[idx]
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		//remove old
		err := bucket.Delete(itob(seq))
		if err != nil {
			return err
		}
		//write new
		seq, err = bucket.NextSequence()
		if err != nil {
			return err
		}
		bts, err := getBytes(value)
		if err != nil {
			return err
		}
		return bucket.Put(itob(seq), bts)
	})
	if err != nil {
		return err
	}

	//update the index and store it!
	index[idx] = seq
	return self.setIndex(index)
}

func (self *Index) Read(idx int) (interface{}, error) {

	index, err := self.getIndex()
	if err != nil {
		return nil, err
	}

	if idx > (len(index) - 1) {
		return nil, fmt.Errorf("Index out of bounds")
	}

	//get the relevant sequence id
	seq := index[idx]

	var result interface{}
	err = self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(itob(seq))
		if data == nil {
			return fmt.Errorf("Index was not set before read")
		}
		res, err := getInterface(data)
		result = res
		return err
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (self *Index) IsValid() bool {

	if self.db == nil {
		return false
	}

	valid := true
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		if bucket == nil {
			valid = false
			return nil
		}
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
			if bucket == nil {
				valid = false
				return nil
			}
		}
		return nil
	})

	return valid
}

func (self *Index) remove() error {

	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Delete(self.key)
	})

	return err
}

func (self *Index) getIndex() ([]uint64, error) {

	var index []uint64
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(itob(INDEX))
		if data == nil {
			return fmt.Errorf("No index stored")
		}
		val, err := indexFromBytes(data)
		if err != nil {
			return err
		}
		index = val
		return nil
	})

	return index, err
}

func (self *Index) setIndex(index []uint64) error {

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		bts, err := getBytes(index)
		if err != nil {
			return err
		}
		return bucket.Put(itob(INDEX), bts)
	})
}

func indexFromBytes(bts []byte) ([]uint64, error) {

	res, err := getInterface(bts)
	if err != nil {
		return nil, err
	}
	idx, ok := res.([]uint64)
	if !ok {
		return nil, fmt.Errorf("Unable to load index from data")
	}
	return idx, nil
}
