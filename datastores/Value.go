// Value database: The key value database saves the multiple entries per set which
// are accessed by keys.
package datastore

/*
Data layout of versioned key value store:

bucket(SetKey) [
	entry(1) = "first value"
	entry(2) = "second vale"
]
*/

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"

	"github.com/boltdb/bolt"
)

var INVALID_VALUE = make([]byte, 0)

//True if val is INVALID_VALUE. False if any other (including nil)
func isInvalid(val []byte) bool {

	//nil is valid
	if val == nil {
		return false
	}
	return bytes.Equal(val, INVALID_VALUE)
}

func NewValueDatabase(db *bolt.DB) (*ValueDatabase, error) {

	//make sure key value store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("Value"))
		return nil
	})

	return &ValueDatabase{db, []byte("Value")}, nil
}

//implements the database interface
type ValueDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self ValueDatabase) HasSet(set [32]byte) bool {

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

func (self ValueDatabase) GetOrCreateSet(set [32]byte) Set {

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

	return &ValueSet{self.db, self.dbkey, [][]byte{set[:]}}
}

func (self ValueDatabase) RemoveSet(set [32]byte) error {

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

func (self ValueDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type ValueSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *ValueSet) IsValid() bool {

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

func (self *ValueSet) Print(params ...int) {

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

			//check if it is a number instead of string
			if len(k) == 8 {
				num := btoi(k)
				fmt.Printf("%s\t%v: %v\n", indent, num, inter)
			}
			//print as string
			fmt.Printf("%s\t%v: %v\n", indent, k, inter)
			return nil
		})
		return nil
	})
}

/*
 * Value functions
 * ********************************************************************************
 */
func (self ValueSet) GetType() StorageType {
	return ValueType
}

func (self *ValueSet) HasKey(key []byte) bool {

	value := Value{self.db, self.dbkey, self.setkey, key}
	return value.IsValid()
}

func (self *ValueSet) GetOrCreateValue(key []byte) (*Value, error) {

	//we create it by writing empty data into it
	if !self.HasKey(key) {
		err := self.db.Update(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(self.dbkey)
			for _, bkey := range self.setkey {
				bucket = bucket.Bucket(bkey)
			}
			return bucket.Put(key, make([]byte, 0))
		})
		if err != nil {
			return nil, err
		}
	}

	return &Value{self.db, self.dbkey, self.setkey, key}, nil
}

func (self *ValueSet) removeKey(key []byte) error {

	if !self.HasKey(key) {
		return fmt.Errorf("key does not exists, cannot be removed")
	}
	value, err := self.GetOrCreateValue(key)
	if err != nil {
		return err
	}
	if value.remove() != nil {
		return fmt.Errorf("Unable to remove key")
	}
	return nil
}

func (self *ValueSet) getSetKey() []byte {
	return self.setkey[len(self.setkey)-1]
}

/*
 * Value functions
 * ********************************************************************************
 */
type Value struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
	key    []byte
}

func (self *Value) Write(value interface{}) error {

	bts, err := getBytes(value)
	if err != nil {
		return err
	}

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Put(self.key, bts)
	})
}

func (self *Value) Read() (interface{}, error) {

	var result interface{}
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(self.key)
		if data == nil {
			return fmt.Errorf("Value was not set before read")
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

func (self *Value) IsValid() bool {

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
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
			if bucket == nil {
				valid = false
				return nil
			}
		}
		data := bucket.Get(self.key)
		if data == nil {
			valid = false
			return nil
		}
		return nil
	})

	return valid
}

func (self *Value) remove() error {

	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Delete(self.key)
	})

	return err
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
