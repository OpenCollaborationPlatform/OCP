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
	"github.com/ickby/CollaborationNode/utils"
	"bytes"
	"encoding/gob"
	"fmt"

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

func NewValueDatabase(db *boltWrapper) (*ValueDatabase, error) {

	//make sure key value store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("Value"))
		return nil
	})

	return &ValueDatabase{db, []byte("Value")}, nil
}

//implements the database interface
type ValueDatabase struct {
	db    *boltWrapper
	dbkey []byte
}

func (self ValueDatabase) HasSet(set [32]byte) (bool, error) {

	var result bool = false
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result, err
}

func (self ValueDatabase) GetOrCreateSet(set [32]byte) (Set, error) {

	if !self.db.CanAccess() {
		return nil, fmt.Errorf("No transaction open")
	}

	if has, _ := self.HasSet(set); !has {
		//make sure the bucket exists
		err := self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			_, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			return nil, utils.StackError(err, "Cannot create set")
		}
	}

	return &ValueSet{self.db, self.dbkey, [][]byte{set[:]}}, nil
}

func (self ValueDatabase) RemoveSet(set [32]byte) error {

	if has, _ := self.HasSet(set); has {

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
	db     *boltWrapper
	dbkey  []byte
	setkey [][]byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *ValueSet) IsValid() bool {

	var result bool = true
	err := self.db.View(func(tx *bolt.Tx) error {

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

	if err != nil {
		return false
	}

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
	res, err := value.HoldsValue()
	if err != nil {
		return false
	}
	return res
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

func (self *ValueSet) getKeys() ([][]byte, error) {

	entries := make([][]byte, 0)

	//iterate over all entries...
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}

		//collect the entries
		err := bucket.ForEach(func(k []byte, v []byte) error {

			//copy the key as it is not valid outside for each
			entries = append(entries, k)
			return nil
		})
		return err
	})

	return entries, err
}

/*
 * Value functions
 * ********************************************************************************
 */
type Value struct {
	db     *boltWrapper
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

	var res interface{}
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(self.key)
		if data == nil {
			return fmt.Errorf("Value was not set before read")
		}
		var err error 
		res, err = getInterface(data)
		return err
	})

	if err != nil {
		return nil, utils.StackError(err, "Unable to read value")
	}

	return res, nil
}

//returns true if:
//- setup correctly and able to write
//- value is not INVALID
//note that it does not mean that anything was written yet.
func (self *Value) IsValid() bool {

	valid := true
	err := self.db.View(func(tx *bolt.Tx) error {

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
		if isInvalid(data) {
			valid = false
		}
		return nil
	})
	if err != nil {
		return false
	}
	return valid
}

//return true if the value was already written, false otherwise
//Note that it also returns true if the value is INVALID
func (self *Value) HoldsValue() (bool, error) {

	var hasValue bool
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		hasValue = (bucket.Get(self.key) != nil)
		return nil
	})

	if err != nil {
		return false, utils.StackError(err, "Cannot check if value holds data or not")
	}
	return hasValue, nil
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

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func getInterface(bts []byte) (interface{}, error) {

	var res interface{}
	
	buf := bytes.NewBuffer(bts)
	dec := gob.NewDecoder(buf)

	if err := dec.Decode(&res); err != nil {
		return nil, err
	}
	
	return res, nil
}
