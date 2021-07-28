// List database: The list database saves multiple lists per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/utils"

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
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})

	return &ListDatabase{db, []byte("List")}, err
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

	return &ListSet{self.db, self.dbkey, set[:]}, nil
}

func (self ListDatabase) RemoveSet(set [32]byte) error {

	if has, _ := self.HasSet(set); has {

		err := self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			err := bucket.DeleteBucket(set[:])
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
		return err
	}

	return NewDSError(Error_Key_Not_Existant, "No such set available")
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

func (self *ListSet) RemoveList(key []byte) error {

	err := self.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		return bucket.DeleteBucket(key)
	})

	if err != nil {
		return wrapDSError(err, Error_Bolt_Access_Failure)
	}
	return nil
}

func (self *ListSet) GetOrCreateList(key []byte) (*List, error) {

	if !self.HasList(key) {

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

	mp := newList(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &mp, nil
}

func (self *ListSet) GetEntry(key []byte) (Entry, error) {

	if !self.HasList(key) {
		return nil, NewDSError(Error_Key_Not_Existant, "Set does not have list", "List", key)
	}

	return self.GetOrCreateList(key)
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

func (self *List) Add(value interface{}) (ListValue, error) {

	var id uint64
	err := self.kvset.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}
		val, err := bucket.NextSequence()
		if err != nil {
			return wrapDSError(err, Error_Bolt_Access_Failure)
		}
		id = val
		return nil
	})
	if err != nil {
		return nil, err
	}

	entry, err := self.kvset.GetOrCreateValue(itob(id))
	if err != nil {
		return nil, utils.StackError(err, "Unable to get or create value Set")
	}
	return &listValue{*entry}, utils.StackError(entry.Write(value), "Unable to write value")
}

func (self *List) GetValues() ([]ListValue, error) {

	entries := make([]ListValue, 0)

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
			entries = append(entries, &listValue{value})
			return nil
		})
		return err
	})

	return entries, wrapDSError(err, Error_Bolt_Access_Failure)
}

//returns first value. If no existant, does not error, but returns invalid ListValue
func (self *List) First() (ListValue, error) {

	var value ListValue = &listValue{}
	err := self.kvset.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}

		cursor := bucket.Cursor()
		retKey, _ := cursor.First()
		value = &listValue{Value{self.kvset.db, self.kvset.dbkey, self.kvset.setkey, retKey}}
		return nil
	})

	return value, err
}

func (self *List) Last() (ListValue, error) {

	var value ListValue
	err := self.kvset.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}

		cursor := bucket.Cursor()
		retKey, _ := cursor.Last()
		value = &listValue{Value{self.kvset.db, self.kvset.dbkey, self.kvset.setkey, retKey}}
		return nil
	})

	return value, err
}

func (self *List) HasReference(ref uint64) bool {
	//check if exist
	var exists bool = false
	self.kvset.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}

		val := bucket.Get(itob(ref))
		exists = (val != nil)
		return nil
	})
	return exists
}

func (self *List) getListKey() []byte {
	return self.kvset.getSetKey()
}

func (self *List) SupportsSubentries() bool {
	return true
}

func (self *List) GetSubentry(key interface{}) (Entry, error) {

	var id uint64
	switch k := key.(type) {
	case int:
		id = uint64(k)
	case int8:
		id = uint64(k)
	case int16:
		id = uint64(k)
	case int32:
		id = uint64(k)
	case uint:
		id = uint64(k)
	case uint8:
		id = uint64(k)
	case uint16:
		id = uint64(k)
	case uint32:
		id = uint64(k)
	case uint64:
		id = uint64(k)
	case ListValue:
		id = k.(ListValue).Reference()
	default:
		return nil, NewDSError(Error_Operation_Invalid, "Subentry key must be integer or ListValue type")
	}

	if !self.HasReference(id) {
		return nil, NewDSError(Error_Key_Not_Existant, "List does not have value with given reference")
	}

	return &listValue{Value{self.kvset.db, self.kvset.dbkey, self.kvset.setkey, itob(id)}}, nil
}

/*
 * List entries functions
 * ********************************************************************************
 */
type ListValue interface {
	Entry
	Write(value interface{}) error
	Read() (interface{}, error)
	IsValid() bool
	Remove() error
	Reference() uint64
	Previous() (ListValue, error)
	Next() (ListValue, error)
}

type listValue struct {
	value Value
}

func (self *listValue) Write(value interface{}) error {
	return utils.StackError(self.value.Write(value), "Unable to write ds value")
}

func (self *listValue) Read() (interface{}, error) {
	val, err := self.value.Read()
	return val, utils.StackError(err, "Unable to read ds value")
}

func (self *listValue) IsValid() bool {
	return self.value.IsValid()
}

func (self *listValue) Remove() error {
	return utils.StackError(self.value.remove(), "Unable to remove ds value")
}

func (self *listValue) Reference() uint64 {
	return btoi(self.value.key)
}

//returns previous value. If no existant, does not error, but returns nil
func (self *listValue) Previous() (ListValue, error) {

	var value ListValue = nil
	err := self.value.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.value.dbkey)
		for _, bkey := range self.value.setkey {
			bucket = bucket.Bucket(bkey)
		}

		cursor := bucket.Cursor()
		retKey, _ := cursor.Seek(self.value.key)
		if retKey == nil {
			return NewDSError(Error_Setup_Incorrectly, "List value seems not to exist in List")
		}
		retKey, _ = cursor.Prev()
		if retKey == nil {
			return nil
		}

		value = &listValue{Value{self.value.db, self.value.dbkey, self.value.setkey, retKey}}
		return nil
	})

	return value, err
}

//returns next value. If no existant, does not error, but returns nil
func (self *listValue) Next() (ListValue, error) {

	var value ListValue = nil
	err := self.value.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.value.dbkey)
		for _, bkey := range self.value.setkey {
			bucket = bucket.Bucket(bkey)
		}

		cursor := bucket.Cursor()
		retKey, _ := cursor.Seek(self.value.key)
		if retKey == nil {
			return NewDSError(Error_Setup_Incorrectly, "List value seems not to exist in List")
		}
		retKey, _ = cursor.Next()
		if retKey == nil {
			return nil
		}

		value = &listValue{Value{self.value.db, self.value.dbkey, self.value.setkey, retKey}}
		return nil
	})

	return value, err
}

func (self *listValue) SupportsSubentries() bool {
	return false
}
func (self *listValue) GetSubentry(interface{}) (Entry, error) {
	return nil, NewDSError(Error_Operation_Invalid, "ListValue does not support subentries")
}
