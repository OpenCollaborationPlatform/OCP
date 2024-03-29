// ValueVersioned database: The key valueVersioned database saves the multiple entries per set which
// are accessed by keys.
package datastore

/*
Data layout of versioned key valueVersioned store:

bucket(SetKey) [
	entry(CURRENT) = HEAD
	bucket(VERSIONS) [
		entry(1) = Versionmap(key1->1, key2->1)
		entry(2) = Versionmap(key1->2, key2->1)
	]
	bucket(key1) [
		entry(1) = "first valueVersioned"
		entry(2) = "second vale"
		entry(CURRENT) = HEAD
		entry(HEAD) = "current valueVersioned"
	]
	bucket(key2) [
		entry(1) = 14
		entry(CURRENT) = HEAD
		entry(HEAD) = 29
	]
]

Definitions:
- CURRENT 	in keys gives the used version number. It is set to INVALID if it does
         	not has a valid version for the checked out key version
- HEAD    	gives the currently worked on valueVersioned. It is set to []byte{} and hence made
		  	invalid on removal
- Bucket sequence: Is alwas on the latest version existing in the bucket

Lifecycle in checked out version:
- CURRENT = INVALID: it was not created yet in current version, exist = false, IsValid = false
- CURRENT = HEAD, HEAD = nil: it was created, but not written yet: exist = true, IsValid = false
- CURRENT = VERSION, VERSION = INVALID_DATA: it was removed in this version, exist = false, IsValid = false
- CURRENT = VERSION, VERSION = data: it exists in this version and is valid, exist = true, IsValid = true
*/

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/boltdb/bolt"
	"github.com/mr-tron/base58/base58"
)

const (
	CURRENT  uint64 = math.MaxUint64 - 10
	VERSIONS uint64 = math.MaxUint64 - 11
)

func init() {
	gob.Register(new(map[string]string))
}

func NewValueVersionedDatabase(db *boltWrapper) (*ValueVersionedDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("ValueVersioned"))
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
	if err != nil {
		return nil, err
	}

	return &ValueVersionedDatabase{db, []byte("ValueVersioned")}, nil
}

//implements the database interface
type ValueVersionedDatabase struct {
	db    *boltWrapper
	dbkey []byte
}

func (self ValueVersionedDatabase) HasSet(set [32]byte) (bool, error) {

	var result bool = false
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result, err
}

func (self ValueVersionedDatabase) GetOrCreateSet(set [32]byte) (Set, error) {

	if !self.db.CanAccess() {
		return nil, fmt.Errorf("No transaction open")
	}

	if has, _ := self.HasSet(set); !has {
		//make sure the bucket exists
		err := self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			newbucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}

			//set default valueVersioneds
			if err := newbucket.Put(itob(CURRENT), itob(HEAD)); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
			_, err = newbucket.CreateBucketIfNotExists(itob(VERSIONS))
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
		if err != nil {
			return nil, err
		}
	}

	return &ValueVersionedSet{self.db, self.dbkey, [][]byte{set[:]}}, nil
}

func (self ValueVersionedDatabase) RemoveSet(set [32]byte) error {

	if !self.db.CanAccess() {
		return NewDSError(Error_Transaction_Invalid, "No transaction open")
	}

	if has, _ := self.HasSet(set); has {

		return self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			err := bucket.DeleteBucket(set[:])
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
	}

	return nil
}

func (self ValueVersionedDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type ValueVersionedSet struct {
	db     *boltWrapper
	dbkey  []byte
	setkey [][]byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *ValueVersionedSet) IsValid() bool {

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

func (self *ValueVersionedSet) Print(params ...int) {

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

			if bytes.Equal(k, itob(CURRENT)) {
				if btoi(v) == HEAD {
					fmt.Printf("%sCURRENT: HEAD\n", indent)
				} else {
					fmt.Printf("%sCURRENT: %v\n", indent, btoi(v))
				}

			} else if bytes.Equal(k, itob(VERSIONS)) {
				fmt.Printf("%sVERSIONS:\n", indent)
				//print the versions out
				subbucket := bucket.Bucket(k)
				subbucket.ForEach(func(sk []byte, sv []byte) error {
					inter, _ := getInterface(sv)
					//build the versioning string
					str := "["
					data, ok := inter.(map[string]string)
					if ok {
						for mk, mv := range data {
							byt := stob(mk)
							if len(byt) == 8 {
								str = str + fmt.Sprintf("%v", btoi(byt)) + ": %v,  "
							} else {
								str = str + string(stob(mk)) + ": %v,  "
							}
							mvid := stoi(mv)
							if mvid == INVALID {
								str = fmt.Sprintf(str, "INVALID")
							} else {
								str = fmt.Sprintf(str, mvid)
							}
						}
					}
					str = str + "]"
					fmt.Printf("%s\t%v: %v\n", indent, btoi(sk), str)
					return nil
				})
			} else {
				//check if it is a number instead of string
				if len(k) == 8 {
					num := btoi(k)
					fmt.Printf("%s%v:\n", indent, num)
				} else {
					fmt.Printf("%s%s:\n", indent, string(k))
				}
				subbucket := bucket.Bucket(k)
				subbucket.ForEach(func(sk []byte, sv []byte) error {
					var inter string
					if sv == nil {
						inter = "nil"
					} else if isInvalid(sv) {
						inter = "INVALID_DATA"
					} else {
						t, _ := getInterface(sv)
						inter = fmt.Sprintf("%v", t)
					}

					key := btoi(sk)
					if key == HEAD {
						fmt.Printf("%s\tHEAD: %v\n", indent, inter)
					} else if key == CURRENT {
						if btoi(sv) == HEAD {
							fmt.Printf("%s\tCURRENT: HEAD\n", indent)
						} else if btoi(sv) == INVALID {
							fmt.Printf("%s\tCURRENT: INVALID\n", indent)
						} else {
							fmt.Printf("%s\tCURRENT: %v\n", indent, btoi(sv))
						}
					} else {
						fmt.Printf("%s\t%v: %v\n", indent, key, inter)
					}
					return nil
				})
			}
			return nil
		})

		return nil
	})
}

func (self *ValueVersionedSet) collectValueVersioneds() []ValueVersioned {

	valueVersioneds := make([]ValueVersioned, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				key := make([]byte, len(k))
				copy(key, k)
				val := ValueVersioned{self.db, self.dbkey, self.setkey, key}
				valueVersioneds = append(valueVersioneds, val)
			}
			return nil
		})
		return nil
	})
	return valueVersioneds
}

func (self *ValueVersionedSet) HasUpdates() (bool, error) {

	//if no versions available yet we always have updates!
	ups, err := self.HasVersions()
	if err != nil {
		return false, utils.StackError(err, "Unable to determine if verions exist")
	}

	updates := !ups
	//check if the individual valueVersioneds have updates
	if !updates {
		valueVersioneds := self.collectValueVersioneds()
		for _, val := range valueVersioneds {
			if has, _ := val.HasUpdates(); has {
				return true, nil
			}
		}
	}

	return updates, nil
}

func (self *ValueVersionedSet) HasVersions() (bool, error) {

	var versions bool
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(bkey)
		}
		versions = (bucket.Sequence() != 0)
		return nil
	})

	return versions, err
}

func (self *ValueVersionedSet) ResetHead() error {

	if !self.db.CanAccess() {
		return NewDSError(Error_Transaction_Invalid, "No transaction open")
	}

	valueVersioneds := self.collectValueVersioneds()
	for _, val := range valueVersioneds {

		if has, _ := val.HasVersions(); has {
			err := val.ResetHead()
			if err != nil {
				return utils.StackError(err, "Unable to reset value")
			}

		} else {
			//if no version was ever create in this value, it does not exist in the HEAD version.
			//Hence we can fully delete it.
			//"remove" does not delete bucket, hence we do it manually
			err := self.db.Update(func(tx *bolt.Tx) error {

				bucket := tx.Bucket(self.dbkey)
				for _, bkey := range self.setkey {
					bucket = bucket.Bucket(bkey)
				}
				err := bucket.DeleteBucket(val.key)
				return wrapDSError(err, Error_Bolt_Access_Failure)
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *ValueVersionedSet) FixStateAsVersion() (VersionID, error) {
	//check if opertion is possible
	cv, err := self.GetCurrentVersion()
	if err != nil {
		return VersionID(INVALID), utils.StackError(err, "Current version not accessible")
	}
	if !cv.IsHead() {
		return VersionID(INVALID), NewDSError(Error_Operation_Invalid, "Unable to create version if HEAD is not checked out")
	}

	//we iterate over all entries and get the sequence number to store as current
	//state
	version := make(map[string]string)
	values := self.collectValueVersioneds()
	for _, val := range values {
		v, err := val.FixStateAsVersion()
		if err != nil {
			return VersionID(INVALID), err
		}
		version[btos(val.key)] = itos(uint64(v))
	}

	//write the new version into store
	var currentVersion uint64
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(bkey)
		}
		data, err := getBytes(version)
		if err != nil {
			return err
		}
		currentVersion, err = bucket.NextSequence()
		if err != nil {
			return wrapDSError(err, Error_Bolt_Access_Failure)
		}
		err = bucket.Put(itob(currentVersion), data)
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})

	if err != nil {
		return VersionID(INVALID), err
	}

	return VersionID(currentVersion), nil
}

func (self *ValueVersionedSet) getVersionInfo(id VersionID) (map[string]string, error) {

	version := make(map[string]string)
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, sk := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(sk)
		}
		data := bucket.Get(itob(uint64(id)))
		if data == nil || len(data) == 0 {
			return NewDSError(Error_Key_Not_Existant, "Version does not exist")
		}
		res, err := getInterface(data)
		if err != nil {
			return err
		}
		resmap, ok := res.(*map[string]string)
		if !ok {
			return NewDSError(Error_Invalid_Data, "Problem with parsing the saved version data")
		}
		version = *resmap
		return nil
	})
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (self *ValueVersionedSet) LoadVersion(id VersionID) error {

	if cv, _ := self.GetCurrentVersion(); cv == id {
		return nil
	}

	//grab the needed verion
	var version map[string]string
	if !id.IsHead() {
		var err error
		version, err = self.getVersionInfo(id)
		if err != nil {
			return utils.StackError(err, "Unable to get version Info")
		}
	}

	//make sure all subentries have loaded the correct subversion
	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}

		//it could happen that the loaded version does not include some entries.
		//to catch them we need to go thorugh all entires that are versionized
		c := bucket.Cursor()
		for key, val := c.First(); key != nil; key, val = c.Next() {

			if !bytes.Equal(key, itob(VERSIONS)) && val == nil {

				data := base58.Encode(key)
				subbucket := bucket.Bucket(key)

				var err error
				if id.IsHead() {
					err = subbucket.Put(itob(CURRENT), itob(HEAD))

				} else {

					vers, ok := version[data]

					if !ok {
						err = subbucket.Put(itob(CURRENT), itob(INVALID))
					} else {
						err = subbucket.Put(itob(CURRENT), stob(vers))
					}
				}
				if err != nil {
					return wrapDSError(err, Error_Bolt_Access_Failure)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	//we write the current version
	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}
		err := bucket.Put(itob(CURRENT), itob(uint64(id)))
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
}

func (self *ValueVersionedSet) GetLatestVersion() (VersionID, error) {

	//read the last version we created
	var version uint64 = 0
	found := false
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			found = true
			if val > version {
				version = val
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return VersionID(INVALID), err
	}

	if !found {
		return VersionID(INVALID), NewDSError(Error_Operation_Invalid, "No versions availble")
	}

	return VersionID(version), nil
}

func (self *ValueVersionedSet) GetCurrentVersion() (VersionID, error) {

	//read the last version we created
	var currentVersion uint64 = INVALID
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}
		versdata := bucket.Get(itob(CURRENT))
		if versdata == nil {
			return NewDSError(Error_Setup_Incorrectly, "Could not access sets version information")
		}
		currentVersion = btoi(versdata)
		return nil
	})
	return VersionID(currentVersion), err
}

func (self *ValueVersionedSet) RemoveVersionsUpTo(ID VersionID) error {

	cv, _ := self.GetCurrentVersion()
	if cv < ID {
		return NewDSError(Error_Operation_Invalid, "Cannot delete currently loaded version")
	}

	//get the version info
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return utils.StackError(err, "Unable to get version info for %v", ID)
	}

	//remove everything that is not needed anymore
	deleted_keys := make([]string, 0)
	for key, valueVersioned := range version {

		valueVersioneddata := stoi(valueVersioned)
		keydata := stob(key)

		//check what is the values latest version
		value := ValueVersioned{self.db, self.dbkey, self.setkey, keydata}

		//delete what is not needed anymore: the whole bucket or subentries
		err := value.RemoveVersionsUpTo(VersionID(valueVersioneddata))
		if err != nil {
			return err
		}

		//check if the entry was fully deleted
		if !value.IsValid() {
			deleted_keys = append(deleted_keys, valueVersioned)
		}
	}

	//with all data cleared we can delete the version entries
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(sk)
		}

		todelete := make([][]byte, 0)
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val < uint64(ID) {
				//deep copy of key, as the slice is invalid outside foreach
				keycopy := make([]byte, len(k))
				copy(keycopy, k)
				todelete = append(todelete, keycopy)
			}
			return nil
		})
		for _, k := range todelete {
			if err := bucket.Delete(k); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}
		return nil
	})

	//rewrite all existing versions to not include deleted keys
	latest, err := self.GetLatestVersion()
	if err != nil {
		return utils.StackError(err, "Unable to access latest version")
	}
	for i := uint64(ID); i <= uint64(latest); i++ {
		version, err := self.getVersionInfo(VersionID(i))
		if err != nil {
			return utils.StackError(err, "Unable to get version info for %v", i)
		}

		//remove all deleted keys from version map
		for _, key := range deleted_keys {
			delete(version, key)
		}

		//write version map back
		err = self.db.Update(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(self.dbkey)
			for _, bkey := range append(self.setkey, itob(VERSIONS)) {
				bucket = bucket.Bucket(bkey)
			}
			data, err := getBytes(version)
			if err != nil {
				return err
			}
			err = bucket.Put(itob(i), data)
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *ValueVersionedSet) RemoveVersionsUpFrom(ID VersionID) error {

	cv, _ := self.GetCurrentVersion()
	if cv > ID {
		return NewDSError(Error_Operation_Invalid, "Cannot delete currently loaded version")
	}

	//get the version info
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return utils.StackError(err, "Unable to access version info for %v", ID)
	}

	values := self.collectValueVersioneds()
	toDelete := make([]ValueVersioned, 0)
	for _, value := range values {

		ifval, ok := version[btos(value.key)]
		if !ok {
			//if the value does not yet exist the provided version we can fully delete it
			toDelete = append(toDelete, value)
		} else {

			//we are already available in the given version. But we can delete
			//all versions that are too new
			if err := value.RemoveVersionsUpFrom(VersionID(stoi(ifval))); err != nil {
				return err
			}
		}
	}

	//delete all not required values
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		for _, value := range toDelete {
			if err := bucket.DeleteBucket(value.key); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	//with all data cleared we can delete the version entries
	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(sk)
		}

		todelete := make([][]byte, 0)
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val > uint64(ID) {
				//deep copy of key, as the slice is invalid outside foreach
				keycopy := make([]byte, len(k))
				copy(keycopy, k)
				todelete = append(todelete, keycopy)
			}
			return nil
		})
		for _, k := range todelete {
			if err := bucket.Delete(k); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}

		return nil
	})
}

func (self *ValueVersionedSet) getKeys() ([][]byte, error) {

	keys := make([][]byte, 0)
	values := self.collectValueVersioneds()
	for _, value := range values {
		if value.IsValid() {
			keys = append(keys, value.key)
		}
	}

	return keys, nil
}

func (self *ValueVersionedSet) getValues() ([]ValueVersioned, error) {

	result := make([]ValueVersioned, 0)
	values := self.collectValueVersioneds()
	for _, value := range values {
		if value.IsValid() {
			result = append(result, value)
		}
	}

	return result, nil
}

/*
 * Key-ValueVersioned functions
 * ********************************************************************************
 */
func (self ValueVersionedSet) GetType() StorageType {
	return ValueType
}

func (self *ValueVersionedSet) HasKey(key []byte) bool {

	pair := ValueVersioned{self.db, self.dbkey, self.setkey, key}
	ok, _ := pair.Exists()
	return ok
}

func (self *ValueVersionedSet) GetOrCreateValue(key []byte) (*ValueVersioned, error) {

	if !self.HasKey(key) {

		curr, err := self.GetCurrentVersion()
		if err != nil {
			return nil, utils.StackError(err, "Unable to access current version")
		}
		if !curr.IsHead() {
			return nil, NewDSError(Error_Operation_Invalid, "Key does not exist and cannot be created when version is loaded")
		}

		//make sure the set exists in the db with null valueVersioned
		err = self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket(self.dbkey)
			for _, sk := range self.setkey {
				bucket = bucket.Bucket(sk)
			}
			bucket, err = bucket.CreateBucketIfNotExists(key)
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}

			//setup the basic structure
			if err := bucket.Put(itob(CURRENT), itob(HEAD)); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}

			//we set HEAD to nil: this means it was created, but is not valid. This is needed
			//in case someone called remove() on this key. Than HEAD = INVALID_DATA, which leads
			//to Exists() false
			err = bucket.Delete(itob(HEAD))
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
		if err != nil {
			return nil, err
		}
	}

	return &ValueVersioned{self.db, self.dbkey, self.setkey, key}, nil
}

func (self *ValueVersionedSet) GetEntry(key interface{}) (Entry, error) {

	var bkey []byte
	switch t := key.(type) {
	case []byte:
		bkey = t
	case string:
		bkey = []byte(t)
	default:
		return nil, NewDSError(Error_Operation_Invalid, "Provided key must be byte or string")
	}

	if !self.HasKey(bkey) {
		return nil, NewDSError(Error_Key_Not_Existant, "Key does not exist in set", "Key", key)
	}
	return self.GetOrCreateValue(bkey)
}

func (self *ValueVersionedSet) SupportsSubentries() bool {
	return true
}

func (self *ValueVersionedSet) Erase() error {
	return self.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey[:len(self.setkey)-1] {
			bucket = bucket.Bucket(bkey)
		}
		return wrapDSError(bucket.DeleteBucket(self.setkey[len(self.setkey)-1]), Error_Bolt_Access_Failure)
	})
}

func (self *ValueVersionedSet) GetVersionedEntry(key interface{}) (VersionedEntry, error) {

	var bkey []byte
	switch t := key.(type) {
	case []byte:
		bkey = t
	case string:
		bkey = []byte(t)
	default:
		return nil, NewDSError(Error_Operation_Invalid, "Provided key must be byte or string")
	}

	if !self.HasKey(bkey) {
		return nil, NewDSError(Error_Key_Not_Existant, "Key does not exist in set", "Key", key)
	}
	return self.GetOrCreateValue(bkey)
}

func (self *ValueVersionedSet) removeKey(key []byte) error {

	if !self.HasKey(key) {
		return NewDSError(Error_Key_Not_Existant, "key does not exists, cannot be removed")
	}
	pair, err := self.GetOrCreateValue(key)
	if err != nil {
		return utils.StackError(err, "Unable to get key from set")
	}
	err = pair.remove()
	if err != nil {
		return utils.StackError(err, "Unable to remove value in set")
	}
	return nil
}

func (self *ValueVersionedSet) getSetKey() []byte {
	return self.setkey[len(self.setkey)-1]
}

/*
 * ValueVersioned functions
 * ********************************************************************************
 */
type ValueVersioned struct {
	db     *boltWrapper
	dbkey  []byte
	setkey [][]byte
	key    []byte
}

func (self *ValueVersioned) Write(valueVersioned interface{}) error {

	//check if we are allowed to write: are we in HEAD?
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		val := bucket.Get(itob(CURRENT))
		if btoi(val) != HEAD {
			return NewDSError(Error_Operation_Invalid, "Can only write data when in HEAD")
		}
		return nil
	})
	if err != nil {
		return err
	}

	bts, err := getBytes(valueVersioned)
	if err != nil {
		return err
	}

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		err := bucket.Put(itob(HEAD), bts)
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
}

//True if:
// - setup correctly and writeable/readable
// - is available in currently load version
// - contains a value, hence can be safely read
func (self *ValueVersioned) IsValid() bool {

	var result bool = true
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		if bucket == nil {
			result = false
			return nil
		}
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
			if bucket == nil {
				result = false
				return nil
			}
		}

		cur := bucket.Get(itob(CURRENT))
		if cur == nil || btoi(cur) == INVALID {
			result = false
			return nil
		}
		cur = bucket.Get(cur)
		result = !((cur == nil) || isInvalid(cur))
		return nil
	})

	if err != nil {
		return false
	}
	return result
}

//return true if
// - the value was already written in HEAD, or
// - any versions exist with valid data
func (self *ValueVersioned) WasWrittenOnce() (bool, error) {

	var result bool = false
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//check if something valid is in HEAD
		head := bucket.Get(itob(HEAD))
		if head != nil && !isInvalid(head) {
			result = true
			return nil
		}

		//head was not written, check if we have any versions
		bucket.ForEach(func(k, v []byte) error {
			key := btoi(k)
			if key != HEAD && key != CURRENT {
				if v != nil && !isInvalid(v) {
					result = true
					return nil
				}
			}
			return nil
		})
		return nil
	})

	if err != nil {
		return false, utils.StackError(err, "Cannot check if value holds data or not")
	}
	return result, nil
}

//True if:
// - was setup with GetOrCreate
// - was not removed
//
// Note that it does state nothing about content, could still be invalid or never
// be written before
func (self *ValueVersioned) Exists() (bool, error) {

	var result bool = true
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		if bucket == nil {
			result = false
			return nil
		}
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
			if bucket == nil {
				result = false
				return nil
			}
		}

		cur := bucket.Get(itob(CURRENT))
		if cur == nil || btoi(cur) == INVALID {
			result = false
			return nil
		}

		cur = bucket.Get(cur)
		if cur == nil {
			result = true
			return nil
		}

		if isInvalid(cur) {
			result = false
			return nil
		}

		return nil
	})

	if err != nil {
		return false, err
	}
	return result, nil
}

func (self *ValueVersioned) Read() (interface{}, error) {

	current, err := self.GetCurrentVersion()
	if err != nil {
		return nil, err
	}
	result, err := self.readVersion(current)
	if err != nil {
		return nil, utils.StackError(err, "Unable to read value for current version")
	}
	return result, nil
}

func (self *ValueVersioned) SupportsSubentries() bool {
	return false
}

func (self *ValueVersioned) GetEntry(interface{}) (Entry, error) {
	return nil, NewDSError(Error_Operation_Invalid, "ValueVersioned does not have subentries")
}

func (self *ValueVersioned) GetVersionedEntry(interface{}) (VersionedEntry, error) {
	return nil, NewDSError(Error_Operation_Invalid, "ValueVersioned does not have subentries")
}

func (self *ValueVersioned) readVersion(ID VersionID) (interface{}, error) {

	var res interface{}
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(itob(uint64(ID)))
		if isInvalid(data) {
			return NewDSError(Error_Key_Not_Existant, "Key ValueVersioned pair does not exist in currently loaded version")
		}
		if data == nil {
			return NewDSError(Error_Invalid_Data, "ValueVersioned was not set before read")
		}

		var err error
		res, err = getInterface(data)
		return err
	})

	return res, err
}

func (self *ValueVersioned) remove() error {

	//check if we are allowed to remove: are we in HEAD?
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		val := bucket.Get(itob(CURRENT))
		if btoi(val) != HEAD {
			return NewDSError(Error_Operation_Invalid, "Can only remove data when in HEAD")
		}
		return nil
	})
	if err != nil {
		return err
	}

	//removing does not mean to delete everything. We need the data for loading older
	//versions. It just means we set it as "not existing".
	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		err := bucket.Put(itob(HEAD), INVALID_VALUE)
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
}

func (self *ValueVersioned) Erase() error {

	//Full erasure, including all versions etc. No checks done.
	return self.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}
		return wrapDSError(bucket.DeleteBucket(self.key), Error_Bolt_Access_Failure)
	})
}

func (self *ValueVersioned) GetCurrentVersion() (VersionID, error) {

	var version uint64
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		version = btoi(bucket.Get(itob(CURRENT)))
		return nil
	})

	return VersionID(version), nil
}

func (self *ValueVersioned) GetLatestVersion() (VersionID, error) {

	var version uint64 = 0
	found := false
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val != HEAD && val != CURRENT {
				found = true
				if val > version {
					version = val
				}
			}
			return nil
		})
		return nil
	})

	if !found {
		return VersionID(INVALID), nil
	}

	return VersionID(version), nil
}

func (self *ValueVersioned) HasUpdates() (bool, error) {

	updates := false
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//if there is no version available yet we definitly have updates
		if bucket.Sequence() == 0 {
			updates = true
			return nil
		}

		cur := bucket.Get(itob(HEAD))
		old := bucket.Get(itob(bucket.Sequence()))

		updates = !bytes.Equal(cur, old)

		return nil
	})
	if err != nil {
		panic(err.Error())
	}

	return updates, nil
}

func (self *ValueVersioned) HasVersions() (bool, error) {

	var versions bool
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//we check if the current sequence exists, which would mean a verion was written
		versions = bucket.Get(itob(bucket.Sequence())) != nil
		return nil
	})

	return versions, err
}

func (self *ValueVersioned) ResetHead() error {

	latest, err := self.GetLatestVersion()
	if err != nil {
		return err
	}

	//if no version available we cannot reset
	if !latest.IsValid() {
		return NewDSError(Error_Operation_Invalid, "No version available to reset to")
	}

	//if we have a real version we need the data to return to!
	data, err := self.readVersion(latest)

	//if the version is invalid we don't do anything
	if err != nil {
		return utils.StackError(err, "Unable to access version in versioned ds value")
	}

	//normal write checks for invalid, but we want to override invalid too
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		input, _ := getBytes(data)
		err := bucket.Put(itob(HEAD), input)
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
	return err
}

func (self *ValueVersioned) FixStateAsVersion() (VersionID, error) {

	id := VersionID(INVALID)
	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//create a new version inside the subbucket if the valueVersioned changed
		vid := bucket.Sequence()
		olddata := bucket.Get(itob(vid))
		data := bucket.Get(itob(HEAD))

		if (olddata == nil) || !bytes.Equal(data, olddata) {
			var err error
			vid, err = bucket.NextSequence()
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
			if err := bucket.Put(itob(vid), data); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}

		id = VersionID(vid)
		return nil
	})

	return id, err
}

func (self *ValueVersioned) LoadVersion(id VersionID) error {

	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//check if the version to load exists
		if bucket.Get(itob(uint64(id))) == nil {
			return NewDSError(Error_Operation_Invalid, "Version does not exists")
		}

		//set CURRENT to the new version
		return bucket.Put(itob(CURRENT), itob(uint64(id)))
	})

	return err
}

func (self *ValueVersioned) RemoveVersionsUpTo(version VersionID) error {

	err := self.db.Update(func(tx *bolt.Tx) error {

		setBucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			setBucket = setBucket.Bucket(bkey)
		}
		bucket := setBucket.Bucket(self.key)

		//check if the version exists
		if bucket.Get(itob(uint64(version))) == nil {
			return NewDSError(Error_Operation_Invalid, "Version does not exists")
		}

		latest := VersionID(bucket.Sequence())
		versionData := bucket.Get(itob(uint64(version)))
		if isInvalid(versionData) && (latest == version) {
			//if the latest version for this bucket points to INVALID_DATA we know it
			//was removed, hence can be fully deleted. (as it could be that is was written
			//again after setting it invalid and make it hence valid again in later versions)
			if err := setBucket.DeleteBucket(self.key); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		} else {
			//delete each entry that belongs to older versions. We use the fact
			//that versions have always increasing numbers
			todelete := make([][]byte, 0)
			bucket.ForEach(func(k, v []byte) error {
				val := btoi(k)
				if val < uint64(version) {
					//deep copy of key, as the slice is invalid outside foreach
					keycopy := make([]byte, len(k))
					copy(keycopy, k)
					todelete = append(todelete, keycopy)
				}
				return nil
			})
			for _, k := range todelete {
				if err := bucket.Delete(k); err != nil {
					return wrapDSError(err, Error_Bolt_Access_Failure)
				}
			}
		}
		return nil
	})
	return err
}

func (self *ValueVersioned) RemoveVersionsUpFrom(version VersionID) error {

	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//check if version exists
		if bucket.Get(itob(uint64(version))) == nil {
			return NewDSError(Error_Operation_Invalid, "Version does not exist", "Version", version)
		}

		todelete := make([][]byte, 0)
		err := bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val != CURRENT && val != HEAD && val > uint64(version) {
				//deep copy of key, as the slice is invalid outside foreach
				keycopy := make([]byte, len(k))
				copy(keycopy, k)
				todelete = append(todelete, keycopy)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, k := range todelete {
			if err := bucket.Delete(k); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}
		//make sure the sequence is always set to the highest version
		if err := bucket.SetSequence(uint64(version)); err != nil {
			return wrapDSError(err, Error_Bolt_Access_Failure)
		}
		return nil
	})
	return err
}

//helper functions
func itob(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

func btoi(b []byte) uint64 {
	value, _ := binary.Uvarint(b)
	return value
}

func btos(val []byte) string {
	return base58.Encode(val)
}

func stob(val string) []byte {
	data, err := base58.Decode(val)
	if err != nil {
		panic("Stored version information could not be decoded to VersionID")
	}
	return data
}

func itos(val uint64) string {
	return btos(itob(val))
}

func stoi(val string) uint64 {
	return btoi(stob(val))
}
