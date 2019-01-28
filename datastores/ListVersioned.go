// ListVersioned database: The listVersioned database saves multiple listVersioneds per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
)

/*
ListVersioned database uses a valueVersioned database underneath, just one level deeper in the
hirarchy.  Each ValueVersionedSet is a single listVersioned.

Data layout of versioned listVersioned store:

bucket(SetKey) [
    entry(CURRENT) = HEAD
	ValueVersionedSet(VERSIONS) [
		entry(1) = VersionlistVersioned(1->1, 2->1)
		entry(2) = VersionlistVersioned(1->2, 2->1)
	]
	ValueVersionedSet(1)
	ValueVersionedSet(2)
]
*/

func NewListVersionedDatabase(db *bolt.DB) (*ListVersionedDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("ListVersioned"))
		return nil
	})

	return &ListVersionedDatabase{db, []byte("ListVersioned")}, nil
}

//ilistlements the database interface
type ListVersionedDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self ListVersionedDatabase) HasSet(set [32]byte) bool {

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

func (self ListVersionedDatabase) GetOrCreateSet(set [32]byte) Set {

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

	return &ListVersionedSet{self.db, self.dbkey, set[:]}
}

func (self ListVersionedDatabase) RemoveSet(set [32]byte) error {

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

func (self ListVersionedDatabase) Close() {

}

//The store itself is very silistle, as all the access logic will be in the set type
//this is only to manage the existing entries
type ListVersionedSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey []byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *ListVersionedSet) IsValid() bool {

	return true
}

func (self *ListVersionedSet) Print(params ...int) {

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
					data := inter.(map[string]interface{})
					//build the versioning string
					str := "["
					for mk, mv := range data {
						str = str + fmt.Sprintf("%v", string(stob(mk))) + ": %v,  "
						mvid := stoi(mv.(string))
						if mvid == INVALID {
							str = fmt.Sprintf(str, "INVALID")
						} else {
							str = fmt.Sprintf(str, mvid)
						}
					}

					fmt.Printf("%s\t%v: %v]\n", indent, btoi(sk), str)
					return nil
				})

			} else {
				fmt.Println(string(k))
				kvset := ValueVersionedSet{self.db, self.dbkey, [][]byte{self.setkey, k}}
				if len(params) > 0 {
					kvset.Print(1 + params[0])
				} else {
					kvset.Print(1)
				}
			}
			return nil
		})
		return nil
	})

}

func (self *ListVersionedSet) collectLists() []ListVersioned {

	listVersioneds := make([]ListVersioned, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				//key must be copied, as it gets invalid outside of ForEach
				var key = make([]byte, len(k))
				copy(key, k)
				list := newListVersioned(self.db, self.dbkey, [][]byte{self.setkey, key})
				listVersioneds = append(listVersioneds, list)
			}
			return nil
		})
		return nil
	})
	return listVersioneds
}

func (self *ListVersionedSet) HasUpdates() bool {

	//we cycle through all listVersioneds and check if they have updates
	listVersioneds := self.collectLists()
	for _, list := range listVersioneds {
		if list.HasUpdates() {
			return true
		}
	}
	return false
}

func (self *ListVersionedSet) HasVersions() bool {

	var versions bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(bkey)
		}

		versions = (bucket.Sequence() != 0)
		return nil
	})
	return versions
}

func (self *ListVersionedSet) ResetHead() {

	listVersioneds := self.collectLists()
	for _, list := range listVersioneds {
		//if the list has a version we can reset, otherwise we need a full delete
		//(to reset to not-available-state
		if list.LatestVersion().IsValid() {
			list.kvset.ResetHead()
		} else {
			//make sure the set exists in the db with null valueVersioned
			self.db.Update(func(tx *bolt.Tx) error {

				bucket := tx.Bucket(self.dbkey)
				bucket = bucket.Bucket(self.setkey)
				return bucket.DeleteBucket(list.getListKey())
			})
		}

	}
}

func (self *ListVersionedSet) FixStateAsVersion() (VersionID, error) {

	//check if opertion is possible
	cv, err := self.GetCurrentVersion()
	if err != nil {
		return VersionID(INVALID), err
	}
	if !cv.IsHead() {
		return VersionID(INVALID), fmt.Errorf("Unable to create version if HEAD is not checked out")
	}

	//collect all versions we need for the current version
	version := make(map[string]string, 0)
	lists := self.collectLists()
	for _, list := range lists {
		if list.HasUpdates() {
			v, err := list.kvset.FixStateAsVersion()
			if err != nil {
				return VersionID(INVALID), err
			}
			version[btos(list.getListKey())] = itos(uint64(v))

		} else {
			v, err := list.kvset.GetLatestVersion()
			if err != nil {
				return VersionID(INVALID), err
			}
			version[btos(list.getListKey())] = itos(uint64(v))
		}
	}

	//write the new version into store
	var currentVersion uint64
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(bkey)
		}
		data, err := getBytes(version)
		if err != nil {
			return err
		}
		currentVersion, err = bucket.NextSequence()
		if err != nil {
			return nil
		}
		bucket.Put(itob(currentVersion), data)
		return nil
	})

	return VersionID(currentVersion), nil
}

func (self *ListVersionedSet) getVersionInfo(id VersionID) (map[string]interface{}, error) {

	version := make(map[string]interface{})
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, sk := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(sk)
		}
		data := bucket.Get(itob(uint64(id)))
		if data == nil || len(data) == 0 {
			return fmt.Errorf("Version does not exist")
		}
		res, err := getInterface(data)
		if err != nil {
			return err
		}
		reslistVersioned, ok := res.(map[string]interface{})
		if !ok {
			return fmt.Errorf("Problem with parsing the saved data")
		}
		version = reslistVersioned
		return nil
	})
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (self *ListVersionedSet) LoadVersion(id VersionID) error {

	if cv, _ := self.GetCurrentVersion(); cv == id {
		return nil
	}

	//grab the needed verion
	var version map[string]interface{}
	if !id.IsHead() {
		var err error
		version, err = self.getVersionInfo(id)
		if err != nil {
			return err
		}
	}

	//load the versions of the individual listVersioneds
	lists := self.collectLists()
	for _, list := range lists {

		if id.IsHead() {
			list.kvset.LoadVersion(id)

		} else {
			v, ok := version[btos(list.getListKey())]
			if !ok {
				return fmt.Errorf("Unable to load version information for %v", string(list.getListKey()))
			}
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("Stored version information has wrong format")
			}
			list.kvset.LoadVersion(VersionID(stoi(s)))
		}
	}

	//write the current version
	err := self.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		return bucket.Put(itob(CURRENT), itob(uint64(id)))
	})

	return err
}

func (self *ListVersionedSet) GetLatestVersion() (VersionID, error) {

	var version uint64 = 0
	found := false
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			found = true
			if btoi(k) > version {
				version = btoi(k)
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

func (self *ListVersionedSet) GetCurrentVersion() (VersionID, error) {

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

func (self *ListVersionedSet) RemoveVersionsUpTo(ID VersionID) error {

	listVersioneds := self.collectLists()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, list := range listVersioneds {
		//remove up to version
		val := version[btos(list.getListKey())]
		ival := stoi(val.(string))
		err := list.kvset.RemoveVersionsUpTo(VersionID(ival))
		if err != nil {
			return err
		}
	}

	//remove the versions from the relevant bucket
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range [][]byte{self.setkey, itob(VERSIONS)} {
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
			bucket.Delete(k)
		}

		return nil
	})

	return err
}

func (self *ListVersionedSet) RemoveVersionsUpFrom(ID VersionID) error {

	listVersioneds := self.collectLists()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, list := range listVersioneds {
		//remove up to version
		val := version[btos(list.getListKey())]
		ival := stoi(val.(string))
		err := list.kvset.RemoveVersionsUpFrom(VersionID(ival))
		if err != nil {
			return err
		}
	}

	//remove the versions from the relevant bucket
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range [][]byte{self.setkey, itob(VERSIONS)} {
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
			bucket.Delete(k)
		}

		return nil
	})

	return err
}

/*
 * ListVersioned functions
 * ********************************************************************************
 */
func (self ListVersionedSet) GetType() StorageType {
	return ListType
}

func (self *ListVersionedSet) HasList(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		result = bucket.Bucket(key) != nil
		return nil
	})

	return result
}

func (self *ListVersionedSet) GetOrCreateList(key []byte) (*ListVersioned, error) {

	if !self.HasList(key) {

		curr, err := self.GetCurrentVersion()
		if err != nil {
			return nil, err
		}
		if !curr.IsHead() {
			return nil, fmt.Errorf("Key does not exist and cannot be created when version is loaded")
		}

		//make sure the set exists in the db with null valueVersioned
		err = self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket(self.dbkey)
			bucket = bucket.Bucket(self.setkey)
			newbucket, err := bucket.CreateBucketIfNotExists(key)
			if err != nil {
				return err
			}
			newbucket.Put(itob(CURRENT), itob(HEAD))
			newbucket.CreateBucketIfNotExists(itob(VERSIONS))

			return err
		})

		if err != nil {
			return nil, err
		}
	}

	list := newListVersioned(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &list, nil
}

/*
 * ListVersioned functions
 * ********************************************************************************
 */

type ListVersioned struct {
	kvset ValueVersionedSet
}

func newListVersioned(db *bolt.DB, dbkey []byte, listVersionedkeys [][]byte) ListVersioned {

	kv := ValueVersionedSet{db, dbkey, listVersionedkeys}
	return ListVersioned{kv}
}

func (self *ListVersioned) Add(value interface{}) (ListEntry, error) {

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
	kv, err := self.kvset.GetOrCreateValue(itob(id))
	if err != nil {
		return nil, err
	}
	return &listVersionedEntry{*kv}, kv.Write(value)
}

func (self *ListVersioned) GetEntries() ([]ListEntry, error) {

	entries := make([]ListEntry, 0)

	//iterate over all entries...
	err := self.kvset.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.kvset.dbkey)
		for _, bkey := range self.kvset.setkey {
			bucket = bucket.Bucket(bkey)
		}

		//collect the entries
		err := bucket.ForEach(func(k []byte, v []byte) error {

			//don't use VERSIONS and CURRENT
			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				//copy the key as it is not valid outside for each
				var key = make([]byte, len(k))
				copy(key, k)

				//build the value and add to the list
				value := ValueVersioned{self.kvset.db, self.kvset.dbkey, self.kvset.setkey, key}
				entries = append(entries, &listVersionedEntry{value})
			}
			return nil
		})
		return err
	})

	return entries, err
}

func (self *ListVersioned) CurrentVersion() VersionID {

	v, _ := self.kvset.GetCurrentVersion()
	return v
}

func (self *ListVersioned) LatestVersion() VersionID {

	v, _ := self.kvset.GetLatestVersion()
	return v
}

func (self *ListVersioned) HasUpdates() bool {

	return self.kvset.HasUpdates()
}

func (self *ListVersioned) HasVersions() bool {

	return self.kvset.HasVersions()
}

func (self *ListVersioned) getListKey() []byte {
	return self.kvset.getSetKey()
}

/*
 * List entries functions
 * ********************************************************************************
 */
type listVersionedEntry struct {
	value ValueVersioned
}

func (self *listVersionedEntry) Write(value interface{}) error {
	return self.value.Write(value)
}

func (self *listVersionedEntry) Read() (interface{}, error) {
	return self.value.Read()
}

func (self *listVersionedEntry) ReadType(value interface{}) error {
	return self.value.ReadType(value)
}

func (self *listVersionedEntry) IsValid() bool {
	return self.value.IsValid()
}

func (self *listVersionedEntry) Remove() error {
	return self.value.remove()
}
