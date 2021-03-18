// ListVersioned database: The listVersioned database saves multiple listVersioneds per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP /utils"

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

func NewListVersionedDatabase(db *boltWrapper) (*ListVersionedDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("ListVersioned"))
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
	if err != nil {
		return nil, err
	}

	return &ListVersionedDatabase{db, []byte("ListVersioned")}, nil
}

//ilistlements the database interface
type ListVersionedDatabase struct {
	db    *boltWrapper
	dbkey []byte
}

func (self ListVersionedDatabase) HasSet(set [32]byte) (bool, error) {

	var result bool = false
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result, err
}

func (self ListVersionedDatabase) GetOrCreateSet(set [32]byte) (Set, error) {

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

			//setup the basic structure
			err = bucket.Put(itob(CURRENT), itob(HEAD))
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
			_, err = bucket.CreateBucketIfNotExists(itob(VERSIONS))
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return &ListVersionedSet{self.db, self.dbkey, set[:]}, nil
}

func (self ListVersionedDatabase) RemoveSet(set [32]byte) error {

	if has, _ := self.HasSet(set); has {

		return self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			err := bucket.DeleteBucket(set[:])
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
	}

	return nil
}

func (self ListVersionedDatabase) Close() {

}

//The store itself is very silistle, as all the access logic will be in the set type
//this is only to manage the existing entries
type ListVersionedSet struct {
	db     *boltWrapper
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
					data := inter.(map[string]string)
					//build the versioning string
					str := "["
					for mk, mv := range data {
						str = str + fmt.Sprintf("%v", string(stob(mk))) + ": %v,  "
						mvid := stoi(mv)
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

func (self *ListVersionedSet) HasUpdates() (bool, error) {

	//we cycle through all listVersioneds and check if they have updates
	listVersioneds := self.collectLists()
	for _, list := range listVersioneds {
		has, err := list.HasUpdates()
		if err != nil {
			return false, utils.StackError(err, "Unable to check for Updates")
		}
		if has {
			return true, nil
		}
	}
	return false, nil
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

func (self *ListVersionedSet) ResetHead() error {

	listVersioneds := self.collectLists()
	for _, list := range listVersioneds {
		//if the list has a version we can reset, otherwise we need a full delete
		//(to reset to not-available-state
		if list.LatestVersion().IsValid() {
			if err := list.kvset.ResetHead(); err != nil {
				return utils.StackError(err, "Unable to reset head in ds value set")
			}
		} else {
			//make sure the set exists in the db with null valueVersioned
			err := self.db.Update(func(tx *bolt.Tx) error {

				bucket := tx.Bucket(self.dbkey)
				bucket = bucket.Bucket(self.setkey)

				return wrapDSError(bucket.DeleteBucket(list.getListKey()), Error_Bolt_Access_Failure)
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *ListVersionedSet) FixStateAsVersion() (VersionID, error) {

	//check if opertion is possible
	cv, err := self.GetCurrentVersion()
	if err != nil {
		return VersionID(INVALID), utils.StackError(err, "Unable to access curent version")
	}
	if !cv.IsHead() {
		return VersionID(INVALID), NewDSError(Error_Operation_Invalid, "Unable to create version if HEAD is not checked out")
	}

	//collect all versions we need for the current version
	version := make(map[string]string, 0)
	lists := self.collectLists()
	for _, list := range lists {
		if has, _ := list.HasUpdates(); has {
			v, err := list.kvset.FixStateAsVersion()
			if err != nil {
				return VersionID(INVALID), utils.StackError(err, "Unable to fix state in ds value")
			}
			version[btos(list.getListKey())] = itos(uint64(v))

		} else {
			v, err := list.kvset.GetLatestVersion()
			if err != nil {
				return VersionID(INVALID), utils.StackError(err, "Unable to access latest version in ds value")
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
			return wrapDSError(err, Error_Bolt_Access_Failure)
		}
		return wrapDSError(bucket.Put(itob(currentVersion), data), Error_Bolt_Access_Failure)
	})
	if err != nil {
		return VersionID(INVALID), err
	}

	return VersionID(currentVersion), nil
}

func (self *ListVersionedSet) getVersionInfo(id VersionID) (map[string]string, error) {

	version := make(map[string]string)
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, sk := range [][]byte{self.setkey, itob(VERSIONS)} {
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
		reslistVersioned, ok := res.(*map[string]string)
		if !ok {
			return NewDSError(Error_Invalid_Data, "Problem with parsing the saved data")
		}
		version = *reslistVersioned
		return nil
	})
	return version, err
}

func (self *ListVersionedSet) LoadVersion(id VersionID) error {

	if cv, _ := self.GetCurrentVersion(); cv == id {
		return nil
	}

	//grab the needed verion
	var version map[string]string
	if !id.IsHead() {
		var err error
		version, err = self.getVersionInfo(id)
		if err != nil {
			return utils.StackError(err, "Unable to load version information")
		}
	}

	//load the versions of the individual listVersioneds
	lists := self.collectLists()
	for _, list := range lists {

		if id.IsHead() {
			err := list.kvset.LoadVersion(id)
			if err != nil {
				return utils.StackError(err, "Unable to load version from ds value")
			}

		} else {
			v, ok := version[btos(list.getListKey())]
			if !ok {
				return NewDSError(Error_Invalid_Data, fmt.Sprintf("Unable to load version information for %v", string(list.getListKey())))
			}
			if err := list.kvset.LoadVersion(VersionID(stoi(v))); err != nil {
				return utils.StackError(err, "Unable to load version for ds value")
			}
		}
	}

	//write the current version
	return self.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		err := bucket.Put(itob(CURRENT), itob(uint64(id)))
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
}

func (self *ListVersionedSet) GetLatestVersion() (VersionID, error) {

	var found bool = false
	var version uint64 = 0
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			if btoi(k) > version {
				found = true
				version = btoi(k)
			}
			return nil
		})
		return nil
	})
	if !found {
		return VersionID(INVALID), err
	}

	return VersionID(version), err
}

func (self *ListVersionedSet) GetCurrentVersion() (VersionID, error) {

	var version uint64 = INVALID
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		val := bucket.Get(itob(CURRENT))
		if val == nil {
			return NewDSError(Error_Setup_Incorrectly, "No current version set")
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
		return utils.StackError(err, "Unable to access version info for %v", ID)
	}

	for _, list := range listVersioneds {
		//remove up to version
		val := version[btos(list.getListKey())]
		ival := stoi(val)
		err := list.kvset.RemoveVersionsUpTo(VersionID(ival))
		if err != nil {
			return utils.StackError(err, "Unable to remove versions in ds value set upt to %v", ival)
		}
	}

	//remove the versions from the relevant bucket
	return self.db.Update(func(tx *bolt.Tx) error {

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
			if err := bucket.Delete(k); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}

		return nil
	})
}

func (self *ListVersionedSet) RemoveVersionsUpFrom(ID VersionID) error {

	listVersioneds := self.collectLists()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return utils.StackError(err, "Unable to get version info for %v", ID)
	}

	for _, list := range listVersioneds {
		//remove up to version
		val := version[btos(list.getListKey())]
		ival := stoi(val)
		err := list.kvset.RemoveVersionsUpFrom(VersionID(ival))
		if err != nil {
			return utils.StackError(err, "Unable to remove version %v from ds value set", ival)
		}
	}

	//remove the versions from the relevant bucket
	return self.db.Update(func(tx *bolt.Tx) error {

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
			if err := bucket.Delete(k); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
		}

		return nil
	})
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
			return nil, utils.StackError(err, "Unable to access current version")
		}
		if !curr.IsHead() {
			return nil, NewDSError(Error_Operation_Invalid, "Key does not exist and cannot be created when version is loaded")
		}

		//make sure the set exists in the db with null valueVersioned
		err = self.db.Update(func(tx *bolt.Tx) error {

			//get correct bucket
			bucket := tx.Bucket(self.dbkey)
			bucket = bucket.Bucket(self.setkey)
			newbucket, err := bucket.CreateBucketIfNotExists(key)
			if err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
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

func newListVersioned(db *boltWrapper, dbkey []byte, listVersionedkeys [][]byte) ListVersioned {

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
			return wrapDSError(err, Error_Bolt_Access_Failure)
		}
		id = val
		return nil
	})
	kv, err := self.kvset.GetOrCreateValue(itob(id))
	if err != nil {
		return nil, utils.StackError(err, "Unable to access or create value in ds value set")
	}
	return &listVersionedEntry{*kv}, utils.StackOnError(kv.Write(value), "Unable to write ds value")
}

func (self *ListVersioned) GetEntries() ([]ListEntry, error) {

	vals, err := self.kvset.getValues()
	if err != nil {
		return []ListEntry{}, utils.StackError(err, "Unable to get values from ds value set")
	}

	entries := make([]ListEntry, len(vals))
	for i, val := range vals {
		entries[i] = &listVersionedEntry{val}
	}

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

func (self *ListVersioned) HasUpdates() (bool, error) {

	res, err := self.kvset.HasUpdates()
	return res, utils.StackOnError(err, "Unable to query value set for updates")
}

func (self *ListVersioned) HasVersions() (bool, error) {

	res, err := self.kvset.HasVersions()
	return res, utils.StackOnError(err, "Unable to query value set for versions")
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
	return utils.StackOnError(self.value.Write(value), "Unable to write ds value")
}

func (self *listVersionedEntry) Read() (interface{}, error) {
	res, err := self.value.Read()
	return res, utils.StackOnError(err, "Unable to read ds value")
}

func (self *listVersionedEntry) IsValid() bool {
	return self.value.IsValid()
}

func (self *listVersionedEntry) Remove() error {
	return utils.StackOnError(self.value.remove(), "Unable to remove ds value")
}

func (self *listVersionedEntry) Id() uint64 {
	return btoi(self.value.key)
}
