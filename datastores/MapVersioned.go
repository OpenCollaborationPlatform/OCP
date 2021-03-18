// MapVersioned database: The mapVersioned database saves multiple mapVersioneds per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP /utils"

	"github.com/boltdb/bolt"
)

/*
MapVersioned database uses a keyvalueVersioned database underneath, just one level deeper in the
hirarchy.  Each ValueVersionedSet is a single mapVersioned.

Data layout of versioned mapVersioned store:

bucket(SetKey) [
    entry(CURRENT) = HEAD
	ValueVersionedSet(VERSIONS) [
		entry(1) = VersionmapVersioned(key1->1, key2->1)
		entry(2) = VersionmapVersioned(key1->2, key2->1)
	]
	ValueVersionedSet(key1)
	ValueVersionedSet(key2)
]
*/

func NewMapVersionedDatabase(db *boltWrapper) (*MapVersionedDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("MapVersioned"))
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})
	if err != nil {
		return nil, err
	}

	return &MapVersionedDatabase{db, []byte("MapVersioned")}, nil
}

//implements the database interface
type MapVersionedDatabase struct {
	db    *boltWrapper
	dbkey []byte
}

func (self MapVersionedDatabase) HasSet(set [32]byte) (bool, error) {

	var result bool = false
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		result = bucket.Bucket(set[:]) != nil
		return nil
	})

	return result, err
}

func (self MapVersionedDatabase) GetOrCreateSet(set [32]byte) (Set, error) {

	if !self.db.CanAccess() {
		return nil, NewDSError(Error_Transaction_Invalid, "No transaction open")
	}

	if has, _ := self.HasSet(set); !has {
		//make sure the bucket exists
		err := self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			bucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			//setup the basic structure
			if err := bucket.Put(itob(CURRENT), itob(HEAD)); err != nil {
				return wrapDSError(err, Error_Bolt_Access_Failure)
			}
			_, err = bucket.CreateBucketIfNotExists(itob(VERSIONS))
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
		if err != nil {
			return nil, err
		}
	}

	return &MapVersionedSet{self.db, self.dbkey, set[:]}, nil
}

func (self MapVersionedDatabase) RemoveSet(set [32]byte) error {

	if has, _ := self.HasSet(set); has {

		return self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			err := bucket.DeleteBucket(set[:])
			return wrapDSError(err, Error_Bolt_Access_Failure)
		})
	}
	return nil
}

func (self MapVersionedDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type MapVersionedSet struct {
	db     *boltWrapper
	dbkey  []byte
	setkey []byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *MapVersionedSet) IsValid() bool {

	return true
}

func (self *MapVersionedSet) Print(params ...int) {

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
						str = str + string(stob(mk)) + ": %v,  "
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

func (self *MapVersionedSet) collectMapVersioneds() []MapVersioned {

	mapVersioneds := make([]MapVersioned, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				//key must be copied, as it gets invalid outside of ForEach
				var key = make([]byte, len(k))
				copy(key, k)
				mp := newMapVersioned(self.db, self.dbkey, [][]byte{self.setkey, key})
				mapVersioneds = append(mapVersioneds, mp)
			}
			return nil
		})
		return nil
	})
	return mapVersioneds
}

func (self *MapVersionedSet) HasUpdates() (bool, error) {

	//we cycle through all mapVersioneds and check if they have updates
	mapVersioneds := self.collectMapVersioneds()
	for _, mp := range mapVersioneds {
		has, err := mp.HasUpdates()
		if err != nil {
			return false, utils.StackError(err, "Unable to check map for updates")
		}
		if has {
			return true, nil
		}
	}
	return false, nil
}

func (self *MapVersionedSet) HasVersions() (bool, error) {

	var versions bool = false
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(bkey)
		}

		versions = (bucket.Sequence() != 0)
		return nil
	})
	return versions, err
}

func (self *MapVersionedSet) ResetHead() error {

	mapVersioneds := self.collectMapVersioneds()
	for _, mp := range mapVersioneds {
		err := mp.kvset.ResetHead()
		if err != nil {
			return utils.StackError(err, "Unableto reset head of Map")
		}
	}
	return nil
}

func (self *MapVersionedSet) FixStateAsVersion() (VersionID, error) {

	//check if opertion is possible
	cv, err := self.GetCurrentVersion()
	if err != nil {
		return VersionID(INVALID), utils.StackError(err, "Unable to access current version")
	}
	if !cv.IsHead() {
		return VersionID(INVALID), NewDSError(Error_Operation_Invalid, "Unable to create version if HEAD is not checked out")
	}

	//collect all versions we need for the current version
	version := make(map[string]string, 0)
	mapVersioneds := self.collectMapVersioneds()
	for _, mp := range mapVersioneds {
		if has, _ := mp.HasUpdates(); has {
			v, err := mp.kvset.FixStateAsVersion()
			if err != nil {
				return VersionID(INVALID), utils.StackError(err, "Unable to fix state in ds value set")
			}
			version[btos(mp.getMapVersionedKey())] = itos(uint64(v))

		} else {
			v, err := mp.kvset.GetLatestVersion()
			if err != nil {
				return VersionID(INVALID), utils.StackError(err, "Unable to get latest version from ds value set")
			}
			version[btos(mp.getMapVersionedKey())] = itos(uint64(v))
		}
	}

	//write the new version into store
	var currentVersion uint64 = INVALID
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
		err = bucket.Put(itob(currentVersion), data)
		return wrapDSError(err, Error_Bolt_Access_Failure)
	})

	return VersionID(currentVersion), err
}

func (self *MapVersionedSet) getVersionInfo(id VersionID) (map[string]string, error) {

	version := make(map[string]string)
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, sk := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(sk)
		}
		data := bucket.Get(itob(uint64(id)))
		if data == nil || len(data) == 0 {
			return NewDSError(Error_Key_Not_Existant, "Version does not exist", id)
		}
		res, err := getInterface(data)
		if err != nil {
			return err
		}
		resmapVersioned, ok := res.(*map[string]string)
		if !ok {
			return NewDSError(Error_Invalid_Data, "Problem with parsing the saved data")
		}
		version = *resmapVersioned
		return nil
	})
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (self *MapVersionedSet) LoadVersion(id VersionID) error {

	if cv, _ := self.GetCurrentVersion(); cv == id {
		return nil
	}

	//grab the needed verion
	var version map[string]string
	if !id.IsHead() {
		var err error
		version, err = self.getVersionInfo(id)
		if err != nil {
			return utils.StackError(err, "Unable to get information for version %v", id)
		}
	}

	//load the versions of the individual mapVersioneds
	mapVersioneds := self.collectMapVersioneds()
	for _, mp := range mapVersioneds {

		if id.IsHead() {
			if err := mp.kvset.LoadVersion(id); err != nil {
				return utils.StackError(err, "unable to load version %v in ds value set", id)
			}

		} else {
			v, ok := version[btos(mp.getMapVersionedKey())]
			if !ok {
				return NewDSError(Error_Invalid_Data, "Unable to load version information for %v", string(mp.getMapVersionedKey()))
			}
			if err := mp.kvset.LoadVersion(VersionID(stoi(v))); err != nil {
				return utils.StackError(err, "Unable to load version %v in ds value set", v)
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

func (self *MapVersionedSet) GetLatestVersion() (VersionID, error) {

	var found bool = false
	var version uint64 = 0
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range [][]byte{self.setkey, itob(VERSIONS)} {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		return bucket.ForEach(func(k, v []byte) error {
			if btoi(k) > version {
				found = true
				version = btoi(k)
			}
			return nil
		})
	})
	if !found {
		return VersionID(INVALID), err
	}

	return VersionID(version), err
}

func (self *MapVersionedSet) GetCurrentVersion() (VersionID, error) {

	var version uint64 = INVALID
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		val := bucket.Get(itob(CURRENT))
		if val == nil {
			return NewDSError(Error_Operation_Invalid, "No current version set")
		}
		version = btoi(val)
		return nil
	})

	return VersionID(version), err
}

func (self *MapVersionedSet) RemoveVersionsUpTo(ID VersionID) error {

	mapVersioneds := self.collectMapVersioneds()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return utils.StackError(err, "Unable to get version info for %v", ID)
	}

	for _, mp := range mapVersioneds {
		//remove up to version
		val := version[btos(mp.getMapVersionedKey())]
		ival := stoi(val)
		err := mp.kvset.RemoveVersionsUpTo(VersionID(ival))
		if err != nil {
			return utils.StackError(err, "Unable to remove version up to %v in ds value set", ival)
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

func (self *MapVersionedSet) RemoveVersionsUpFrom(ID VersionID) error {

	mapVersioneds := self.collectMapVersioneds()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return utils.StackError(err, "Unable to get version info for %v", ID)
	}

	for _, mp := range mapVersioneds {
		//remove up to version
		val := version[btos(mp.getMapVersionedKey())]
		ival := stoi(val)
		err := mp.kvset.RemoveVersionsUpFrom(VersionID(ival))
		if err != nil {
			return utils.StackError(err, "Unable to remove version up to %v in ds value set", ival)
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
 * MapVersioned functions
 * ********************************************************************************
 */
func (self MapVersionedSet) GetType() StorageType {
	return MapType
}

func (self *MapVersionedSet) HasMap(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		result = bucket.Bucket(key) != nil
		return nil
	})

	return result
}

func (self *MapVersionedSet) GetOrCreateMap(key []byte) (*MapVersioned, error) {

	if !self.HasMap(key) {

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

	mp := newMapVersioned(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &mp, nil
}

/*
 * MapVersioned functions
 * ********************************************************************************
 */

type MapVersioned struct {
	kvset ValueVersionedSet
}

func newMapVersioned(db *boltWrapper, dbkey []byte, mapVersionedkeys [][]byte) MapVersioned {

	kv := ValueVersionedSet{db, dbkey, mapVersionedkeys}
	return MapVersioned{kv}
}

func (self *MapVersioned) Write(key interface{}, valueVersioned interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	pair, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return utils.StackError(err, "Unable to access or create value in ds value set")
	}
	return utils.StackOnError(pair.Write(valueVersioned), "Unable to write ds value")
}

func (self *MapVersioned) IsValid() bool {

	return self.kvset.IsValid()
}

func (self *MapVersioned) HasKey(key interface{}) bool {

	if !self.kvset.IsValid() {
		panic("cannot access")
	}

	k, err := getBytes(key)
	if err != nil {
		panic("Unable to encode key")
	}
	return self.kvset.HasKey(k)

}

func (self *MapVersioned) Read(key interface{}) (interface{}, error) {

	if !self.HasKey(key) {
		return nil, NewDSError(Error_Key_Not_Existant, "Key does not exist, cannot read value")
	}

	k, err := getBytes(key)
	if err != nil {
		return nil, err
	}
	pair, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access or create value in ds value set")
	}
	res, err := pair.Read()
	return res, utils.StackOnError(err, "Unable to write ds value")
}

func (self *MapVersioned) Remove(key interface{}) error {

	if !self.HasKey(key) {
		return NewDSError(Error_Key_Not_Existant, "Key does not exist, cannot remove")
	}

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	return utils.StackOnError(self.kvset.removeKey(k), "Unable to remove entry in ds value set")
}

func (self *MapVersioned) CurrentVersion() VersionID {

	v, _ := self.kvset.GetCurrentVersion()
	return v
}

func (self *MapVersioned) LatestVersion() VersionID {

	v, _ := self.kvset.GetLatestVersion()
	return v
}

func (self *MapVersioned) HasUpdates() (bool, error) {

	res, err := self.kvset.HasUpdates()
	return res, utils.StackOnError(err, "Unable to query updates in ds value set")
}

func (self *MapVersioned) HasVersions() (bool, error) {

	res, err := self.kvset.HasVersions()
	return res, utils.StackOnError(err, "Unable to query uversions in ds value set")
}

func (self *MapVersioned) GetKeys() ([]interface{}, error) {

	bytekeys, err := self.kvset.getKeys()
	if err != nil {
		return nil, utils.StackError(err, "Unable to read ds  value set keys")
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

func (self *MapVersioned) Print(params ...int) {

	if len(params) > 0 {
		self.kvset.Print(params[0])
	} else {
		self.kvset.Print()
	}
}

func (self *MapVersioned) getMapVersionedKey() []byte {
	return self.kvset.getSetKey()
}
