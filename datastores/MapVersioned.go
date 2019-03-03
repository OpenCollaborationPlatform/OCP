// MapVersioned database: The mapVersioned database saves multiple mapVersioneds per set which are accessed by a key
package datastore

import (
	"CollaborationNode/utils"
	"bytes"
	"fmt"

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
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("MapVersioned"))
		return nil
	})

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

			//setup the basic structure
			err = bucket.Put(itob(CURRENT), itob(HEAD))
			if err != nil {
				return err
			}
			_, err = bucket.CreateBucketIfNotExists(itob(VERSIONS))

			return nil
		})
	}

	return &MapVersionedSet{self.db, self.dbkey, set[:]}, nil
}

func (self MapVersionedDatabase) RemoveSet(set [32]byte) error {

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
func (self *MapVersionedSet) IsValid() (bool, error) {

	return true, nil
}

func (self *MapVersionedSet) Print(params ...int) {

	if valid, _ := self.IsValid(); !valid {
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
						str = str + string(stob(mk)) + ": %v,  "
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
			return false, utils.StackError(err, "Unable to check for updates")
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
		return VersionID(INVALID), err
	}
	if !cv.IsHead() {
		return VersionID(INVALID), fmt.Errorf("Unable to create version if HEAD is not checked out")
	}

	//collect all versions we need for the current version
	version := make(map[string]string, 0)
	mapVersioneds := self.collectMapVersioneds()
	for _, mp := range mapVersioneds {
		if has, _ := mp.HasUpdates(); has {
			v, err := mp.kvset.FixStateAsVersion()
			if err != nil {
				return VersionID(INVALID), err
			}
			version[btos(mp.getMapVersionedKey())] = itos(uint64(v))

		} else {
			v, err := mp.kvset.GetLatestVersion()
			if err != nil {
				return VersionID(INVALID), err
			}
			version[btos(mp.getMapVersionedKey())] = itos(uint64(v))
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

func (self *MapVersionedSet) getVersionInfo(id VersionID) (map[string]interface{}, error) {

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
		resmapVersioned, ok := res.(map[string]interface{})
		if !ok {
			return fmt.Errorf("Problem with parsing the saved data")
		}
		version = resmapVersioned
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
	var version map[string]interface{}
	if !id.IsHead() {
		var err error
		version, err = self.getVersionInfo(id)
		if err != nil {
			return err
		}
	}

	//load the versions of the individual mapVersioneds
	mapVersioneds := self.collectMapVersioneds()
	for _, mp := range mapVersioneds {

		if id.IsHead() {
			mp.kvset.LoadVersion(id)

		} else {
			v, ok := version[btos(mp.getMapVersionedKey())]
			if !ok {
				return fmt.Errorf("Unable to load version information for %v", string(mp.getMapVersionedKey()))
			}
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("Stored version information has wrong format")
			}
			mp.kvset.LoadVersion(VersionID(stoi(s)))
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

func (self *MapVersionedSet) GetLatestVersion() (VersionID, error) {

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

func (self *MapVersionedSet) GetCurrentVersion() (VersionID, error) {

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

func (self *MapVersionedSet) RemoveVersionsUpTo(ID VersionID) error {

	mapVersioneds := self.collectMapVersioneds()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, mp := range mapVersioneds {
		//remove up to version
		val := version[btos(mp.getMapVersionedKey())]
		ival := stoi(val.(string))
		err := mp.kvset.RemoveVersionsUpTo(VersionID(ival))
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

func (self *MapVersionedSet) RemoveVersionsUpFrom(ID VersionID) error {

	mapVersioneds := self.collectMapVersioneds()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, mp := range mapVersioneds {
		//remove up to version
		val := version[btos(mp.getMapVersionedKey())]
		ival := stoi(val.(string))
		err := mp.kvset.RemoveVersionsUpFrom(VersionID(ival))
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
		return err
	}
	return pair.Write(valueVersioned)
}

func (self *MapVersioned) IsValid() (bool, error) {

	return self.kvset.IsValid()
}

func (self *MapVersioned) HasKey(key interface{}) bool {

	k, err := getBytes(key)
	if err != nil {
		return false
	}
	return self.kvset.HasKey(k)
}

func (self *MapVersioned) Read(key interface{}) (interface{}, error) {

	k, err := getBytes(key)
	if err != nil {
		return nil, err
	}
	pair, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return nil, err
	}
	return pair.Read()
}

func (self *MapVersioned) ReadType(key interface{}, value interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	pair, err := self.kvset.GetOrCreateValue(k)
	if err != nil {
		return err
	}
	return pair.ReadType(value)
}

func (self *MapVersioned) Remove(key interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return utils.StackError(err, "Cannot remove MapVersioned key")
	}
	return self.kvset.removeKey(k)
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

	return self.kvset.HasUpdates()
}

func (self *MapVersioned) HasVersions() (bool, error) {

	return self.kvset.HasVersions()
}

func (self *MapVersioned) GetKeys() ([]interface{}, error) {

	bytekeys, err := self.kvset.getKeys()
	if err != nil {
		return nil, utils.StackError(err, "Unable to read map keys")
	}

	//convert from byte keys to user type keys
	keys := make([]interface{}, len(bytekeys))
	for i, bytekey := range bytekeys {
		key, err := getInterface(bytekey)
		if err != nil {
			return nil, utils.StackError(err, "Unable to convert a key into user type")
		}
		keys[i] = key
	}

	return keys, nil
}

func (self *MapVersioned) getMapVersionedKey() []byte {
	return self.kvset.getSetKey()
}
