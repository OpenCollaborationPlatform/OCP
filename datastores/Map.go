// Map database: The map database saves multiple maps per set which are accessed by a key
package datastore

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
)

/*
Map database uses a keyvalue database underneath, just one level deeper in the
hirarchy.  Each ValueSet is a single map.

Data layout of versioned map store:

bucket(SetKey) [
    entry(CURRENT) = HEAD
	ValueSet(VERSIONS) [
		entry(1) = Versionmap(key1->1, key2->1)
		entry(2) = Versionmap(key1->2, key2->1)
	]
	ValueSet(key1)
	ValueSet(key2)
]
*/

func NewMapDatabase(db *bolt.DB) (*MapDatabase, error) {

	//make sure key value store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("map"))
		return nil
	})

	return &MapDatabase{db, []byte("map")}, nil
}

//implements the database interface
type MapDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self MapDatabase) HasSet(set [32]byte) bool {

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

func (self MapDatabase) GetOrCreateSet(set [32]byte) Set {

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

	return &MapSet{self.db, self.dbkey, set[:]}
}

func (self MapDatabase) RemoveSet(set [32]byte) error {

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

func (self MapDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type MapSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey []byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *MapSet) IsValid() bool {

	return true
}

func (self *MapSet) Print(params ...int) {

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
				kvset := ValueSet{self.db, self.dbkey, [][]byte{self.setkey, k}}
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

func (self *MapSet) collectMaps() []Map {

	maps := make([]Map, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				//key must be copied, as it gets invalid outside of ForEach
				var key = make([]byte, len(k))
				copy(key, k)
				mp := newMap(self.db, self.dbkey, [][]byte{self.setkey, key})
				maps = append(maps, mp)
			}
			return nil
		})
		return nil
	})
	return maps
}

func (self *MapSet) HasUpdates() bool {

	//we cycle through all maps and check if they have updates
	maps := self.collectMaps()
	for _, mp := range maps {
		if mp.HasUpdates() {
			return true
		}
	}
	return false
}

func (self *MapSet) ResetHead() {

	maps := self.collectMaps()
	for _, mp := range maps {
		mp.kvset.ResetHead()
	}
}

func (self *MapSet) FixStateAsVersion() (VersionID, error) {

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
	maps := self.collectMaps()
	for _, mp := range maps {
		if mp.HasUpdates() {
			v, err := mp.kvset.FixStateAsVersion()
			if err != nil {
				return VersionID(INVALID), err
			}
			version[btos(mp.getMapKey())] = itos(uint64(v))

		} else {
			v, err := mp.kvset.GetLatestVersion()
			if err != nil {
				return VersionID(INVALID), err
			}
			version[btos(mp.getMapKey())] = itos(uint64(v))
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

func (self *MapSet) getVersionInfo(id VersionID) (map[string]interface{}, error) {

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
		resmap, ok := res.(map[string]interface{})
		if !ok {
			return fmt.Errorf("Problem with parsing the saved data")
		}
		version = resmap
		return nil
	})
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (self *MapSet) LoadVersion(id VersionID) error {

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

	//load the versions of the individual maps
	maps := self.collectMaps()
	for _, mp := range maps {

		if id.IsHead() {
			mp.kvset.LoadVersion(id)

		} else {
			v, ok := version[btos(mp.getMapKey())]
			if !ok {
				return fmt.Errorf("Unable to load version information for %v", string(mp.getMapKey()))
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

func (self *MapSet) GetLatestVersion() (VersionID, error) {

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

func (self *MapSet) GetCurrentVersion() (VersionID, error) {

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

func (self *MapSet) RemoveVersionsUpTo(ID VersionID) error {

	maps := self.collectMaps()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, mp := range maps {
		//remove up to version
		val := version[btos(mp.getMapKey())]
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

func (self *MapSet) RemoveVersionsUpFrom(ID VersionID) error {

	maps := self.collectMaps()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, mp := range maps {
		//remove up to version
		val := version[btos(mp.getMapKey())]
		ival := stoi(val.(string))
		fmt.Printf("Remove up from: %v\n", ival)
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
 * Map functions
 * ********************************************************************************
 */
func (self *MapSet) HasMap(key []byte) bool {

	var result bool
	self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		bucket = bucket.Bucket(self.setkey)

		result = bucket.Bucket(key) != nil
		return nil
	})

	return result
}

func (self *MapSet) GetOrCreateMap(key []byte) (*Map, error) {

	if !self.HasMap(key) {

		curr, err := self.GetCurrentVersion()
		if err != nil {
			return nil, err
		}
		if !curr.IsHead() {
			return nil, fmt.Errorf("Key does not exist and cannot be created when version is loaded")
		}

		//make sure the set exists in the db with null value
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

	mp := newMap(self.db, self.dbkey, [][]byte{self.setkey, key})
	return &mp, nil
}

/*
 * Map functions
 * ********************************************************************************
 */

type Map struct {
	kvset ValueSet
}

func newMap(db *bolt.DB, dbkey []byte, mapkeys [][]byte) Map {

	kv := ValueSet{db, dbkey, mapkeys}
	return Map{kv}
}

func (self *Map) Write(key interface{}, value interface{}) error {

	k, err := getBytes(key)
	if err != nil {
		return err
	}
	pair, err := self.kvset.GetOrCreateKey(k)
	if err != nil {
		return err
	}
	return pair.Write(value)
}

func (self *Map) IsValid() bool {

	return self.kvset.IsValid()
}

func (self *Map) HasKey(key interface{}) bool {

	k, err := getBytes(key)
	if err != nil {
		return false
	}
	return self.kvset.HasKey(k)
}

func (self *Map) Read(key interface{}) (interface{}, error) {

	k, err := getBytes(key)
	if err != nil {
		return nil, err
	}
	pair, err := self.kvset.GetOrCreateKey(k)
	if err != nil {
		return nil, err
	}
	return pair.Read()
}

func (self *Map) Remove(key interface{}) bool {

	k, err := getBytes(key)
	if err != nil {
		return false
	}
	return self.kvset.removeKey(k) == nil
}

func (self *Map) CurrentVersion() VersionID {

	v, _ := self.kvset.GetCurrentVersion()
	return v
}

func (self *Map) LatestVersion() VersionID {

	v, _ := self.kvset.GetLatestVersion()
	return v
}

func (self *Map) HasUpdates() bool {

	return self.kvset.HasUpdates()
}

func (self *Map) getMapKey() []byte {
	return self.kvset.getSetKey()
}
