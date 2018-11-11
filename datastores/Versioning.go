package datastore

import (
	"fmt"
	"math"

	"github.com/boltdb/bolt"
)

type VersionID uint64

const (
	INVALID uint64 = math.MaxUint64
	HEAD    uint64 = math.MaxUint64 - 1
)

func (self VersionID) IsHead() bool {
	return uint64(self) == HEAD
}

func (self VersionID) IsValid() bool {
	return uint64(self) != INVALID
}

/* VersionedData interface: Handle any kind of data according to versioning rules
 *
 * - A Version is only created on demand from current state (FixStateAsVersion)
 * - If the loaded version is not the highest one no change of data is allowed
 * - The Versioned data is requried to check on write if an older version is loaded
 *   and prevent write actively
 * - To change data based on older version, all newer versions muse be deleted
 *   and normal processing of change and versioning goes on
 *   (in general this means forking or removal of version data)
 */
type VersionedData interface {
	HasUpdates() bool
	ResetHead()
	FixStateAsVersion() (VersionID, error)
	LoadVersion(id VersionID) error
	GetLatestVersion() (VersionID, error)
	GetCurrentVersion() (VersionID, error)
	RemoveVersionsUpTo(VersionID) error
	RemoveVersionsUpFrom(VersionID) error
}

/* Makes version managing for a number of equal key sets easy
 *
 * It is possible to have multiple sets for the same key, just different kinds. As a
 * set is able to be versioned, it might be useful to version those sets with a
 * single key together, meaning creating versions over the whole collection of sets.
 * This is handled by the VersionManager, who collects all sets for a key and
 * provides the same functionality for versioning as the set itself.
 */
type VersionManager interface {
	VersionedData
	GetDatabaseSet(sType StorageType) Set
}

func NewVersionManager(key [32]byte, ds *Datastore) VersionManagerImp {
	mngr := VersionManagerImp{key, ds}

	//make sure the default data layout is available
	ds.boltdb.Update(func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists([]byte("VersionManager"))
		if err != nil {
			return err
		}
		_, err = bucket.CreateBucketIfNotExists(key[:])
		if err != nil {
			return err
		}
		return nil
	})

	return mngr
}

type VersionManagerImp struct {
	key   [32]byte
	store *Datastore
}

/*
Data layout for VersionManagerImp

bucket(SetKey) [
	entry(1) = [KeyValue: 1, Map: 3]
	entry(2) = [KeyValue: 2, Map: 3]
	entry(CURRENT) = HEAD
]
*/

func (self *VersionManagerImp) GetDatabaseSet(sType StorageType) Set {
	return self.store.GetOrCreateSet(sType, self.key)
}

//VerionedData interface
//******************************************************************************
func (self *VersionManagerImp) collectSets() []Set {

	sets := make([]Set, 0)
	for _, stype := range StorageTypes {
		if self.store.GetDatabase(stype).HasSet(self.key) {
			sets = append(sets, self.store.GetDatabase(stype).GetOrCreateSet(self.key))
		}
	}

	return sets
}

func (self *VersionManagerImp) HasUpdates() bool {

	//if we have no version yet we have updates to allow to come back to default
	var updates bool
	self.store.boltdb.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])
		updates = bucket.Sequence() == 0
		return nil
	})

	//if we already have versions check the individual sets
	if !updates {

		sets := self.collectSets()
		for _, set := range sets {
			if set.HasUpdates() {
				return true
			}
		}
	}

	return updates
}

func (self *VersionManagerImp) ResetHead() {

	sets := self.collectSets()
	for _, set := range sets {
		set.ResetHead()
	}
}

func (self *VersionManagerImp) FixStateAsVersion() (VersionID, error) {

	//we go over all sets and fix their version.
	version := make(map[string]string)
	sets := self.collectSets()
	for _, set := range sets {

		if set.HasUpdates() {

			//we need to create and store a new version
			v, err := set.FixStateAsVersion()
			if err != nil {
				return v, err
			}
			version[itos(uint64(set.GetType()))] = itos(uint64(v))

		} else {

			//having no updates means we are able to reuse the latest version
			//and don't need to add a new one
			v, err := set.GetLatestVersion()
			if err != nil {
				return v, err
			}
			version[itos(uint64(set.GetType()))] = itos(uint64(v))
		}
	}

	//store the version data
	id := VersionID(INVALID)
	err := self.store.boltdb.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

		data, err := getBytes(version)
		if err != nil {
			return err
		}
		intid, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		err = bucket.Put(itob(intid), data)
		if err != nil {
			return err
		}
		id = VersionID(intid)
		return nil
	})

	return id, err
}

func (self *VersionManagerImp) getVersionInfo(id VersionID) (map[string]interface{}, error) {

	version := make(map[string]interface{})
	err := self.store.boltdb.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

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
			return fmt.Errorf("Problem with parsing the saved data, type: %T", res)
		}
		version = resmap
		return nil
	})
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (self *VersionManagerImp) LoadVersion(id VersionID) error {

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

	//go through all sets and set the version
	for _, set := range self.collectSets() {

		if id.IsHead() {
			err := set.LoadVersion(id)
			if err != nil {
				return err
			}

		} else {
			data, ok := version[itos(uint64(set.GetType()))]
			if !ok {
				return fmt.Errorf("No version saved for the set")
			}
			strid, ok := data.(string)
			if !ok {
				return fmt.Errorf("Unable to read saved data: %T", data)
			}
			err := set.LoadVersion(VersionID(stoi(strid)))
			if err != nil {
				return err
			}
		}
	}

	//we write the current version
	err := self.store.boltdb.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

		bucket.Put(itob(CURRENT), itob(uint64(id)))
		return nil
	})

	return err
}

func (self *VersionManagerImp) GetLatestVersion() (VersionID, error) {

	var version uint64 = 0
	found := false
	err := self.store.boltdb.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val != CURRENT {
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
		return VersionID(INVALID), fmt.Errorf("No versions saved yet")
	}

	return VersionID(version), err
}

func (self *VersionManagerImp) GetCurrentVersion() (VersionID, error) {

	current := INVALID
	err := self.store.boltdb.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

		data := bucket.Get(itob(CURRENT))
		if data == nil {
			return fmt.Errorf("Current version invalid: was never set")
		}
		current = btoi(data)
		return nil
	})

	return VersionID(current), err
}

func (self *VersionManagerImp) RemoveVersionsUpTo(ID VersionID) error {

	sets := self.collectSets()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, set := range sets {
		//remove up to version
		val := version[itos(uint64(set.GetType()))]
		ival := stoi(val.(string))
		err := set.RemoveVersionsUpTo(VersionID(ival))
		if err != nil {
			return err
		}
	}

	//remove the versions from the relevant bucket
	err = self.store.boltdb.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

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

func (self *VersionManagerImp) RemoveVersionsUpFrom(ID VersionID) error {

	sets := self.collectSets()
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	for _, set := range sets {
		//remove up to version
		val := version[itos(uint64(set.GetType()))]
		ival := stoi(val.(string))
		err := set.RemoveVersionsUpFrom(VersionID(ival))
		if err != nil {
			return err
		}
	}

	//remove the versions from the relevant bucket
	err = self.store.boltdb.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("VersionManager"))
		bucket = bucket.Bucket(self.key[:])

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
