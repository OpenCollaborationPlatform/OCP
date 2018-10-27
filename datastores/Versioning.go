package datastore

import "math"

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
	FixStateAsVersion() (VersionID, error)
	LoadVersion(id VersionID) error
	GetLatestVersion() (VersionID, error)
	GetCurrentVersion() (VersionID, error)
	RemoveVersionsUpTo(VersionID) error
	RemoveVersionsUpFrom(VersionID) error
}

/* Makes version managing for a number of sets easy
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

func NewVersionManager(key [32]byte, ds *Datastore) VersionManager {
	mngr := VersionManagerImp{key, ds, make(map[StorageType]Set)}
	return &mngr
}

type VersionManagerImp struct {
	key   [32]byte
	store *Datastore
	sets  map[StorageType]Set
}

func (self *VersionManagerImp) GetDatabaseSet(sType StorageType) Set {

	set, ok := self.sets[sType]
	if !ok {
		return self.store.GetOrCreateSet(sType, self.key)
	}
	return set
}

func (self *VersionManagerImp) FixStateAsVersion() (VersionID, error) {

	return 1, nil
}

func (self *VersionManagerImp) LoadVersion(id VersionID) error {

	return nil
}

func (self *VersionManagerImp) GetLatestVersion() (VersionID, error) {

	return 1, nil
}

func (self *VersionManagerImp) GetCurrentVersion() (VersionID, error) {

	return 1, nil
}

func (self *VersionManagerImp) RemoveVersionsUpTo(VersionID) error {
	return nil
}

func (self *VersionManagerImp) RemoveVersionsUpFrom(VersionID) error {
	return nil
}
