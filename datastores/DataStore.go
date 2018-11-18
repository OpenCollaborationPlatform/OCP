/* Data dbs as base for persistence
 *
 * Datastore: A folder that holds all the relevant data and in which a multitude of
 *            different databases can be placed. The datastore handles the creation
 *            and management of all different databases
 * Database:  Special type of storage with unique properties, e.g. ValueVersionedType database,
 *            relational database etc. A database lives within a Datastorage and is
 *            managed by it. It provides access to its functionality in sub entries,
 *            meaning it provides its special storage for multiple keys.
 * Set:       A set in a database for a certain key. Set means seperated group, and
 *            can contain a hughe amount of data. E.g. a Set for a ValueVersionedType database
 *            is a group of multiple ValueVersioneds, accessed by keys. A MapVersionedType is a group
 *            of MapVersioneds, each accessed by a key. Keys in a set cannot be removed and
 *            must be added at the beginning, bevor versioning, as they are not part
 *            of the versioning process. Versioning happens inside the set, e.g.
 *            for a ValueVersionedType set the individual valueVersioneds are versioned.
 *
 *
 * General properties:
 * - Not enabled for concurrent usage, user needs to ensure single access
 * - Golang objects do not store any state, hence having multiple objects for the same
 *   data works well
 */
package datastore

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

//Describes a
type DataBase interface {
	Close()
	HasSet(set [32]byte) bool
	GetOrCreateSet(set [32]byte) Set
	RemoveSet(set [32]byte) error
}

//Describes a single set in a store and allows to access it
type Set interface {
	GetType() StorageType
	IsValid() bool
	Print(params ...int)
}

type VersionedSet interface {
	VersionedData
	Set
}

type StorageType uint64

const (
	ValueType StorageType = 1
	MapType   StorageType = 3
	IndexType StorageType = 4
)

var StorageTypes = []StorageType{ValueType, MapType, IndexType}

func NewDatastore(path string) (*Datastore, error) {

	//make sure the path exist...
	dir := filepath.Join(path, "Datastore")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	//database storages
	dbs := make(map[StorageType]DataBase, 0)
	vdbs := make(map[StorageType]DataBase, 0)

	//build the default blt db
	path = filepath.Join(path, "bolt.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	value, err := NewValueDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	dbs[ValueType] = value

	valueVersioned, err := NewValueVersionedDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	vdbs[ValueType] = valueVersioned

	map_, err := NewMapDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	dbs[MapType] = map_

	mapVersioned, err := NewMapVersionedDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	vdbs[MapType] = mapVersioned

	index, err := NewIndexDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	dbs[IndexType] = index

	return &Datastore{db, dbs, vdbs}, nil
}

type Datastore struct {
	boltdb *bolt.DB
	dbs    map[StorageType]DataBase
	vDbs   map[StorageType]DataBase
}

func (self *Datastore) GetDatabase(kind StorageType, versioned bool) (DataBase, error) {

	var db DataBase
	var ok bool
	if versioned {
		db, ok = self.vDbs[kind]
	} else {
		db, ok = self.dbs[kind]
	}
	if !ok {
		return nil, fmt.Errorf("No such database type available")
	}

	return db, nil
}

func (self *Datastore) GetOrCreateSet(kind StorageType, versioned bool, set [32]byte) (Set, error) {

	db, err := self.GetDatabase(kind, versioned)
	if err != nil {
		return nil, err
	}
	return db.GetOrCreateSet(set), nil
}

func (self *Datastore) Close() {

	for _, store := range self.dbs {
		store.Close()
	}
	for _, store := range self.vDbs {
		store.Close()
	}

	//close the boltdb
	self.boltdb.Close()
}
