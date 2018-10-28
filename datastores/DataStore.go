/* Data stores as base for persistence
 *
 * Datastore: A folder that holds all the relevant data and in which a multitude of
 *            different databases can be placed. The datastore handles the creation
 *            and management of all different databases
 * Database:  Special type of storage with unique properties, e.g. KeyValue database,
 *            relational database etc. A database lives within a Datastorage and is
 *            managed by it. It provides access to its functionality in sub entries,
 *            meaning it provides its special storage for multiple keys.
 * Set:     A set in a database for a certain key. The Database has a set for
 *            each key. Set means seperated group, and can contain a hughe amount
 *            of data. E.g. a Set for a KeyValue database is just a group of keys,
 *            and can have unlimited key value pairs.
 *
 */
package datastore

import (
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
	VersionedData

	IsValid() bool
	Print()
}

type StorageType int

const (
	KeyValue StorageType = 1
	MapType  StorageType = 2
)

func NewDatastore(path string) (*Datastore, error) {

	//make sure the path exist...
	dir := filepath.Join(path, "Datastore")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	//build the default blt db
	path = filepath.Join(path, "bolt.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	keyvalue, err := NewKeyValueDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	mapdb, err := NewMapDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	stores := make(map[StorageType]DataBase, 0)
	stores[KeyValue] = keyvalue
	stores[MapType] = mapdb

	return &Datastore{db, stores}, nil
}

type Datastore struct {
	boltdb *bolt.DB
	stores map[StorageType]DataBase
}

func (self *Datastore) GetDatabase(kind StorageType) DataBase {

	store, ok := self.stores[kind]
	if !ok {
		panic("no such storage available")
	}

	return store
}

func (self *Datastore) GetOrCreateSet(kind StorageType, set [32]byte) Set {

	store, ok := self.stores[kind]
	if !ok {
		panic("no such storage available")
	}

	return store.GetOrCreateSet(set)
}

func (self *Datastore) Close() {

	for _, store := range self.stores {
		store.Close()
	}

	//close the boltdb
	self.boltdb.Close()
}
