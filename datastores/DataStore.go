/* Data stores as base for persistence
 *
 * Datastore: A folder that holds all the relevant data and in which a multitude of
 *            different databases can be placed. The datastore handles the creation
 *            and management of all different databases
 * Database:  Special type of storage with unique properties, e.g. KeyValue database,
 *            relational database etc. A database lives within a Datastorage and is
 *            managed by it. It provides access to its functionality in sub entries,
 *            meaning it provides its special storage for multiple keys.
 * Entry:     A entry in a database for a certain key. The Database has a entry for
 *            each key. Entry means seperated group, and can contain a hughe amount
 *            of data. E.g. a Entry for a KeyValue database is just a group of keys,
 *            and can have unlimited key value pairs.
 *
 */
package datastore

import (
	"os"
	"path/filepath"
)

//Describes a
type DataBase interface {
	Close()
	HasEntry(entry [32]byte) bool
	GetOrCreateEntry(entry [32]byte) Entry
	RemoveEntry(entry [32]byte) error
}

//Describes a single entry in a store and allows to access it
type Entry interface {
	//GetDatabase() DataBase
	IsValid() bool
}

type StorageType int

const (
	KeyValue StorageType = 1
)

func NewDatastore(path string) (*Datastore, error) {

	//make sure the path exist...
	dir := filepath.Join(path, "Datastore")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	keyvalue, err := NewKeyValueDatabase(dir, "keyvalue")
	if err != nil {
		return nil, err
	}

	stores := make(map[StorageType]DataBase, 0)
	stores[KeyValue] = keyvalue

	return &Datastore{stores}, nil
}

type Datastore struct {
	stores map[StorageType]DataBase
}

func (self *Datastore) GetDatabase(kind StorageType) DataBase {

	store, ok := self.stores[kind]
	if !ok {
		panic("no such storage available")
	}

	return store
}

func (self *Datastore) GetOrCreateEntry(kind StorageType, entry [32]byte) Entry {

	store, ok := self.stores[kind]
	if !ok {
		panic("no such storage available")
	}

	return store.GetOrCreateEntry(entry)
}

func (self *Datastore) Close() {

	for _, store := range self.stores {
		store.Close()
	}
}
