package datastore

import (
	"os"
	"path/filepath"
)

type StorageType int

const (
	KeyValue StorageType = 1
)

type DataBase interface {
	Close()
	HasStore(name string) bool
	GetOrCreateStore(name string) Store
}

//the interface of all the different storages, no matter the underlaying type
type Store interface {
	HasEntry(name string) bool
	GetOrCreateEntry(name string) Entry
	RemoveEntry(name string)
}

//Describes a single entry in a store and allows to access it
type Entry interface {
	IsValid() bool
	Read() (interface{}, error)
	Write(interface{}) error

	Remove() bool
}

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

func (self *Datastore) GetOrCreateStore(kind StorageType, name string) Store {

	store, ok := self.stores[kind]
	if !ok {
		panic("no such storage available")
	}

	return store.GetOrCreateStore(name)
}

func (self *Datastore) Close() {

	for _, store := range self.stores {
		store.Close()
	}
}
