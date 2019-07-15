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

	"CollaborationNode/utils"

	"github.com/boltdb/bolt"
)

//Describes a
type DataBase interface {
	Close()
	HasSet(set [32]byte) (bool, error)
	GetOrCreateSet(set [32]byte) (Set, error)
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
	ListType  StorageType = 4
)

var StorageTypes = []StorageType{ValueType, MapType, ListType}

func NewDatastore(path string) (*Datastore, error) {

	//make sure the path exist...
	dir := filepath.Join(path, "Datastore")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, utils.StackError(err, "Cannot open path %s", dir)
	}

	//database storages
	dbs := make(map[StorageType]DataBase, 0)
	vdbs := make(map[StorageType]DataBase, 0)

	//build the default blt db
	path = filepath.Join(dir, "bolt.db")
	db_, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, utils.StackError(err, "Unable to open bolt db: %s", path)
	}
	bolt := boltWrapper{db_, nil}

	//open to allow creation of default setups
	bolt.Begin()
	defer bolt.Commit()

	value, err := NewValueDatabase(&bolt)
	if err != nil {
		db_.Close()
		return nil, utils.StackError(err, "Unable to open value database")
	}
	dbs[ValueType] = value

	valueVersioned, err := NewValueVersionedDatabase(&bolt)
	if err != nil {
		db_.Close()
		return nil, utils.StackError(err, "Unable to open versioned value database")
	}
	vdbs[ValueType] = valueVersioned

	map_, err := NewMapDatabase(&bolt)
	if err != nil {
		db_.Close()
		return nil, utils.StackError(err, "Unable to open map database")
	}
	dbs[MapType] = map_

	mapVersioned, err := NewMapVersionedDatabase(&bolt)
	if err != nil {
		db_.Close()
		return nil, utils.StackError(err, "Unable to open versioned map database")
	}
	vdbs[MapType] = mapVersioned

	list, err := NewListDatabase(&bolt)
	if err != nil {
		db_.Close()
		return nil, utils.StackError(err, "Unable to open list database")
	}
	dbs[ListType] = list

	listVersioned, err := NewListVersionedDatabase(&bolt)
	if err != nil {
		db_.Close()
		return nil, utils.StackError(err, "Unable to open versioned list database")
	}
	vdbs[ListType] = listVersioned

	return &Datastore{&bolt, dir, dbs, vdbs}, nil
}

type Datastore struct {
	boltdb *boltWrapper
	path   string
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
		return nil, utils.StackError(err, "Unable to get database of type %v (versioned=%v)", kind, versioned)
	}
	return db.GetOrCreateSet(set)
}

func (self *Datastore) Begin() error {
	return self.boltdb.Begin()
}

func (self *Datastore) Commit() error {
	return self.boltdb.Commit()
}

func (self *Datastore) Rollback() error {
	return self.boltdb.Rollback()
}

func (self *Datastore) Close() {

	//just in case something is still open!
	self.boltdb.Rollback()

	for _, store := range self.dbs {
		store.Close()
	}
	for _, store := range self.vDbs {
		store.Close()
	}

	//close the boltdb
	self.boltdb.db.Close()
}

func (self *Datastore) Delete() error {

	//we fully remove the datastore!
	self.Close()
	return os.RemoveAll(self.path)
}

func (self *Datastore) Path() string {
	return self.path
}

//prepares for a backup: afterwards the directory can simply be copied!
func (self *Datastore) PrepareFileBackup() error {

	//close boltdb!
	return self.boltdb.db.Close()
}

//finishes backup: normal operation is restored afterwards
func (self *Datastore) FinishFileBackup() error {

	//reopen the db
	path := filepath.Join(self.path, "bolt.db")
	db_, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return utils.StackError(err, "Unable to open bolt db: %s", path)
	}
	self.boltdb.db = db_

	return err
}
