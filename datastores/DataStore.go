/* Data dbs as base for persistence
 *
 * Datastore: A type that holds all the relevant data and in which a multitude of
 *            different databases can exist. The datastore handles the creation
 *            and management of all different databases
 * Key:	  	  A universal way of accessing data entries within a datastore without knowing any
 *			  specifics about it. A key consists of the DB type, the set key and than
 *			  multiple entrie keys, dependend on the required recursiveness
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
 * Entry: 	  A data entry within a database. It is accessed via a key in a Set, and can be
 *			  almost anything dependend in type of database. E.g for a ValueType Set a entry
 * 			  is just a single value. For a MapType a entry is a Map. Entries are recursive,
 * 			  that means an entry can hold other entries also accessbile by key. This goes
 * 			  down till no subentries are available. E.g. a ValueType entry does not have
 *			  any more subentries, but a MapType entry has one subentry for each map key.
 * 			  Entries can be Versioned, as well as subentries. Any versioning action on a
 * 			  VersionedEntry is also applied to the subentries.
 *
 * General properties:
 * - Not enabled for concurrent usage, user needs to ensure single access
 * - Golang objects do not store any state, hence having multiple objects for the same
 *   data works well
 */
package datastore

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/boltdb/bolt"
)

func NewDSError(reason, msg string, args ...interface{}) utils.OCPError {
	err := utils.NewError(utils.Internal, "DS", reason, msg, args)
	return err
}

func wrapDSError(err error, reason string) error {
	if err != nil {
		return utils.NewError(utils.Internal, "DS", reason, err.Error())
	}
	return err
}

//DS error reasons
const Error_Key_Not_Existant = "key_not_existant"
const Error_Invalid_Data = "invalid_data"
const Error_Operation_Invalid = "operation_invalid"
const Error_Bolt_Access_Failure = "bolt_access_failure"
const Error_Setup_Incorrectly = "setup_incorectly"
const Error_Transaction_Invalid = "transaction_invalid"
const Error_Datastore_invalid = "datastore_invalid"

//Describes a Database of any kind, parent of sets of a storage type
type DataBase interface {
	Close()
	HasSet(set [32]byte) (bool, error)
	GetOrCreateSet(set [32]byte) (Set, error)
	RemoveSet(set [32]byte) error
}

type Entry interface {
	SupportsSubentries() bool
	GetEntry(interface{}) (Entry, error)
	Erase() error //Erases completely, 100%, including all version information if available. No checks done. Use carfully
}

type VersionedEntry interface {
	VersionedData
	Entry

	GetVersionedEntry(interface{}) (VersionedEntry, error)
}

//Set: Describes a single set in a store and allows to access it
type baseSet interface {
	GetType() StorageType
	IsValid() bool
	Print(params ...int)
}

type Set interface {
	Entry
	baseSet
}

type VersionedSet interface {
	VersionedEntry
	baseSet
}

type StorageType uint64

const (
	ValueType StorageType = 1
	MapType   StorageType = 3
	ListType  StorageType = 4
)

var StorageTypes = []StorageType{ValueType, MapType, ListType}

func NewKey(stype StorageType, versioned bool, set [32]byte, entries ...interface{}) Key {
	return Key{stype, versioned, set, entries}
}

type Key struct {
	Type      StorageType
	Versioned bool
	Set       [32]byte
	Entries   []interface{}
}

func NewDatastore(path string) (*Datastore, error) {

	//make sure the path exist...
	dir := filepath.Join(path, "Datastore")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, NewDSError(Error_Datastore_invalid, err.Error(), dir)
	}

	//database storages
	dbs := make(map[StorageType]DataBase, 0)
	vdbs := make(map[StorageType]DataBase, 0)

	//build the default blt db
	path = filepath.Join(dir, "bolt.db")
	db_, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, NewDSError(Error_Datastore_invalid, err.Error(), path)
	}
	bolt := boltWrapper{db_, nil, sync.Mutex{}}

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
		return nil, NewDSError(Error_Setup_Incorrectly, "Storage type not available", "Type", kind)
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

//Retrieve any entry from within the datastore
func (self *Datastore) GetEntry(key Key) (Entry, error) {

	var db DataBase
	var ok bool
	if key.Versioned {
		db, ok = self.vDbs[key.Type]
	} else {
		db, ok = self.dbs[key.Type]
	}
	if !ok {
		return nil, NewDSError(Error_Setup_Incorrectly, "Storage type not available", "Type", key.Type)
	}

	if has, err := db.HasSet(key.Set); !has || err != nil {
		return nil, NewDSError(Error_Key_Not_Existant, "Set does not exist in Database", "Type", key.Type, "Set", key.Set)
	}

	set, err := db.GetOrCreateSet(key.Set)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access set in DB, even though it should exist", "Type", key.Type, "Set", key.Set)
	}

	if len(key.Entries) < 1 {
		return nil, NewDSError(Error_Operation_Invalid, "No entries into set specified")
	}

	//get set entry
	entry, err := set.GetEntry(key.Entries[0])
	if err != nil {
		return nil, utils.StackError(err, "Unable to access entry in set", "Type", key.Type, "Set", key.Set, "Entry", key.Entries[0])
	}

	//get subentries if available
	if len(key.Entries) > 1 && !entry.SupportsSubentries() {
		return nil, NewDSError(Error_Operation_Invalid, "Key specifies subentries, but entry doe not support those")
	}
	for _, v := range key.Entries[1:] {
		entry, err = entry.GetEntry(v)
		if err != nil {
			return nil, utils.StackError(err, "Unable to access subentry", "Type", "Entry", v)
		}
	}
	return entry, nil
}

//Retrieve any versioned entry from the datastore
func (self *Datastore) GetVersionedEntry(key Key) (VersionedEntry, error) {

	if !key.Versioned {
		return nil, NewDSError(Error_Operation_Invalid, "Accessing of versioned entry with unversioned key")
	}

	entry, err := self.GetEntry(key)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access entry as specified by key", "Key", key)
	}

	ventry, ok := entry.(VersionedEntry)
	if !ok {
		return nil, NewDSError(Error_Invalid_Data, "Versioned entry is of wrong type")
	}

	return ventry, nil
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

func (self *Datastore) RollbackKeepOpen() error {
	return self.boltdb.RollbackKeepOpen()
}

func (self *Datastore) Close() error {

	//just in case something is still open!
	self.boltdb.Rollback()

	for _, store := range self.dbs {
		store.Close()
	}
	for _, store := range self.vDbs {
		store.Close()
	}

	//close the boltdb
	return wrapDSError(self.boltdb.db.Close(), Error_Datastore_invalid)
}

func (self *Datastore) Delete() error {

	//we fully remove the datastore!
	self.Close()
	return wrapDSError(os.RemoveAll(self.path), Error_Datastore_invalid)
}

func (self *Datastore) Path() string {
	return self.path
}

//prepares for a backup: afterwards the directory can simply be copied!
func (self *Datastore) PrepareFileBackup() error {

	//close boltdb!
	return wrapDSError(self.boltdb.db.Close(), Error_Datastore_invalid)
}

//finishes backup: normal operation is restored afterwards
func (self *Datastore) FinishFileBackup() error {

	//reopen the db
	path := filepath.Join(self.path, "bolt.db")
	db_, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return wrapDSError(err, Error_Datastore_invalid)
	}
	self.boltdb.db = db_

	return nil
}
