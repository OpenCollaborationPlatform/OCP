// ValueVersioned database: The key valueVersioned database saves the multiple entries per set which
// are accessed by keys.
package datastore

/*
Data layout of versioned key valueVersioned store:

bucket(SetKey) [
	entry(CURRENT) = HEAD
	bucket(VERSIONS) [
		entry(1) = Versionmap(key1->1, key2->1)
		entry(2) = Versionmap(key1->2, key2->1)
	]
	bucket(key1) [
		entry(1) = "first valueVersioned"
		entry(2) = "second vale"
		entry(CURRENT) = HEAD
		entry(HEAD) = "current valueVersioned"
	]
	bucket(key2) [
		entry(1) = 14
		entry(CURRENT) = HEAD
		entry(HEAD) = 29
	]
]

Definitions:
- CURRENT 	in keys gives the used version number. It is set to INVALID if it does
         	not has a valid version for the checked out key version
- HEAD    	gives the currently worked on valueVersioned. It is set to []byte{} and hence made
		  	invalid on removal
- Bucket sequence: Is alwas on the latest version existing in the bucket
*/

import (
	"CollaborationNode/utils"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/boltdb/bolt"
	"github.com/mr-tron/base58/base58"
)

const (
	CURRENT  uint64 = math.MaxUint64 - 10
	VERSIONS uint64 = math.MaxUint64 - 11
)

func NewValueVersionedDatabase(db *bolt.DB) (*ValueVersionedDatabase, error) {

	//make sure key valueVersioned store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("ValueVersioned"))
		return nil
	})

	return &ValueVersionedDatabase{db, []byte("ValueVersioned")}, nil
}

//implements the database interface
type ValueVersionedDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self ValueVersionedDatabase) HasSet(set [32]byte) bool {

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

func (self ValueVersionedDatabase) GetOrCreateSet(set [32]byte) Set {

	if !self.HasSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			newbucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			//set default valueVersioneds
			newbucket.Put(itob(CURRENT), itob(HEAD))
			newbucket.CreateBucketIfNotExists(itob(VERSIONS))

			return nil
		})
	}

	return &ValueVersionedSet{self.db, self.dbkey, [][]byte{set[:]}}
}

func (self ValueVersionedDatabase) RemoveSet(set [32]byte) error {

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

func (self ValueVersionedDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type ValueVersionedSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *ValueVersionedSet) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool = true
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		if bucket == nil {
			result = false
			return nil
		}
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
			if bucket == nil {
				result = false
				return nil
			}
		}
		return nil
	})

	return result
}

func (self *ValueVersionedSet) Print(params ...int) {

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
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}

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
					//build the versioning string
					str := "["
					data, ok := inter.(map[string]interface{})
					if ok {
						for mk, mv := range data {
							byt := stob(mk)
							if len(byt) == 8 {
								str = str + fmt.Sprintf("%v", btoi(byt)) + ": %v,  "
							} else {
								str = str + string(stob(mk)) + ": %v,  "
							}
							mvid := stoi(mv.(string))
							if mvid == INVALID {
								str = fmt.Sprintf(str, "INVALID")
							} else {
								str = fmt.Sprintf(str, mvid)
							}
						}
					}
					str = str + "]"
					fmt.Printf("%s\t%v: %v\n", indent, btoi(sk), str)
					return nil
				})
			} else {
				//check if it is a number instead of string
				if len(k) == 8 {
					num := btoi(k)
					fmt.Printf("%s%v:\n", indent, num)
				} else {
					fmt.Printf("%s%s:\n", indent, string(k))
				}
				subbucket := bucket.Bucket(k)
				subbucket.ForEach(func(sk []byte, sv []byte) error {
					inter, _ := getInterface(sv)
					key := btoi(sk)
					if key == HEAD {
						fmt.Printf("%s\tHEAD: %v\n", indent, inter)
					} else if key == CURRENT {
						if btoi(sv) == HEAD {
							fmt.Printf("%s\tCURRENT: HEAD\n", indent)
						} else if btoi(sv) == INVALID {
							fmt.Printf("%s\tCURRENT: INVALID\n", indent)
						} else {
							fmt.Printf("%s\tCURRENT: %v\n", indent, btoi(sv))
						}
					} else {
						fmt.Printf("%s\t%v: %v\n", indent, key, inter)
					}
					return nil
				})
			}
			return nil
		})

		return nil
	})
}

func (self *ValueVersionedSet) collectValueVersioneds() []ValueVersioned {

	valueVersioneds := make([]ValueVersioned, 0)
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range self.setkey {
			bucket = bucket.Bucket(bkey)
		}

		bucket.ForEach(func(k []byte, v []byte) error {

			if !bytes.Equal(k, itob(VERSIONS)) && v == nil {
				key := make([]byte, len(k))
				copy(key, k)
				val := ValueVersioned{self.db, self.dbkey, self.setkey, key}
				valueVersioneds = append(valueVersioneds, val)
			}
			return nil
		})
		return nil
	})
	return valueVersioneds
}

func (self *ValueVersionedSet) HasUpdates() bool {

	//if no versions available yet we always have updates!
	updates := !self.HasVersions()

	//check if the individual valueVersioneds have updates
	if !updates {
		valueVersioneds := self.collectValueVersioneds()
		for _, val := range valueVersioneds {
			if val.HasUpdates() {
				return true
			}
		}
	}

	return updates
}

func (self *ValueVersionedSet) HasVersions() bool {

	var versions bool
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(bkey)
		}
		versions = (bucket.Sequence() != 0)
		return nil
	})

	return versions
}

func (self *ValueVersionedSet) ResetHead() {

	valueVersioneds := self.collectValueVersioneds()
	for _, val := range valueVersioneds {
		latest := val.LatestVersion()

		//if no version available we delete the key as it was never written
		if !latest.IsValid() {
			//normal write checks for invalid, but we want to override invalid too
			self.db.Update(func(tx *bolt.Tx) error {

				bucket := tx.Bucket(self.dbkey)
				for _, bkey := range self.setkey {
					bucket = bucket.Bucket(bkey)
				}
				return bucket.DeleteBucket(val.key)
			})
			continue
		}

		//if we have a real version we need the data to return to!
		var data interface{}
		err := val.readVersion(latest, &data)

		//if the version is invalid we don't do anything
		if err != nil {
			return
		}

		//normal write checks for invalid, but we want to override invalid too
		self.db.Update(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(self.dbkey)
			for _, bkey := range append(self.setkey, val.key) {
				bucket = bucket.Bucket(bkey)
			}
			input, _ := getBytes(data)
			return bucket.Put(itob(HEAD), input)
		})
	}
}

func (self *ValueVersionedSet) FixStateAsVersion() (VersionID, error) {
	//check if opertion is possible
	cv, err := self.GetCurrentVersion()
	if err != nil {
		return VersionID(INVALID), err
	}
	if !cv.IsHead() {
		return VersionID(INVALID), fmt.Errorf("Unable to create version if HEAD is not checked out")
	}

	//we iterate over all entries and get the sequence number to store as current
	//state
	version := make(map[string]string)
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}
		if bucket == nil {
			return fmt.Errorf("Unable to get set data")
		}

		c := bucket.Cursor()
		for key, val := c.First(); key != nil; key, val = c.Next() {

			//do nothing for versions bucket or any key valueVersioned pair (only buckets are
			// versioned key valueVersioned pairs)
			if !bytes.Equal(key, itob(VERSIONS)) && val == nil {

				subbucket := bucket.Bucket(key)
				if subbucket == nil {
					return fmt.Errorf("Accessing entry in set failed")
				}

				//create a new version inside the subbucket if the valueVersioned changed
				vid := subbucket.Sequence()
				olddata := subbucket.Get(itob(vid))
				data := subbucket.Get(itob(HEAD))

				if (olddata == nil) || !bytes.Equal(data, olddata) {
					vid, err = subbucket.NextSequence()
					if err != nil {
						return err
					}
					subbucket.Put(itob(vid), data)
				}

				//save the old version as the correct entry if it is not invalid.
				version[btos(key)] = itos(vid)
			}
		}
		return nil
	})
	if err != nil {
		return VersionID(INVALID), err
	}

	//write the new version into store
	var currentVersion uint64
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, itob(VERSIONS)) {
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

	if err != nil {
		return VersionID(INVALID), err
	}

	return VersionID(currentVersion), err
}

func (self *ValueVersionedSet) getVersionInfo(id VersionID) (map[string]interface{}, error) {

	version := make(map[string]interface{})
	err := self.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(self.dbkey)
		for _, sk := range append(self.setkey, itob(VERSIONS)) {
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

func (self *ValueVersionedSet) LoadVersion(id VersionID) error {

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

	//make sure all subentries have loaded the correct subversion
	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}

		//it could happen that the loaded version does not include some entries.
		//to catch them we need to go thorugh all entires that are versionized
		c := bucket.Cursor()
		for key, val := c.First(); key != nil; key, val = c.Next() {

			if !bytes.Equal(key, itob(VERSIONS)) && val == nil {

				data := base58.Encode(key)
				subbucket := bucket.Bucket(key)

				var err error
				if id.IsHead() {
					err = subbucket.Put(itob(CURRENT), itob(HEAD))

				} else {

					vers, ok := version[data]

					if !ok {
						err = subbucket.Put(itob(CURRENT), itob(INVALID))
					} else {
						verstr, ok := vers.(string)
						if !ok {
							return fmt.Errorf("Stored version information is not a VersionID")
						}
						err = subbucket.Put(itob(CURRENT), stob(verstr))
					}
				}
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	//we write the current version
	self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}
		bucket.Put(itob(CURRENT), itob(uint64(id)))
		return nil
	})

	return err
}

func (self *ValueVersionedSet) GetLatestVersion() (VersionID, error) {

	//read the last version we created
	var version uint64 = 0
	found := false
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, itob(VERSIONS)) {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			found = true
			if val > version {
				version = val
			}
			return nil
		})
		return nil
	})

	if !found {
		return VersionID(INVALID), fmt.Errorf("No versions availble")
	}

	return VersionID(version), nil
}

func (self *ValueVersionedSet) GetCurrentVersion() (VersionID, error) {

	//read the last version we created
	var currentVersion uint64
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}
		versdata := bucket.Get(itob(CURRENT))
		if versdata == nil {
			return fmt.Errorf("Could not access sets version information")
		}
		currentVersion = btoi(versdata)
		return nil
	})
	return VersionID(currentVersion), err
}

func (self *ValueVersionedSet) RemoveVersionsUpTo(ID VersionID) error {

	cv, _ := self.GetCurrentVersion()
	if cv < ID {
		return fmt.Errorf("Cannot delete curretnly loaded version")
	}

	//get the version info
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	//remove everything that is not needed anymore
	deleted_keys := make([]string, 0)
	for key, valueVersioned := range version {

		valueVersionedstr, ok := valueVersioned.(string)
		if !ok {
			return fmt.Errorf("Stored version information is not a VersionID")
		}
		valueVersioneddata := stoi(valueVersionedstr)
		keydata := stob(key)

		//check what is the values latest version
		value := ValueVersioned{self.db, self.dbkey, self.setkey, keydata}
		latest := value.LatestVersion()

		//delete what is not needed anymore: the whole bucket or subentries
		self.db.Update(func(tx *bolt.Tx) error {
			setBucket := tx.Bucket(self.dbkey)
			for _, sk := range self.setkey {
				setBucket = setBucket.Bucket(sk)
			}
			keyBucket := setBucket.Bucket(keydata)

			versionData := keyBucket.Get(itob(valueVersioneddata))
			if isInvalid(versionData) && (latest == VersionID(valueVersioneddata)) {
				//if the latest version for this bucket points to INVALID_DATA we know it
				//was removed, hence can be fully deleted. (it could be that is was written
				//again after setting it invalid and make it hence valid again in later versions)
				setBucket.DeleteBucket(keydata)
				deleted_keys = append(deleted_keys, key)

			} else {
				//delete each entry that belongs to older versions. We use the fact
				//that versions have always increasing numbers
				todelete := make([][]byte, 0)
				keyBucket.ForEach(func(k, v []byte) error {
					val := btoi(k)
					if val < valueVersioneddata {
						//deep copy of key, as the slice is invalid outside foreach
						keycopy := make([]byte, len(k))
						copy(keycopy, k)
						todelete = append(todelete, keycopy)
					}
					return nil
				})
				for _, k := range todelete {
					keyBucket.Delete(k)
				}
			}
			return nil
		})
	}

	//with all data cleared we can delete the version entries
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range append(self.setkey, itob(VERSIONS)) {
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

	//rewrite all existing versions to not include deleted keys
	latest, _ := self.GetLatestVersion()
	for i := uint64(ID); i <= uint64(latest); i++ {
		version, err := self.getVersionInfo(VersionID(i))
		if err != nil {
			return utils.StackError(err, "Unable to change newer versions")
		}

		//remove all deleted keys from version map
		for _, key := range deleted_keys {
			delete(version, key)
		}

		//write version map back
		err = self.db.Update(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(self.dbkey)
			for _, bkey := range append(self.setkey, itob(VERSIONS)) {
				bucket = bucket.Bucket(bkey)
			}
			data, err := getBytes(version)
			if err != nil {
				return err
			}
			bucket.Put(itob(i), data)
			return nil
		})
		if err != nil {
			return utils.StackError(err, "unable to write updated version informtion")
		}
	}

	return err
}

func (self *ValueVersionedSet) RemoveVersionsUpFrom(ID VersionID) error {

	cv, _ := self.GetCurrentVersion()
	if cv > ID {
		return fmt.Errorf("Cannot delete curretnly loaded version")
	}

	//get the version info
	version, err := self.getVersionInfo(ID)
	if err != nil {
		return err
	}

	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}

		//we want to search all entries
		c := bucket.Cursor()
		for key, val := c.First(); key != nil; key, val = c.Next() {

			if !bytes.Equal(key, itob(VERSIONS)) && val == nil {

				ifval, ok := version[btos(key)]
				if !ok {
					//if this bucket is not in the version map it belongs to a newer
					//version, hence can be deleted
					bucket.DeleteBucket(key)
				} else {

					//we are already available in the given version. But we can delete
					//all bucket versions that are too new
					strval, ok := ifval.(string)
					if !ok {
						return fmt.Errorf("Stored data is not version ID")
					}
					idval := stoi(strval)
					subbucket := bucket.Bucket(key)
					todelete := make([][]byte, 0)
					err = subbucket.ForEach(func(k, v []byte) error {
						val := btoi(k)
						if val != CURRENT && val != HEAD && val > idval {
							//deep copy of key, as the slice is invalid outside foreach
							keycopy := make([]byte, len(k))
							copy(keycopy, k)
							todelete = append(todelete, keycopy)
						}
						return nil
					})
					if err != nil {
						return err
					}
					for _, k := range todelete {
						subbucket.Delete(k)
					}
					//make sure the sequence is always set to the highest version
					subbucket.SetSequence(idval)
				}
			}
		}
		return nil
	})

	//with all data cleared we can delete the version entries
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range append(self.setkey, itob(VERSIONS)) {
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

func (self *ValueVersionedSet) getKeys() ([][]byte, error) {

	keys := make([][]byte, 0)
	values := self.collectValueVersioneds()
	for _, value := range values {
		if value.IsValid() {
			keys = append(keys, value.key)
		}
	}

	return keys, nil
}

/*
 * Key-ValueVersioned functions
 * ********************************************************************************
 */
func (self ValueVersionedSet) GetType() StorageType {
	return ValueType
}

func (self *ValueVersionedSet) HasKey(key []byte) bool {

	pair := ValueVersioned{self.db, self.dbkey, self.setkey, key}
	return pair.IsValid()
}

func (self *ValueVersionedSet) GetOrCreateValue(key []byte) (*ValueVersioned, error) {

	if !self.HasKey(key) {

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
			for _, sk := range self.setkey {
				bucket = bucket.Bucket(sk)
			}
			bucket, err = bucket.CreateBucketIfNotExists(key)
			if err != nil {
				return err
			}

			//setup the basic structure
			err = bucket.Put(itob(CURRENT), itob(HEAD))
			if err != nil {
				return err
			}
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	return &ValueVersioned{self.db, self.dbkey, self.setkey, key}, nil
}

func (self *ValueVersionedSet) removeKey(key []byte) error {

	if !self.HasKey(key) {
		return fmt.Errorf("key does not exists, cannot be removed")
	}
	pair, err := self.GetOrCreateValue(key)
	if err != nil {
		return err
	}
	err = pair.remove()
	if err != nil {
		return err
	}
	return nil
}

func (self *ValueVersionedSet) getSetKey() []byte {
	return self.setkey[len(self.setkey)-1]
}

/*
 * ValueVersioned functions
 * ********************************************************************************
 */
type ValueVersioned struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
	key    []byte
}

func (self *ValueVersioned) Write(valueVersioned interface{}) error {

	//check if we are allowed to write: are we in HEAD?
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		val := bucket.Get(itob(CURRENT))
		if btoi(val) != HEAD {
			return fmt.Errorf("Can only write data when in HEAD")
		}
		return nil
	})
	if err != nil {
		return err
	}

	bts, err := getBytes(valueVersioned)
	if err != nil {
		return err
	}

	return self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Put(itob(HEAD), bts)
	})
}

//True if:
// - setup correctly and writeable/readable
// - is available in currently load version
//Note: True does not mean that data was written and reading makes sense
func (self *ValueVersioned) IsValid() bool {

	if self.db == nil {
		return false
	}

	var result bool = true
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		if bucket == nil {
			result = false
			return nil
		}
		cur := bucket.Get(itob(CURRENT))
		if cur == nil || btoi(cur) == INVALID {
			result = false
			return nil
		}
		cur = bucket.Get(cur)
		result = !isInvalid(cur)
		return nil
	})

	return result
}

//return true if the value was already written, false otherwise
func (self *ValueVersioned) HoldsValue() (bool, error) {

	var hasValue bool
	id := self.CurrentVersion()
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(itob(uint64(id)))
		if isInvalid(data) {
			return fmt.Errorf("ValueVersioned does not exist in currently loaded version")
		}
		hasValue = (data != nil)
		return nil
	})

	if err != nil {
		return false, utils.StackError(err, "Cannot check if value holds data or not")
	}
	return hasValue, nil
}

func (self *ValueVersioned) Read() (interface{}, error) {

	var result interface{}
	err := self.readVersion(self.CurrentVersion(), &result)
	if err != nil {
		return nil, utils.StackError(err, "Unable to read stored value")
	}
	return convertInterface(result), nil
}

func (self *ValueVersioned) ReadType(result interface{}) error {

	return self.readVersion(self.CurrentVersion(), result)
}

func (self *ValueVersioned) readVersion(ID VersionID, result interface{}) error {

	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		data := bucket.Get(itob(uint64(ID)))
		if isInvalid(data) {
			return fmt.Errorf("Key ValueVersioned pair does not exist in currently loaded version")
		}
		if data == nil {
			return fmt.Errorf("ValueVersioned was not set before read")
		}
		byteResult, isByte := result.(*[]byte)
		if isByte {
			*byteResult = make([]byte, len(data))
			copy(*byteResult, data)
			return nil
		}

		return json.Unmarshal(data, result)
	})

	if err != nil {
		return err
	}

	return nil
}

func (self *ValueVersioned) remove() error {

	//check if we are allowed to remove: are we in HEAD?
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		val := bucket.Get(itob(CURRENT))
		if btoi(val) != HEAD {
			return fmt.Errorf("Can only remove data when in HEAD")
		}
		return nil
	})
	if err != nil {
		return err
	}

	//removing does not mean to delete everything. We need the data for loading older
	//versions. It just means we set it as "not existing".
	err = self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Put(itob(HEAD), INVALID_VALUE)
	})
	return err
}

func (self *ValueVersioned) CurrentVersion() VersionID {

	var version uint64
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		version = btoi(bucket.Get(itob(CURRENT)))
		return nil
	})

	return VersionID(version)
}

func (self *ValueVersioned) LatestVersion() VersionID {

	var version uint64 = 0
	found := false
	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		//look at each entry and get the largest version
		bucket.ForEach(func(k, v []byte) error {
			val := btoi(k)
			if val != HEAD && val != CURRENT {
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
		return VersionID(INVALID)
	}

	return VersionID(version)
}

func (self *ValueVersioned) HasUpdates() bool {

	updates := false
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}

		//if there is no version available yet we definitly have updates
		if bucket.Sequence() == 0 {
			updates = true
			return nil
		}

		cur := bucket.Get(itob(HEAD))
		old := bucket.Get(itob(bucket.Sequence()))

		updates = !bytes.Equal(cur, old)

		return nil
	})
	if err != nil {
		panic(err.Error())
	}

	return updates
}

//helper functions
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func btos(val []byte) string {
	return base58.Encode(val)
}

func stob(val string) []byte {
	data, err := base58.Decode(val)
	if err != nil {
		panic("Stored version information could not be decoded to VersionID")
	}
	return data
}

func itos(val uint64) string {
	return btos(itob(val))
}

func stoi(val string) uint64 {
	return btoi(stob(val))
}
