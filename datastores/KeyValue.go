// KeyValue database: The key value database saves the multiple entries per set which
// are accessed by keys.
package datastore

/*
Data layout of versioned key value store:

bucket(SetKey) [
	entry(CURRENT) = HEAD
	bucket(VERSIONS) [
		entry(1) = Versionmap(key1->1, key2->1)
		entry(2) = Versionmap(key1->2, key2->1)
	]
	bucket(key1) [
		entry(1) = "first value"
		entry(2) = "second vale"
		entry(CURRENT) = HEAD
		entry(HEAD) = "current value"
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
- HEAD    	gives the currently worked on value. It is set to []byte{} and hence made
		  	invalid on removal
- Bucket sequence: Is alwas on the latest version existing in the bucket
*/

import (
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

var INVALID_VALUE = make([]byte, 0)

func isInvalid(val []byte) bool {

	return (val != nil) && (len(val) == 0)
}

func NewKeyValueDatabase(db *bolt.DB) (*KeyValueDatabase, error) {

	//make sure key value store exists in bolts db:
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("keyvalue"))
		return nil
	})

	return &KeyValueDatabase{db, []byte("keyvalue")}, nil
}

//implements the database interface
type KeyValueDatabase struct {
	db    *bolt.DB
	dbkey []byte
}

func (self KeyValueDatabase) HasSet(set [32]byte) bool {

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

func (self KeyValueDatabase) GetOrCreateSet(set [32]byte) Set {

	if !self.HasSet(set) {
		//make sure the bucket exists
		self.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(self.dbkey)
			newbucket, err := bucket.CreateBucketIfNotExists(set[:])
			if err != nil {
				return err
			}

			//set default values
			newbucket.Put(itob(CURRENT), itob(HEAD))
			newbucket.CreateBucketIfNotExists(itob(VERSIONS))

			return nil
		})
	}

	return &KeyValueSet{self.db, self.dbkey, [][]byte{set[:]}}
}

func (self KeyValueDatabase) RemoveSet(set [32]byte) error {

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

func (self KeyValueDatabase) Close() {

}

//The store itself is very simple, as all the access logic will be in the set type
//this is only to manage the existing entries
type KeyValueSet struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
}

/*
 * Interface functions
 * ********************************************************************************
 */
func (self *KeyValueSet) IsValid() bool {

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

func (self *KeyValueSet) Print() {

	if !self.IsValid() {
		fmt.Println("Invalid set")
		return
	}

	self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, sk := range self.setkey {
			bucket = bucket.Bucket(sk)
		}

		bucket.ForEach(func(k []byte, v []byte) error {

			if bytes.Equal(k, itob(CURRENT)) {
				if btoi(v) == HEAD {
					fmt.Printf("CURRENT: HEAD\n")
				} else {
					fmt.Printf("CURRENT: %v\n", btoi(v))
				}

			} else if bytes.Equal(k, itob(VERSIONS)) {
				fmt.Println("VERSIONS:")
				//print the versions out
				subbucket := bucket.Bucket(k)
				subbucket.ForEach(func(sk []byte, sv []byte) error {
					inter, _ := getInterface(sv)
					data := inter.(map[string]interface{})
					//build the versioning string
					str := ""
					for mk, mv := range data {
						str = str + string(stob(mk)) + ": %v,  "
						mvid := stoi(mv.(string))
						if mvid == INVALID {
							str = fmt.Sprintf(str, "INVALID")
						} else {
							str = fmt.Sprintf(str, mvid)
						}
					}

					fmt.Printf("    %v: %v\n", btoi(sk), str)
					return nil
				})
			} else {
				fmt.Printf("%s:\n", string(k))
				subbucket := bucket.Bucket(k)
				subbucket.ForEach(func(sk []byte, sv []byte) error {
					inter, _ := getInterface(sv)
					key := btoi(sk)
					if key == HEAD {
						fmt.Printf("    HEAD: %v\n", inter)
					} else if key == CURRENT {
						if btoi(sv) == HEAD {
							fmt.Printf("    CURRENT: HEAD\n")
						} else if btoi(sv) == INVALID {
							fmt.Printf("    CURRENT: INVALID\n")
						} else {
							fmt.Printf("    CURRENT: %v\n", btoi(sv))
						}
					} else {
						fmt.Printf("    %v: %v\n", key, inter)
					}
					return nil
				})
			}
			return nil
		})

		return nil
	})
}

func (self *KeyValueSet) FixStateAsVersion() (VersionID, error) {

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

			//do nothing for versions bucket or any key value pair (only buckets are
			// versioned key value pairs)
			if !bytes.Equal(key, itob(VERSIONS)) && val == nil {

				subbucket := bucket.Bucket(key)
				if subbucket == nil {
					return fmt.Errorf("Accessing entry in set failed")
				}

				//create a new version inside the subbucket if the value changed
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

				//save the old version as the correct entry
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

func (self *KeyValueSet) getVersionInfo(id VersionID) (map[string]interface{}, error) {

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

func (self *KeyValueSet) LoadVersion(id VersionID) error {

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

func (self *KeyValueSet) GetLatestVersion() (VersionID, error) {

	//read the last version we created
	vp, err := self.GetOrCreateKey(itob(VERSIONS))
	if err != nil {
		return VersionID(INVALID), err
	}
	return vp.LatestVersion(), nil
}

func (self *KeyValueSet) GetCurrentVersion() (VersionID, error) {

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

func (self *KeyValueSet) RemoveVersionsUpTo(ID VersionID) error {

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
	for key, value := range version {

		valuestr, ok := value.(string)
		if !ok {
			return fmt.Errorf("Stored version information is not a VersionID")
		}
		valuedata := stoi(valuestr)
		keydata := stob(key)

		//delete what is not needed anymore: the whole bucket or subentries
		self.db.Update(func(tx *bolt.Tx) error {
			setBucket := tx.Bucket(self.dbkey)
			for _, sk := range self.setkey {
				setBucket = setBucket.Bucket(sk)
			}
			keyBucket := setBucket.Bucket(keydata)

			versionData := keyBucket.Get(itob(valuedata))
			if isInvalid(versionData) {
				//if the verrsion for this bucket points to INVALID_DATA we know it
				//was removed, hence can be fully deleted.
				setBucket.DeleteBucket(keydata)

			} else {
				//delete each entry that belongs to older versions. We use the fact
				//that versions have always increasing numbers
				todelete := make([][]byte, 0)
				keyBucket.ForEach(func(k, v []byte) error {
					val := btoi(k)
					if val < valuedata {
						todelete = append(todelete, k)
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
				todelete = append(todelete, k)
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

func (self *KeyValueSet) RemoveVersionsUpFrom(ID VersionID) error {

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
							todelete = append(todelete, k)
						}
						return nil
					})
					if err != nil {
						return err
					}
					for _, k := range todelete {
						subbucket.Delete(k)
					}

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
				todelete = append(todelete, k)
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
 * Key-Value functions
 * ********************************************************************************
 */
func (self *KeyValueSet) HasKey(key []byte) bool {

	pair := KeyValuePair{self.db, self.dbkey, self.setkey, key}
	return pair.IsValid()
}

func (self *KeyValueSet) GetOrCreateKey(key []byte) (*KeyValuePair, error) {

	if !self.HasKey(key) {

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

	return &KeyValuePair{self.db, self.dbkey, self.setkey, key}, nil
}

func (self *KeyValueSet) RemoveKey(key []byte) error {

	if !self.HasKey(key) {
		return fmt.Errorf("key does not exists, cannot be removed")
	}
	pair, err := self.GetOrCreateKey(key)
	if err != nil {
		return err
	}
	if !pair.Remove() {
		return fmt.Errorf("Unable to remove key")
	}
	return nil
}

/*
 * Pair functions
 * ********************************************************************************
 */
type KeyValuePair struct {
	db     *bolt.DB
	dbkey  []byte
	setkey [][]byte
	key    []byte
}

func (self *KeyValuePair) Write(value interface{}) error {

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
		val = bucket.Get(itob(HEAD))
		if isInvalid(val) {
			return fmt.Errorf("Key was invalidadet, writing not possbile anymore")
		}
		return nil
	})
	if err != nil {
		return err
	}

	bts, err := getBytes(value)
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
func (self *KeyValuePair) IsValid() bool {

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

func (self *KeyValuePair) Read() (interface{}, error) {

	var result interface{}
	err := self.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		//check if we are valid in the current version
		cur := bucket.Get(itob(CURRENT))
		if btoi(cur) == INVALID {
			return fmt.Errorf("Key Value pair does not exist in currently loaded version")
		}
		data := bucket.Get(cur)
		if isInvalid(data) {
			return fmt.Errorf("Key Value pair does not exist in currently loaded version")
		}
		if data == nil {
			return fmt.Errorf("Value was not set before read")
		}
		res, err := getInterface(data)
		result = res
		return err
	})

	if err != nil {
		return nil, err
	}

	return result, err
}

func (self *KeyValuePair) Remove() bool {

	//removing does not mean to delete everything. We need the data for loading older
	//versions. It just means we set it as "not existing".
	err := self.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(self.dbkey)
		for _, bkey := range append(self.setkey, self.key) {
			bucket = bucket.Bucket(bkey)
		}
		return bucket.Put(itob(HEAD), INVALID_VALUE)
	})
	return err == nil
}

func (self *KeyValuePair) CurrentVersion() VersionID {

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

func (self *KeyValuePair) LatestVersion() VersionID {

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

//helper functions
func getBytes(data interface{}) ([]byte, error) {

	return json.Marshal(data)
}

func getInterface(bts []byte) (interface{}, error) {

	var res interface{}
	err := json.Unmarshal(bts, &res)

	//json does not distuinguish between float and int
	num, ok := res.(float64)
	if ok && num == math.Trunc(num) {
		return int64(num), nil
	}

	return res, err
}

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
