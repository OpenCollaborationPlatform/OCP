//parser for the datastructure markup language
package datastore

import (
	"crypto/sha256"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func makeEntryFromString(name string) [32]byte {

	data, err := json.Marshal(name)
	if err != nil {
		var data [32]byte
		return data
	}
	return sha256.Sum256(data)
}

func TestKeyValue(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db := store.GetDatabase(KeyValue)
		Convey("a key-value database must be creatable,", func() {

			So(db, ShouldNotBeNil)
			_, ok := db.(*KeyValueDatabase)
			So(ok, ShouldBeTrue)
		})

		Convey("from which entries and subentries can be creaded and deleted,", func() {

			name := makeEntryFromString("test")
			So(db.HasEntry(name), ShouldBeFalse)

			//test creation of entry
			entry := db.GetOrCreateEntry(name)
			So(entry, ShouldNotBeNil)
			So(db.HasEntry(name), ShouldBeTrue)

			kventry, ok := entry.(KeyValueEntry)
			So(ok, ShouldBeTrue)

			//check creation of subentry
			name2 := makeEntryFromString("test2")
			subentry := kventry.GetOrCreateSubEntry(name2[:])
			So(subentry, ShouldNotBeNil)
			So(kventry.HasSubEntry(name2[:]), ShouldBeTrue)

			//get or create should return the same entry as bevore, with our subentry
			entry2 := db.GetOrCreateEntry(name)
			So(entry2, ShouldNotBeNil)
			kventry2 := entry.(KeyValueEntry)
			So(kventry2.HasSubEntry(name2[:]), ShouldBeTrue)

			//test removing entries and subentries
			err = kventry2.RemoveSubEntry(name2[:])
			So(err, ShouldBeNil)
			So(kventry2.HasSubEntry(name2[:]), ShouldBeFalse)
			So(kventry.HasSubEntry(name2[:]), ShouldBeFalse)

			err := db.RemoveEntry(name)
			So(err, ShouldBeNil)
			So(db.HasEntry(name), ShouldBeFalse)
		})

		Convey("and to which data can be written and restored.", func() {

			name := makeEntryFromString("test")
			entry := db.GetOrCreateEntry(name).(KeyValueEntry)

			key1 := []byte("key1")
			So(entry.HasKey(key1), ShouldBeFalse)
			pair := entry.GetOrCreateKey(key1)
			So(pair, ShouldNotBeNil)
			So(entry.HasKey(key1), ShouldBeTrue)

			var data int64 = 1
			err := pair.Write(data)
			So(err, ShouldBeNil)
			res, err := pair.Read()
			So(err, ShouldBeNil)
			num, ok := res.(int64)
			So(ok, ShouldBeTrue)
			So(num, ShouldEqual, 1)

			name2 := makeEntryFromString("test2")
			subentry := entry.GetOrCreateSubEntry(name2[:])

			key2 := []byte("key2")
			pair2 := subentry.GetOrCreateKey(key2)
			So(pair2, ShouldNotBeNil)
			So(subentry.HasKey(key2), ShouldBeTrue)
			So(entry.HasKey(key2), ShouldBeFalse)

			var sdata string = "test string"
			err = pair2.Write(sdata)
			So(err, ShouldBeNil)
			res2, err := pair2.Read()
			So(err, ShouldBeNil)
			str, ok := res2.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, sdata)
		})
	})
}
