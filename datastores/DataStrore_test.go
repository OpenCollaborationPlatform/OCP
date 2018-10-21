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

func makeSetFromString(name string) [32]byte {

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

			name := makeSetFromString("test")
			So(db.HasSet(name), ShouldBeFalse)

			//test creation of set
			set := db.GetOrCreateSet(name)
			So(set, ShouldNotBeNil)
			So(db.HasSet(name), ShouldBeTrue)

			kvset, ok := set.(KeyValueSet)
			So(ok, ShouldBeTrue)

			//check creation of subset
			name2 := makeSetFromString("test2")
			subset := kvset.GetOrCreateSubSet(name2[:])
			So(subset, ShouldNotBeNil)
			So(kvset.HasSubSet(name2[:]), ShouldBeTrue)

			//get or create should return the same set as bevore, with our subset
			set2 := db.GetOrCreateSet(name)
			So(set2, ShouldNotBeNil)
			kvset2 := set.(KeyValueSet)
			So(kvset2.HasSubSet(name2[:]), ShouldBeTrue)

			//test removing entries and subentries
			err = kvset2.RemoveSubSet(name2[:])
			So(err, ShouldBeNil)
			So(kvset2.HasSubSet(name2[:]), ShouldBeFalse)
			So(kvset.HasSubSet(name2[:]), ShouldBeFalse)

			err := db.RemoveSet(name)
			So(err, ShouldBeNil)
			So(db.HasSet(name), ShouldBeFalse)
		})

		Convey("and to which data can be written and restored.", func() {

			name := makeSetFromString("test")
			set := db.GetOrCreateSet(name).(KeyValueSet)

			key1 := []byte("key1")
			So(set.HasKey(key1), ShouldBeFalse)
			pair := set.GetOrCreateKey(key1)
			So(pair, ShouldNotBeNil)
			So(set.HasKey(key1), ShouldBeTrue)

			var data int64 = 1
			err := pair.Write(data)
			So(err, ShouldBeNil)
			res, err := pair.Read()
			So(err, ShouldBeNil)
			num, ok := res.(int64)
			So(ok, ShouldBeTrue)
			So(num, ShouldEqual, 1)

			name2 := makeSetFromString("test2")
			subset := set.GetOrCreateSubSet(name2[:])

			key2 := []byte("key2")
			pair2 := subset.GetOrCreateKey(key2)
			So(pair2, ShouldNotBeNil)
			So(subset.HasKey(key2), ShouldBeTrue)
			So(set.HasKey(key2), ShouldBeFalse)

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
