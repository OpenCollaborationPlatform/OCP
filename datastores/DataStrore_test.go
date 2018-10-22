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

		Convey("from which entries can be creaded and deleted,", func() {

			name := makeSetFromString("test")
			So(db.HasSet(name), ShouldBeFalse)

			//test creation of set
			set := db.GetOrCreateSet(name)
			So(set, ShouldNotBeNil)
			So(db.HasSet(name), ShouldBeTrue)

			err := db.RemoveSet(name)
			So(err, ShouldBeNil)
			So(db.HasSet(name), ShouldBeFalse)
		})

		Convey("and to which data can be written and restored.", func() {

			name := makeSetFromString("test")
			set := db.GetOrCreateSet(name).(*KeyValueSet)

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

			key2 := []byte("key2")
			pair2 := set.GetOrCreateKey(key2)
			So(pair2, ShouldNotBeNil)
			So(set.HasKey(key2), ShouldBeTrue)

			var sdata string = "test string"
			err = pair2.Write(sdata)
			So(err, ShouldBeNil)
			res2, err := pair2.Read()
			So(err, ShouldBeNil)
			str, ok := res2.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, sdata)

			err = pair2.Write("annother test")
			So(err, ShouldBeNil)
			res3, err := pair2.Read()
			So(err, ShouldBeNil)
			str, ok = res3.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, "annother test")
		})
	})
}
