//parser for the datastructure markup language
package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestKeyValueData(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with key value database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db := store.GetDatabase(KeyValue)
		So(db, ShouldNotBeNil)
		_, ok := db.(*KeyValueDatabase)
		So(ok, ShouldBeTrue)

		Convey("sets can be creaded and deleted,", func() {

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

		Convey("to which data can be written and restored.", func() {

			name := makeSetFromString("test")
			set := db.GetOrCreateSet(name).(*KeyValueSet)

			key1 := []byte("key1")
			So(set.HasKey(key1), ShouldBeFalse)
			pair, err := set.GetOrCreateKey(key1)
			So(err, ShouldBeNil)
			So(pair, ShouldNotBeNil)
			So(set.HasKey(key1), ShouldBeTrue)

			var data int64 = 1
			err = pair.Write(data)
			So(err, ShouldBeNil)
			res, err := pair.Read()
			So(err, ShouldBeNil)
			num, ok := res.(int64)
			So(ok, ShouldBeTrue)
			So(num, ShouldEqual, 1)

			key2 := []byte("key2")
			pair2, err := set.GetOrCreateKey(key2)
			So(err, ShouldBeNil)
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

func TestKeyValueVersioning(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	fmt.Println(path)
	//defer os.RemoveAll(path)

	Convey("Setting up a KeyValue database with a single set,", t, func() {

		store, _ := NewDatastore(path)
		defer store.Close()

		db := store.GetDatabase(KeyValue)
		set := db.GetOrCreateSet(makeSetFromString("test"))

		Convey("initially all versioning commands must be callable.", func() {

			version, err := set.GetLatestVersion()
			So(err, ShouldBeNil)
			So(version.IsValid(), ShouldBeFalse)
			So(version.IsHead(), ShouldBeFalse)

			version, err = set.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(version.IsHead(), ShouldBeTrue)
			So(version.IsValid(), ShouldBeTrue)

			version, err = set.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 1)

			version, err = set.GetLatestVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 1)

			version, err = set.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(version.IsHead(), ShouldBeTrue)

		})

		Convey("Adding data to the set should not have version information", func() {

			kvset, _ := set.(*KeyValueSet)
			pair1, err := kvset.GetOrCreateKey([]byte("data1"))
			So(err, ShouldBeNil)

			So(pair1.IsValid(), ShouldBeTrue)
			So(pair1.CurrentVersion().IsHead(), ShouldBeTrue)
			So(pair1.LatestVersion().IsValid(), ShouldBeFalse)

			pair1.Write("hello guys")
			So(pair1.IsValid(), ShouldBeTrue)
			So(pair1.CurrentVersion().IsHead(), ShouldBeTrue)
			So(pair1.LatestVersion().IsValid(), ShouldBeFalse)

			pair1.Write("override")
			So(pair1.IsValid(), ShouldBeTrue)
			So(pair1.CurrentVersion().IsHead(), ShouldBeTrue)
			So(pair1.LatestVersion().IsValid(), ShouldBeFalse)
		})

		Convey("and the old version should be reloadable.", func() {

			//fix current state as a new version
			version, err := set.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(version, ShouldEqual, 2)
			latest, _ := set.GetLatestVersion()
			So(latest, ShouldEqual, 2)

			//Version 0 should not exist
			err = set.LoadVersion(VersionID(0))
			So(err, ShouldNotBeNil)

			//if we load the old version directly, the data pair should be invalid
			err = set.LoadVersion(VersionID(1))
			So(err, ShouldBeNil)
			kvset, _ := set.(*KeyValueSet)
			pair1, err := kvset.GetOrCreateKey([]byte("data1"))
			So(err, ShouldBeNil)
			So(pair1.IsValid(), ShouldBeFalse)
			_, err = pair1.Read()
			So(err, ShouldNotBeNil)
			err = pair1.Write("some data")
			So(err, ShouldNotBeNil)

			//new data should not be creatable
			pair2, err := kvset.GetOrCreateKey([]byte("data2"))
			So(err, ShouldNotBeNil)
			So(pair2, ShouldBeNil)

			//and we should not be able to create a new version
			_, err = kvset.FixStateAsVersion()
			So(err, ShouldNotBeNil)

			//we should be able to get back the first values
			err = set.LoadVersion(VersionID(2))
			So(err, ShouldBeNil)
			So(pair1.IsValid(), ShouldBeTrue)
			data, err := pair1.Read()
			So(err, ShouldBeNil)
			value, ok := data.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "override")

			//new data should not be creatable
			pair2, err = kvset.GetOrCreateKey([]byte("data2"))
			So(err, ShouldNotBeNil)

			//new data should be creatable in HEAD
			err = set.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
			So(pair1.IsValid(), ShouldBeTrue)
			data, err = pair1.Read()
			So(err, ShouldBeNil)
			value, ok = data.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "override")

			pair2, err = kvset.GetOrCreateKey([]byte("data2"))
			So(err, ShouldBeNil)
			err = pair2.Write(12)
			So(err, ShouldBeNil)

		})

		Convey("It must be possible to delete older versions", func() {

			kvset, _ := set.(*KeyValueSet)
			pair1, _ := kvset.GetOrCreateKey([]byte("data1"))
			pair2, _ := kvset.GetOrCreateKey([]byte("data2"))
			pair3, _ := kvset.GetOrCreateKey([]byte("data3"))

			So(pair1.Write("next"), ShouldBeNil)
			So(pair2.Write(29), ShouldBeNil)
			So(pair3.Write(1.32), ShouldBeNil)

			version, err := set.FixStateAsVersion()
			So(err, ShouldBeNil)

			So(pair1.Write("again"), ShouldBeNil)
			So(pair2.Write(-5), ShouldBeNil)
			So(pair3.Remove(), ShouldBeTrue)
			set.FixStateAsVersion()

			So(set.RemoveVersionsUpTo(version), ShouldBeNil)
			So(pair1.IsValid(), ShouldBeTrue)
			So(pair2.IsValid(), ShouldBeTrue)
			So(pair3.IsValid(), ShouldBeFalse)

			val, _ := pair1.Read()
			So(val.(string), ShouldEqual, "again")
			val, _ = pair2.Read()
			So(val.(int64), ShouldEqual, -5)
			val, err = pair3.Read()
			So(err, ShouldNotBeNil)

			So(set.LoadVersion(1), ShouldNotBeNil)
			So(set.LoadVersion(version), ShouldBeNil)
			val, _ = pair1.Read()
			So(val.(string), ShouldEqual, "next")
			val, _ = pair2.Read()
			So(val.(int64), ShouldEqual, 29)
			So(pair3.IsValid(), ShouldBeTrue)
			val, _ = pair3.Read()
			So(val.(float64), ShouldAlmostEqual, 1.32)

			So(set.LoadVersion(VersionID(HEAD)), ShouldBeNil)
			val, _ = pair1.Read()
			So(val.(string), ShouldEqual, "again")
			val, _ = pair2.Read()
			So(val.(int64), ShouldEqual, -5)
			So(pair3.IsValid(), ShouldBeFalse)
			_, err = pair3.Read()
			So(err, ShouldNotBeNil)

		})

		Convey("as well as new versions", func() {

		})

	})
}
