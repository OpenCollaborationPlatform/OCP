//parser for the datastructure markup language
package datastore

import (
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

		Convey("entries can be creaded and deleted,", func() {

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
	defer os.RemoveAll(path)

	Convey("Setting up a KeyValue database with a single set,", t, func() {

		store, _ := NewDatastore(path)
		defer store.Close()

		db := store.GetDatabase(KeyValue)
		set := db.GetOrCreateSet(makeSetFromString("test"))

		Convey("initially all versioning commands must be callable.", func() {

			version, err := set.GetLatestVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 0)

			version, err = set.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 0)

			version, err = set.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 1)

			version, err = set.GetLatestVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 1)

			version, err = set.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(uint64(version), ShouldEqual, 1)

		})

		Convey("Adding data to the set should not have version information", func() {

			kvset, _ := set.(*KeyValueSet)
			pair1, err := kvset.GetOrCreateKey([]byte("data1"))
			So(err, ShouldBeNil)

			So(pair1.IsValid(), ShouldBeTrue)
			So(pair1.CurrentVersion(), ShouldEqual, 0)
			So(pair1.LatestVersion(), ShouldEqual, 0)

			pair1.Write("hello guys")
			So(pair1.IsValid(), ShouldBeTrue)
			So(pair1.CurrentVersion(), ShouldEqual, 0)
			So(pair1.LatestVersion(), ShouldEqual, 0)

			pair1.Write("override")
			So(pair1.IsValid(), ShouldBeTrue)
			So(pair1.CurrentVersion(), ShouldEqual, 0)
			So(pair1.LatestVersion(), ShouldEqual, 0)
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

			//new data should be creatable
			pair2, err = kvset.GetOrCreateKey([]byte("data2"))
			So(err, ShouldBeNil)

			err = pair2.Write(12)
			So(err, ShouldBeNil)
		})

	})
}
