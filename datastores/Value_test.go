package datastore

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestValueBasic(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with key value database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		db, err := store.GetDatabase(ValueType, false)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*ValueDatabase)
		So(ok, ShouldBeTrue)

		Convey("sets can be creaded and deleted,", func() {

			name := makeSetFromString("test")
			has, err := db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)

			//test creation of set
			set, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			So(set, ShouldNotBeNil)
			has, err = db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			err = db.RemoveSet(name)
			So(err, ShouldBeNil)
			has, err = db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
		})

		Convey("to which data can be written and read.", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueSet)

			key1 := []byte("key1")
			So(set.HasKey(key1), ShouldBeFalse)
			value, err := set.GetOrCreateValue(key1)
			So(err, ShouldBeNil)
			So(value, ShouldNotBeNil)

			So(set.HasKey(key1), ShouldBeTrue)
			ok, err := value.Exists()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			ok, err = value.WasWrittenOnce()
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			So(value.IsValid(), ShouldBeFalse)

			var data int64 = 1
			err = value.Write(data)
			So(err, ShouldBeNil)
			ok, err = value.Exists()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			ok, err = value.WasWrittenOnce()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			So(value.IsValid(), ShouldBeTrue)

			res, err := value.Read()
			So(err, ShouldBeNil)
			num, ok := res.(int64)
			So(ok, ShouldBeTrue)
			So(num, ShouldEqual, 1)

			key2 := []byte("key2")
			value2, err := set.GetOrCreateValue(key2)
			So(err, ShouldBeNil)
			So(value2, ShouldNotBeNil)
			So(set.HasKey(key2), ShouldBeTrue)

			var sdata string = "test string"
			err = value2.Write(sdata)
			So(err, ShouldBeNil)
			res2, err := value2.Read()
			So(err, ShouldBeNil)
			str, ok := res2.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, sdata)

			err = value2.Write("annother test")
			So(err, ShouldBeNil)
			res3, err := value2.Read()
			So(err, ShouldBeNil)
			str, ok = res3.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, "annother test")
		})

		Convey("Getting the raw data should be supported", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueSet)
			value1, _ := set.GetOrCreateValue([]byte("rawtest1"))

			data := string("This is raw data test")
			value1.Write(data)

			binary, _ := value1.Read()
			So(binary.(string), ShouldEqual, data)

			value2, _ := set.GetOrCreateValue([]byte("rawtest2"))
			value2.Write([]byte(data))
			result, _ := value2.Read()

			So(bytes.Equal(result.([]byte), []byte(data)), ShouldBeTrue)
		})

		Convey("Complex structs are usable", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueSet)
			value, _ := set.GetOrCreateValue([]byte("structtest"))

			type myStruct struct {
				First  interface{}
				Second interface{}
			}
			gob.Register(new(myStruct))

			val := myStruct{1, 2}
			So(value.Write(val), ShouldBeNil)

			val2, err := value.Read()
			So(err, ShouldBeNil)
			So(val2, ShouldResemble, &val)
		})

		Convey("And data can be removed", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueSet)

			key2 := []byte("key2")
			value2, err := set.GetOrCreateValue(key2)

			So(value2.IsValid(), ShouldBeTrue)
			ok, _ := value2.Exists()
			So(ok, ShouldBeTrue)

			value2.remove()

			So(value2.IsValid(), ShouldBeFalse)
			ok, _ = value2.Exists()
			So(ok, ShouldBeFalse)
			So(set.HasKey(key2), ShouldBeFalse)
		})
	})
}

func TestValueVersionedBasics(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with key value database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		db, err := store.GetDatabase(ValueType, true)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*ValueVersionedDatabase)
		So(ok, ShouldBeTrue)

		Convey("sets can be creaded and deleted,", func() {

			name := makeSetFromString("test")
			has, err := db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)

			//test creation of set
			set, _ := db.GetOrCreateSet(name)
			So(set, ShouldNotBeNil)
			has, err = db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			err = db.RemoveSet(name)
			So(err, ShouldBeNil)
			has, err = db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
		})

		Convey("to which data can be written and restored.", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueVersionedSet)

			key1 := []byte("key1")
			So(set.HasKey(key1), ShouldBeFalse)
			pair, err := set.GetOrCreateValue(key1)
			So(err, ShouldBeNil)
			So(pair, ShouldNotBeNil)
			So(set.HasKey(key1), ShouldBeTrue)
			ok, err := pair.Exists()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			ok, err = pair.WasWrittenOnce()
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			So(pair.IsValid(), ShouldBeFalse)

			var data int64 = 1
			err = pair.Write(data)
			So(err, ShouldBeNil)
			ok, err = pair.WasWrittenOnce()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			So(pair.IsValid(), ShouldBeTrue)

			res, err := pair.Read()
			So(err, ShouldBeNil)
			num, ok := res.(int64)
			So(ok, ShouldBeTrue)
			So(num, ShouldEqual, 1)

			key2 := []byte("key2")
			pair2, err := set.GetOrCreateValue(key2)
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

		Convey("Getting the raw data should be supported", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueVersionedSet)
			value1, _ := set.GetOrCreateValue([]byte("rawtest1"))

			data := string("This is raw data test")
			value1.Write(data)

			binary, _ := value1.Read()
			So(binary, ShouldEqual, data)

			value2, _ := set.GetOrCreateValue([]byte("rawtest2"))
			value2.Write([]byte(data))
			result, _ := value2.Read()

			So(bytes.Equal(result.([]byte), []byte(data)), ShouldBeTrue)
		})

		Convey("And values can be removed", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set := genset.(*ValueVersionedSet)

			value, _ := set.GetOrCreateValue([]byte("key2"))

			So(value.IsValid(), ShouldBeTrue)
			ok, _ := value.Exists()
			So(ok, ShouldBeTrue)

			value.remove()

			So(value.IsValid(), ShouldBeFalse)
			ok, _ = value.Exists()
			So(ok, ShouldBeFalse)
		})
	})
}

func TestValueVersioned(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Setting up a ValueVersioned database with a single set,", t, func() {

		store, _ := NewDatastore(path)
		defer store.Close()
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		db, err := store.GetDatabase(ValueType, true)
		So(err, ShouldBeNil)
		genset, err := db.GetOrCreateSet(makeSetFromString("test"))
		So(err, ShouldBeNil)
		set := genset.(VersionedSet)

		Convey("initially all versioning commands must be callable.", func() {

			hasv, _ := set.HasVersions()
			So(hasv, ShouldBeFalse)
			version, err := set.GetLatestVersion()
			So(err, ShouldNotBeNil)
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
			hasv, _ = set.HasVersions()
			So(hasv, ShouldBeTrue)

			version, err = set.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(version.IsHead(), ShouldBeTrue)

		})

		Convey("Adding data to the set should not have version information", func() {

			kvset, _ := set.(*ValueVersionedSet)

			has, _ := kvset.HasUpdates()
			So(has, ShouldBeFalse)
			pair1, err := kvset.GetOrCreateValue([]byte("data1"))

			has, _ = kvset.HasUpdates()
			So(has, ShouldBeTrue)
			So(err, ShouldBeNil)

			So(pair1.IsValid(), ShouldBeFalse)
			cur, err := pair1.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(cur.IsHead(), ShouldBeTrue)
			lat, err := pair1.GetLatestVersion()
			So(err, ShouldBeNil)
			So(lat.IsValid(), ShouldBeFalse)
			upd, err := pair1.HasUpdates()
			So(err, ShouldBeNil)
			So(upd, ShouldBeTrue)
			data, err := pair1.Read()
			So(err, ShouldNotBeNil)
			So(data, ShouldBeNil)

			pair1.Write("hello guys")
			So(pair1.IsValid(), ShouldBeTrue)
			upd, err = pair1.HasUpdates()
			So(err, ShouldBeNil)
			So(upd, ShouldBeTrue)
			cur, err = pair1.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(cur.IsHead(), ShouldBeTrue)
			lat, err = pair1.GetLatestVersion()
			So(err, ShouldBeNil)
			So(lat.IsValid(), ShouldBeFalse)

			pair1.Write("override")
			So(pair1.IsValid(), ShouldBeTrue)
			cur, err = pair1.GetCurrentVersion()
			So(err, ShouldBeNil)
			So(cur.IsHead(), ShouldBeTrue)
			lat, err = pair1.GetLatestVersion()
			So(err, ShouldBeNil)
			So(lat.IsValid(), ShouldBeFalse)
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

			//we cannot load data1, as it does not exist in the checked out version
			//and hence would be created new, which is forbidden
			err = set.LoadVersion(VersionID(1))
			So(err, ShouldBeNil)
			kvset, _ := set.(*ValueVersionedSet)
			pair1, err := kvset.GetOrCreateValue([]byte("data1"))
			So(err, ShouldNotBeNil)

			//new data should not be creatable
			pair2, err := kvset.GetOrCreateValue([]byte("data2"))
			So(err, ShouldNotBeNil)
			So(pair2, ShouldBeNil)

			//and we should not be able to create a new version
			_, err = kvset.FixStateAsVersion()
			So(err, ShouldNotBeNil)

			//we should be able to get back the first values
			err = set.LoadVersion(VersionID(2))
			So(err, ShouldBeNil)
			pair1, err = kvset.GetOrCreateValue([]byte("data1"))
			So(err, ShouldBeNil)
			So(pair1.IsValid(), ShouldBeTrue)
			ok, err := pair1.Exists()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			data, err := pair1.Read()
			So(err, ShouldBeNil)
			value, ok := data.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "override")

			//new data should not be creatable
			pair2, err = kvset.GetOrCreateValue([]byte("data2"))
			So(err, ShouldNotBeNil)

			//new data should be creatable in HEAD
			err = set.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
			So(pair1.IsValid(), ShouldBeTrue)
			ok, err = pair1.Exists()
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			data, err = pair1.Read()
			So(err, ShouldBeNil)
			value, ok = data.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "override")

			pair2, err = kvset.GetOrCreateValue([]byte("data2"))
			So(err, ShouldBeNil)
			err = pair2.Write(12)
			So(err, ShouldBeNil)

		})

		Convey("It must be possible to delete older versions", func() {

			kvset, _ := set.(*ValueVersionedSet)
			pair1, _ := kvset.GetOrCreateValue([]byte("data1"))
			pair2, _ := kvset.GetOrCreateValue([]byte("data2"))
			pair3, _ := kvset.GetOrCreateValue([]byte("data3"))

			So(pair1.Write("next"), ShouldBeNil)
			So(pair2.Write(29), ShouldBeNil)
			So(pair3.Write(1.32), ShouldBeNil)

			version, err := set.FixStateAsVersion()
			So(err, ShouldBeNil)

			So(pair1.Write("again"), ShouldBeNil)
			So(pair2.Write(-5), ShouldBeNil)
			So(pair3.remove(), ShouldBeNil)
			set.FixStateAsVersion()

			So(set.RemoveVersionsUpTo(version), ShouldBeNil)
			So(pair1.IsValid(), ShouldBeTrue)
			So(pair2.IsValid(), ShouldBeTrue)
			So(pair3.IsValid(), ShouldBeFalse)

			val, err := pair1.Read()
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "again")
			val, _ = pair2.Read()
			So(val.(int), ShouldEqual, -5)
			val, err = pair3.Read()
			So(err, ShouldNotBeNil)

			So(set.LoadVersion(1), ShouldNotBeNil)
			So(set.LoadVersion(version), ShouldBeNil)
			val, _ = pair1.Read()
			So(val.(string), ShouldEqual, "next")
			val, _ = pair2.Read()
			So(val.(int), ShouldEqual, 29)
			So(pair3.IsValid(), ShouldBeTrue)
			val, _ = pair3.Read()
			So(val.(float64), ShouldAlmostEqual, 1.32)

			So(set.LoadVersion(VersionID(HEAD)), ShouldBeNil)
			val, _ = pair1.Read()
			So(val.(string), ShouldEqual, "again")
			val, _ = pair2.Read()
			So(val.(int), ShouldEqual, -5)
			So(pair3.IsValid(), ShouldBeFalse)
			_, err = pair3.Read()
			So(err, ShouldNotBeNil)

		})

		Convey("as well as new versions", func() {

			kvset, _ := set.(*ValueVersionedSet)
			pair1, _ := kvset.GetOrCreateValue([]byte("data1"))
			pair2, _ := kvset.GetOrCreateValue([]byte("data2"))
			pair4, _ := kvset.GetOrCreateValue([]byte("data4"))

			So(pair1.Write("hmm"), ShouldBeNil)
			So(pair2.Write(1070), ShouldBeNil)
			So(pair4.Write(-6.5), ShouldBeNil)

			v, _ := set.FixStateAsVersion()
			So(uint64(v), ShouldEqual, 5)
			So(set.RemoveVersionsUpFrom(VersionID(4)), ShouldNotBeNil)
			set.LoadVersion(VersionID(4))
			So(set.RemoveVersionsUpFrom(VersionID(4)), ShouldBeNil)
		})

		Convey("Creating a new set for reset testing", func() {

			genset, _ := db.GetOrCreateSet(makeSetFromString("testreset"))
			set := genset.(VersionedSet)
			kvset, _ := set.(*ValueVersionedSet)

			Convey("Must be resettable directly even if there is no version", func() {

				value, _ := kvset.GetOrCreateValue([]byte("key1"))
				value.Write("test")
				upd, err := value.HasUpdates()
				So(err, ShouldBeNil)
				So(upd, ShouldBeTrue)

				holds, err := value.WasWrittenOnce()
				So(err, ShouldBeNil)
				So(holds, ShouldBeTrue)
				So(value.IsValid(), ShouldBeTrue)
				lat, err := value.GetLatestVersion()
				So(err, ShouldBeNil)
				So(lat.IsValid(), ShouldBeFalse)

				has, _ := kvset.HasUpdates()
				So(has, ShouldBeTrue)

				err = kvset.ResetHead()
				So(err, ShouldBeNil)
				So(kvset.HasKey([]byte("key1")), ShouldBeFalse)
			})
		})

		Convey("Creating a new set for version remove testing", func() {

			set, _ := db.GetOrCreateSet(makeSetFromString("test2"))
			vset := set.(*ValueVersionedSet)
			if has, _ := vset.HasUpdates(); has {
				vset.FixStateAsVersion()
			}

			Convey("Simple adding, fixing and remove should work", func() {

				value, err := vset.GetOrCreateValue([]byte("val1"))
				So(err, ShouldBeNil)
				value.Write("test")
				v, _ := vset.FixStateAsVersion()
				vset.RemoveVersionsUpTo(v)
				keys, _ := vset.getKeys()
				So(len(keys), ShouldEqual, 1)
			})

			Convey("Deleting an entry should work", func() {

				value, err := vset.GetOrCreateValue([]byte("val1"))
				So(err, ShouldBeNil)
				value.remove()
				v, _ := vset.FixStateAsVersion()
				vset.RemoveVersionsUpTo(v)
				keys, _ := vset.getKeys()
				So(len(keys), ShouldEqual, 0)
			})

		})
	})
}
