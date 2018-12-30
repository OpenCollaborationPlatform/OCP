package datastore

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMap(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with map database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db, err := store.GetDatabase(MapType, false)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*MapDatabase)
		So(ok, ShouldBeTrue)

		Convey("sets can be creaded and deleted,", func() {

			name := makeSetFromString("test")
			So(db.HasSet(name), ShouldBeFalse)

			//test creation of set
			set := db.GetOrCreateSet(name)
			So(set, ShouldNotBeNil)
			So(db.HasSet(name), ShouldBeTrue)

			mset, ok := set.(*MapSet)
			So(ok, ShouldBeTrue)
			So(mset, ShouldNotBeNil)

			err := db.RemoveSet(name)
			So(err, ShouldBeNil)
			So(db.HasSet(name), ShouldBeFalse)
		})

		Convey("and maps can be created from the set.", func() {

			name := makeSetFromString("test")
			mset := db.GetOrCreateSet(name).(*MapSet)

			mapkey := []byte("mapkey")
			So(mset.HasMap(mapkey), ShouldBeFalse)

			mp, err := mset.GetOrCreateMap(mapkey)
			So(err, ShouldBeNil)
			So(mp, ShouldNotBeNil)
			So(mset.HasMap(mapkey), ShouldBeTrue)

		})

		Convey("MapVersioneds can be created and data stored", func() {

			name := makeSetFromString("test")
			mset := db.GetOrCreateSet(name).(*MapSet)

			mp, err := mset.GetOrCreateMap([]byte("mymap"))
			So(err, ShouldBeNil)
			So(mp, ShouldNotBeNil)

			key1 := []byte("key1")
			So(mp.HasKey(key1), ShouldBeFalse)
			So(mp.Write(key1, 12), ShouldBeNil)
			So(mp.HasKey(key1), ShouldBeTrue)
			val, err := mp.Read(key1)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 12)

			key2 := []byte("key2")
			So(mp.HasKey(key2), ShouldBeFalse)
			_, err = mp.Read(key2)
			So(err, ShouldNotBeNil)
			So(mp.Write(key2, "hello"), ShouldBeNil)
			So(mp.HasKey(key2), ShouldBeTrue)
			val, err = mp.Read(key2)
			So(err, ShouldBeNil)
			strvalue, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(strvalue, ShouldEqual, "hello")

			So(mp.Remove(key1), ShouldBeTrue)
			So(mp.HasKey(key1), ShouldBeFalse)
			_, err = mp.Read(key1)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestMapVersionedData(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with versioned map database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db, err := store.GetDatabase(MapType, true)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*MapVersionedDatabase)
		So(ok, ShouldBeTrue)

		Convey("sets can be creaded and deleted,", func() {

			name := makeSetFromString("test")
			So(db.HasSet(name), ShouldBeFalse)

			//test creation of set
			set := db.GetOrCreateSet(name)
			So(set, ShouldNotBeNil)
			So(db.HasSet(name), ShouldBeTrue)

			mset, ok := set.(*MapVersionedSet)
			So(ok, ShouldBeTrue)
			So(mset, ShouldNotBeNil)

			err := db.RemoveSet(name)
			So(err, ShouldBeNil)
			So(db.HasSet(name), ShouldBeFalse)
		})

		Convey("and mapVersioneds can be created from the set.", func() {

			name := makeSetFromString("test")
			mset := db.GetOrCreateSet(name).(*MapVersionedSet)

			mapVersionedkey := []byte("mapVersionedkey")
			So(mset.HasMap(mapVersionedkey), ShouldBeFalse)
			So(mset.HasUpdates(), ShouldBeFalse)

			mp, err := mset.GetOrCreateMap(mapVersionedkey)
			So(err, ShouldBeNil)
			So(mp, ShouldNotBeNil)
			So(mset.HasMap(mapVersionedkey), ShouldBeTrue)

			//new mapVersioned means no version yet means there are updates
			So(mp.HasUpdates(), ShouldBeTrue)
			So(mset.HasUpdates(), ShouldBeTrue)
		})

		Convey("MapVersioneds can be created and data stored", func() {

			name := makeSetFromString("test")
			mset := db.GetOrCreateSet(name).(*MapVersionedSet)

			mp, err := mset.GetOrCreateMap([]byte("mymapVersioned"))
			So(err, ShouldBeNil)
			So(mp, ShouldNotBeNil)

			key1 := []byte("key1")
			So(mp.HasKey(key1), ShouldBeFalse)
			So(mp.Write(key1, 12), ShouldBeNil)
			So(mp.HasKey(key1), ShouldBeTrue)
			val, err := mp.Read(key1)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 12)

			key2 := []byte("key2")
			So(mp.HasKey(key2), ShouldBeFalse)
			_, err = mp.Read(key2)
			So(err, ShouldNotBeNil)
			So(mp.Write(key2, "hello"), ShouldBeNil)
			So(mp.HasKey(key2), ShouldBeTrue)
			val, err = mp.Read(key2)
			So(err, ShouldBeNil)
			strvalue, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(strvalue, ShouldEqual, "hello")

			So(mp.Remove(key1), ShouldBeTrue)
			So(mp.HasKey(key1), ShouldBeFalse)
			_, err = mp.Read(key1)
			So(err, ShouldNotBeNil)
		})

		Convey("and versioning of that mapVersioned data works well", func() {

			name := makeSetFromString("test")
			mset := db.GetOrCreateSet(name).(*MapVersionedSet)
			mp, _ := mset.GetOrCreateMap([]byte("mymapVersioned"))

			So(mset.HasUpdates(), ShouldBeTrue)
			So(mp.HasUpdates(), ShouldBeTrue)
			So(mset.HasVersions(), ShouldBeFalse)
			So(mp.HasVersions(), ShouldBeFalse)

			oldversion, err := mset.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(oldversion, ShouldEqual, 1)
			So(mset.HasUpdates(), ShouldBeFalse)
			So(mp.HasUpdates(), ShouldBeFalse)
			So(mset.HasVersions(), ShouldBeTrue)
			So(mp.HasVersions(), ShouldBeTrue)

			key1 := []byte("key1")
			So(mp.HasKey(key1), ShouldBeFalse)
			key2 := []byte("key2")
			So(mp.HasKey(key2), ShouldBeTrue)
			So(mp.Write(key2, "bye"), ShouldBeNil)

			key3 := []byte("key3")
			mp.Write(key3, 1.34)
			newversion, err := mset.FixStateAsVersion()
			So(err, ShouldBeNil)

			So(mp.HasKey(key3), ShouldBeTrue)
			err = mset.LoadVersion(oldversion)
			So(err, ShouldBeNil)
			So(mp.HasKey(key3), ShouldBeFalse)
			val, err := mp.Read(key2)
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "hello")

			err = mset.LoadVersion(newversion)
			So(err, ShouldBeNil)
			So(mp.HasKey(key3), ShouldBeTrue)
			val, err = mp.Read(key2)
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "bye")

			key4 := []byte("key4")
			So(mp.Write(key4, 1), ShouldNotBeNil)
			version, err := mset.FixStateAsVersion()
			So(err, ShouldNotBeNil)
			So(version.IsValid(), ShouldBeFalse)

			err = mset.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
			So(mp.Write(key4, 1), ShouldBeNil)
			So(mp.Remove(key2), ShouldBeTrue)

			version, err = mset.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(version.IsValid(), ShouldBeTrue)
			So(mset.LoadVersion(version), ShouldBeNil)
			So(mp.HasKey(key1), ShouldBeFalse)
			So(mp.HasKey(key4), ShouldBeTrue)
			err = mset.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
			So(mp.HasKey(key1), ShouldBeFalse)
			So(mp.HasKey(key4), ShouldBeTrue)
		})

		Convey("Finally versions must be removable", func() {

			name := makeSetFromString("test")
			mset := db.GetOrCreateSet(name).(*MapVersionedSet)
			mp, _ := mset.GetOrCreateMap([]byte("mymapVersioned"))

			So(mset.RemoveVersionsUpTo(VersionID(2)), ShouldBeNil)
			err := mset.LoadVersion(VersionID(1))
			So(err, ShouldNotBeNil)

			So(mset.RemoveVersionsUpFrom(VersionID(2)), ShouldNotBeNil)
			So(mset.LoadVersion(VersionID(2)), ShouldBeNil)
			So(mset.RemoveVersionsUpFrom(VersionID(2)), ShouldBeNil)
			err = mset.LoadVersion(VersionID(3))
			So(err, ShouldNotBeNil)

			//check if we can reset heads
			So(mset.LoadVersion(VersionID(HEAD)), ShouldBeNil)
			key3 := []byte("key3")
			So(mp.Write(key3, 9.38), ShouldBeNil)
			mset.ResetHead()
			data, err := mp.Read(key3)
			So(err, ShouldBeNil)
			val, ok := data.(float64)
			So(ok, ShouldBeTrue)
			So(val, ShouldAlmostEqual, 1.34)

			mp.Remove(key3)
			So(mp.HasKey(key3), ShouldBeFalse)
			mset.ResetHead()
			So(mp.HasKey(key3), ShouldBeTrue)
		})
	})
}
