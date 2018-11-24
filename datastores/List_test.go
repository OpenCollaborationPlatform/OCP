package datastore

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestListBasic(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with list database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db, err := store.GetDatabase(ListType, false)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*ListDatabase)
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

		Convey("and data can be written and retreived.", func() {

			name := makeSetFromString("test")
			set, ok := db.GetOrCreateSet(name).(*ListSet)
			So(ok, ShouldBeTrue)
			So(set, ShouldNotBeNil)

			list, err := set.GetOrCreateList([]byte("list"))
			So(err, ShouldBeNil)
			So(list, ShouldNotBeNil)

			id1, err := list.Add("data1")
			So(err, ShouldBeNil)
			So(id1, ShouldEqual, 1)

			id2, err := list.Add("data2")
			So(err, ShouldBeNil)
			So(id2, ShouldEqual, 2)

			val, err := list.Read(id1)
			So(err, ShouldBeNil)
			So(val, ShouldNotBeNil)
			str, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, "data1")

			err = list.Set(id1, 12)
			So(err, ShouldBeNil)
			val, err = list.Read(id1)
			So(err, ShouldBeNil)
			So(val, ShouldNotBeNil)
			inte, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(inte, ShouldEqual, 12)

			err = list.Remove(id2)
			So(err, ShouldBeNil)
			val, err = list.Read(id2)
			So(err, ShouldNotBeNil)
			So(val, ShouldBeNil)
		})

	})
}

func TestListVersionedData(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with versioned list database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db, err := store.GetDatabase(ListType, true)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*ListVersionedDatabase)
		So(ok, ShouldBeTrue)

		Convey("sets can be creaded and deleted,", func() {

			name := makeSetFromString("test")
			So(db.HasSet(name), ShouldBeFalse)

			//test creation of set
			set := db.GetOrCreateSet(name)
			So(set, ShouldNotBeNil)
			So(db.HasSet(name), ShouldBeTrue)

			lset, ok := set.(*ListVersionedSet)
			So(ok, ShouldBeTrue)
			So(lset, ShouldNotBeNil)

			err := db.RemoveSet(name)
			So(err, ShouldBeNil)
			So(db.HasSet(name), ShouldBeFalse)
		})

		Convey("and listVersioneds can be created from the set.", func() {

			name := makeSetFromString("test")
			lset := db.GetOrCreateSet(name).(*ListVersionedSet)

			listVersionedkey := []byte("listVersionedkey")
			So(lset.HasList(listVersionedkey), ShouldBeFalse)
			So(lset.HasUpdates(), ShouldBeFalse)

			mp, err := lset.GetOrCreateList(listVersionedkey)
			So(err, ShouldBeNil)
			So(mp, ShouldNotBeNil)
			So(lset.HasList(listVersionedkey), ShouldBeTrue)

			//new listVersioned means no version yet means there are updates
			So(mp.HasUpdates(), ShouldBeTrue)
			So(lset.HasUpdates(), ShouldBeTrue)
		})

		Convey("ListVersioneds can be created and data stored", func() {

			name := makeSetFromString("test")
			lset := db.GetOrCreateSet(name).(*ListVersionedSet)

			list, err := lset.GetOrCreateList([]byte("mylistVersioned"))
			So(err, ShouldBeNil)
			So(list, ShouldNotBeNil)

			p1, err := list.Add(12)
			So(err, ShouldBeNil)
			So(p1, ShouldEqual, 1)
			val, err := list.Read(p1)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 12)

			_, err = list.Read(2)
			So(err, ShouldNotBeNil)
			p2, err := list.Add("hello")
			So(err, ShouldBeNil)
			So(p2, ShouldEqual, 2)
			val, err = list.Read(p2)
			So(err, ShouldBeNil)
			strvalue, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(strvalue, ShouldEqual, "hello")

			So(list.Remove(p1), ShouldBeTrue)
			_, err = list.Read(p1)
			So(err, ShouldNotBeNil)
		})

		Convey("and versioning of that listVersioned data works well", func() {

			name := makeSetFromString("test")
			lset := db.GetOrCreateSet(name).(*ListVersionedSet)
			list, _ := lset.GetOrCreateList([]byte("mylistVersioned"))

			So(list.HasUpdates(), ShouldBeTrue)

			oldversion, err := lset.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(oldversion, ShouldEqual, 1)
			So(list.HasUpdates(), ShouldBeFalse)

			//we rebuild the list pointers. hacky but well...
			var p2 uint64 = 2

			So(list.Set(p2, "bye"), ShouldBeNil)

			_, err = list.Add(1.34)
			So(err, ShouldBeNil)
			newversion, err := lset.FixStateAsVersion()
			So(err, ShouldBeNil)

			err = lset.LoadVersion(oldversion)
			So(err, ShouldBeNil)
			val, err := list.Read(p2)
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "hello")

			err = lset.LoadVersion(newversion)
			So(err, ShouldBeNil)
			val, err = list.Read(p2)
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "bye")

			p4, err := list.Add(1)
			So(err, ShouldBeNil)
			version, err := lset.FixStateAsVersion()
			So(err, ShouldNotBeNil)
			So(version.IsValid(), ShouldBeFalse)

			err = lset.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
			So(list.Set(p4, 1), ShouldBeNil)
			So(list.Remove(p2), ShouldBeTrue)

			version, err = lset.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(version.IsValid(), ShouldBeTrue)
			So(lset.LoadVersion(version), ShouldBeNil)
			err = lset.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
		})

		Convey("Finally versions must be removable", func() {

			name := makeSetFromString("test")
			lset := db.GetOrCreateSet(name).(*ListVersionedSet)
			list, _ := lset.GetOrCreateList([]byte("mylistVersioned"))

			So(lset.RemoveVersionsUpTo(VersionID(2)), ShouldBeNil)
			err := lset.LoadVersion(VersionID(1))
			So(err, ShouldNotBeNil)

			So(lset.RemoveVersionsUpFrom(VersionID(2)), ShouldNotBeNil)
			So(lset.LoadVersion(VersionID(2)), ShouldBeNil)
			So(lset.RemoveVersionsUpFrom(VersionID(2)), ShouldBeNil)
			err = lset.LoadVersion(VersionID(3))
			So(err, ShouldNotBeNil)

			//check if we can reset heads
			So(lset.LoadVersion(VersionID(HEAD)), ShouldBeNil)
			var p3 uint64 = 3
			So(list.Set(p3, 9.38), ShouldBeNil)
			lset.ResetHead()
			data, err := list.Read(p3)
			So(err, ShouldBeNil)
			val, ok := data.(float64)
			So(ok, ShouldBeTrue)
			So(val, ShouldAlmostEqual, 1.34)

			list.Remove(p3)
			//So(list.HasKey(key3), ShouldBeFalse)
			lset.ResetHead()
			//So(list.HasKey(key3), ShouldBeTrue)
		})
	})
}
