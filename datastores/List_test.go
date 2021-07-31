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
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		db, err := store.GetDatabase(ListType, false)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*ListDatabase)
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

		Convey("and data can be written and retreived.", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set, ok := genset.(*ListSet)
			So(ok, ShouldBeTrue)
			So(set, ShouldNotBeNil)

			list, err := set.GetOrCreateList([]byte("list"))
			So(err, ShouldBeNil)
			So(list, ShouldNotBeNil)
			iterValue, err := list.First()
			So(err, ShouldBeNil)
			So(iterValue, ShouldBeNil)
			iterValue, err = list.Last()
			So(err, ShouldBeNil)
			So(iterValue, ShouldBeNil)

			entry1, err := list.Add("data1")
			So(err, ShouldBeNil)
			So(entry1, ShouldNotBeNil)

			iterValue, err = list.First()
			So(err, ShouldBeNil)
			So(iterValue.IsValid(), ShouldBeTrue)
			So(iterValue.Reference(), ShouldEqual, entry1.Reference())

			iterValue, err = list.Last()
			So(err, ShouldBeNil)
			So(iterValue.IsValid(), ShouldBeTrue)
			So(iterValue.Reference(), ShouldEqual, entry1.Reference())

			prev, err := iterValue.Previous()
			So(err, ShouldBeNil)
			So(prev, ShouldBeNil)
			next, err := iterValue.Next()
			So(err, ShouldBeNil)
			So(next, ShouldBeNil)

			entry2, err := list.Add("data2")
			So(err, ShouldBeNil)
			So(entry2, ShouldNotBeNil)

			iterValue, err = list.Last()
			So(err, ShouldBeNil)
			So(iterValue.IsValid(), ShouldBeTrue)
			So(iterValue.Reference(), ShouldEqual, entry2.Reference())

			prev, err = entry2.Previous()
			So(err, ShouldBeNil)
			So(prev, ShouldNotBeNil)
			So(prev.Reference(), ShouldEqual, entry1.Reference())

			next, err = entry1.Next()
			So(err, ShouldBeNil)
			So(prev, ShouldNotBeNil)
			So(next.Reference(), ShouldEqual, entry2.Reference())

			val, err := entry1.Read()
			So(err, ShouldBeNil)
			So(val, ShouldNotBeNil)
			str, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, "data1")

			err = entry1.Write(12)
			So(err, ShouldBeNil)
			val, err = entry1.Read()
			So(err, ShouldBeNil)
			So(val, ShouldNotBeNil)
			inte, ok := val.(int)
			So(ok, ShouldBeTrue)
			So(inte, ShouldEqual, 12)

			pval, _ := prev.Read()
			So(pval, ShouldEqual, val)

			err = entry2.Remove()
			So(err, ShouldBeNil)
			val, err = entry2.Read()
			So(err, ShouldNotBeNil)
			So(val, ShouldBeNil)

			next, err = entry1.Next()
			So(err, ShouldBeNil)
			So(next, ShouldBeNil)

			iterValue, err = list.Last()
			So(err, ShouldBeNil)
			So(iterValue.IsValid(), ShouldBeTrue)
			So(iterValue.Reference(), ShouldEqual, entry1.Reference())
		})

		Convey("Entries shall be retrievable", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			set, _ := genset.(*ListSet)
			list, err := set.GetOrCreateList([]byte("list"))

			entries, err := list.GetValues()
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
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
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		db, err := store.GetDatabase(ListType, true)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		_, ok := db.(*ListVersionedDatabase)
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

			lset, ok := set.(*ListVersionedSet)
			So(ok, ShouldBeTrue)
			So(lset, ShouldNotBeNil)

			err = db.RemoveSet(name)
			So(err, ShouldBeNil)
			has, err = db.HasSet(name)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
		})

		Convey("and listVersioneds can be created from the set.", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			lset, _ := genset.(*ListVersionedSet)

			listVersionedkey := []byte("listVersionedkey")
			So(lset.HasList(listVersionedkey), ShouldBeFalse)
			has, _ := lset.HasUpdates()
			So(has, ShouldBeFalse)

			mp, err := lset.GetOrCreateList(listVersionedkey)
			So(err, ShouldBeNil)
			So(mp, ShouldNotBeNil)
			So(lset.HasList(listVersionedkey), ShouldBeTrue)

			//new listVersioned means no version yet means there are updates
			has, _ = mp.HasUpdates()
			So(has, ShouldBeTrue)
			has, _ = lset.HasUpdates()
			So(has, ShouldBeTrue)
		})

		Convey("ListVersioneds can be created and data stored", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			lset, ok := genset.(*ListVersionedSet)

			list, err := lset.GetOrCreateList([]byte("mylistVersioned"))
			So(err, ShouldBeNil)
			So(list, ShouldNotBeNil)

			entry1, err := list.Add(12)
			So(err, ShouldBeNil)
			So(entry1, ShouldNotBeNil)
			val, err := entry1.Read()
			So(err, ShouldBeNil)
			value, ok := val.(int)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 12)

			entry2, err := list.Add("hello")
			So(err, ShouldBeNil)
			So(entry2, ShouldNotBeNil)
			val, err = entry2.Read()
			So(err, ShouldBeNil)
			strvalue, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(strvalue, ShouldEqual, "hello")

			entries, err := list.GetEntries()
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 2)

			So(entry1.Remove(), ShouldBeNil)
			_, err = entry1.Read()
			So(err, ShouldNotBeNil)

			entries, err = list.GetEntries()
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
		})

		Convey("and versioning of that listVersioned data works well", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			lset, _ := genset.(*ListVersionedSet)
			list, _ := lset.GetOrCreateList([]byte("mylistVersioned"))

			has, _ := lset.HasUpdates()
			So(has, ShouldBeTrue)
			has, _ = list.HasUpdates()
			So(has, ShouldBeTrue)
			has, _ = lset.HasVersions()
			So(has, ShouldBeFalse)
			has, _ = list.HasVersions()
			So(has, ShouldBeFalse)

			oldversion, err := lset.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(oldversion, ShouldEqual, 1)
			has, _ = lset.HasUpdates()
			So(has, ShouldBeFalse)
			has, _ = list.HasUpdates()
			So(has, ShouldBeFalse)
			has, _ = lset.HasVersions()
			So(has, ShouldBeTrue)
			has, _ = list.HasVersions()
			So(has, ShouldBeTrue)

			//we rebuild the list entries
			entries, err := list.GetEntries()
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
			entry2 := entries[0]

			So(entry2.Write("bye"), ShouldBeNil)

			_, err = list.Add(1.34)
			So(err, ShouldBeNil)
			newversion, err := lset.FixStateAsVersion()
			So(err, ShouldBeNil)

			err = lset.LoadVersion(oldversion)
			So(err, ShouldBeNil)
			val, err := entry2.Read()
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "hello")

			err = lset.LoadVersion(newversion)
			So(err, ShouldBeNil)
			val, err = entry2.Read()
			So(err, ShouldBeNil)
			So(val.(string), ShouldEqual, "bye")

			//when version is checked out no change is allowed
			_, err = list.Add(1)
			So(err, ShouldNotBeNil)
			version, err := lset.FixStateAsVersion()
			So(err, ShouldNotBeNil)
			So(version.IsValid(), ShouldBeFalse)

			//checking out HEAD allows change
			err = lset.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
			So(entry2.Remove(), ShouldBeNil)

			version, err = lset.FixStateAsVersion()
			So(err, ShouldBeNil)
			So(version.IsValid(), ShouldBeTrue)
			So(lset.LoadVersion(version), ShouldBeNil)
			err = lset.LoadVersion(VersionID(HEAD))
			So(err, ShouldBeNil)
		})

		Convey("Finally versions must be removable", func() {

			name := makeSetFromString("test")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			lset, ok := genset.(*ListVersionedSet)
			list, _ := lset.GetOrCreateList([]byte("mylistVersioned"))

			So(lset.RemoveVersionsUpTo(VersionID(2)), ShouldBeNil)
			err = lset.LoadVersion(VersionID(1))
			So(err, ShouldNotBeNil)

			So(lset.RemoveVersionsUpFrom(VersionID(2)), ShouldNotBeNil)
			So(lset.LoadVersion(VersionID(2)), ShouldBeNil)
			So(lset.RemoveVersionsUpFrom(VersionID(2)), ShouldBeNil)
			err = lset.LoadVersion(VersionID(3))
			So(err, ShouldNotBeNil)

			//check if we can reset heads
			So(lset.LoadVersion(VersionID(HEAD)), ShouldBeNil)

			entries, err := list.GetEntries()
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
			entry3 := entries[0]
			So(entry3.Write(9.38), ShouldBeNil)
			lset.ResetHead()
			data, err := entry3.Read()
			So(err, ShouldBeNil)
			val, ok := data.(float64)
			So(ok, ShouldBeTrue)
			So(val, ShouldAlmostEqual, 1.34)

			entry3.Remove()
			So(entry3.IsValid(), ShouldBeFalse)
			lset.ResetHead()
			So(entry3.IsValid(), ShouldBeTrue)
		})

		Convey("A new ListSet with lists, but no versions yet", func() {

			name := makeSetFromString("testreset")
			genset, err := db.GetOrCreateSet(name)
			So(err, ShouldBeNil)
			lset, _ := genset.(*ListVersionedSet)

			Convey("should be resettable to not have list", func() {
				list, _ := lset.GetOrCreateList([]byte("myresetlist"))
				list.Add("entry1")
				list.Add("entry2")

				lset.ResetHead()
				So(lset.HasList([]byte("myresetlist")), ShouldBeFalse)
			})

			Convey("Creating a initial list version and adding data", func() {
				list, _ := lset.GetOrCreateList([]byte("myresetlist"))
				lset.FixStateAsVersion()
				list.Add("entry1")
				list.Add("entry2")

				Convey("should reset to list available, but empty", func() {

					lset.ResetHead()
					So(lset.HasList([]byte("myresetlist")), ShouldBeTrue)
					entries, err := list.GetEntries()
					So(err, ShouldBeNil)
					So(len(entries), ShouldEqual, 0)
				})
			})
		})
	})
}
