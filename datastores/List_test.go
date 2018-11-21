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

			err = list.Change(id1, 12)
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
