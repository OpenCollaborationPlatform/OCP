//parser for the datastructure markup language
package datastore

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMapData(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with map database,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db := store.GetDatabase(MapType)
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

		Convey("Maps can be created and data stored", func() {

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
