package datastore

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestVersioning(t *testing.T) {

	Convey("Creating a temporary datastore with a set manager,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		name := makeSetFromString("testset")
		mngr, err := NewVersionManager(name, store)
		So(err, ShouldBeNil)

		Convey("handling sets should behave normal", func() {

			//check value type
			set, err := mngr.GetDatabaseSet(ValueType)
			So(err, ShouldBeNil)
			So(set, ShouldNotBeNil)
			vset, ok := set.(*ValueVersionedSet)
			So(ok, ShouldBeTrue)

			key1 := []byte("key1")
			value, err := vset.GetOrCreateValue(key1)
			So(err, ShouldBeNil)
			ok, err = value.Exists()
			So(ok, ShouldBeTrue)
			So(err, ShouldBeNil)
			So(value.IsValid(), ShouldBeFalse)

			//check map type
			set, err = mngr.GetDatabaseSet(MapType)
			So(err, ShouldBeNil)
			mset, ok := set.(*MapVersionedSet)
			So(ok, ShouldBeTrue)
			mp, err := mset.GetOrCreateMap(key1)
			So(err, ShouldBeNil)
			So(mp.IsValid(), ShouldBeTrue)

			Convey("and versioning shall be aligned.", func() {

				So(value.Write(1), ShouldBeNil)
				So(mp.HasKey(1), ShouldBeFalse)
				So(mp.Write(1, "test"), ShouldBeNil)
				So(mp.HasKey(1), ShouldBeTrue)

				has, _ := mngr.HasUpdates()
				So(has, ShouldBeTrue)
				version, err := mngr.FixStateAsVersion()
				So(err, ShouldBeNil)
				So(uint64(version), ShouldEqual, 1)
				has, _ = mngr.HasUpdates()
				So(has, ShouldBeFalse)

				has, _ = vset.HasUpdates()
				So(has, ShouldBeFalse)
				has, _ = mset.HasUpdates()
				So(has, ShouldBeFalse)
				v, err := vset.GetCurrentVersion()
				So(err, ShouldBeNil)
				So(v.IsHead(), ShouldBeTrue)
				v, err = mset.GetCurrentVersion()
				So(err, ShouldBeNil)
				So(v.IsHead(), ShouldBeTrue)
				v, err = vset.GetLatestVersion()
				So(err, ShouldBeNil)
				So(uint64(v), ShouldEqual, 1)
				v, err = mset.GetLatestVersion()
				So(err, ShouldBeNil)
				So(uint64(v), ShouldEqual, 1)

				Convey("Old data is accessible", func() {

					So(value.Write(12), ShouldBeNil)

					has, _ := mngr.HasUpdates()
					So(has, ShouldBeTrue)
					has, _ = vset.HasUpdates()
					So(has, ShouldBeTrue)
					has, err = mset.HasUpdates()
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)
					version, err := mngr.FixStateAsVersion()
					So(err, ShouldBeNil)
					So(uint64(version), ShouldEqual, 2)
					has, _ = mngr.HasUpdates()
					So(has, ShouldBeFalse)

					v, err := vset.GetCurrentVersion()
					So(err, ShouldBeNil)
					So(v.IsHead(), ShouldBeTrue)
					v, err = mset.GetCurrentVersion()
					So(err, ShouldBeNil)
					So(v.IsHead(), ShouldBeTrue)
					v, err = vset.GetLatestVersion()
					So(err, ShouldBeNil)
					So(uint64(v), ShouldEqual, 2)
					v, err = mset.GetLatestVersion()
					So(err, ShouldBeNil)
					So(uint64(v), ShouldEqual, 1)

					So(mngr.LoadVersion(version), ShouldBeNil)
					v, err = vset.GetCurrentVersion()
					So(err, ShouldBeNil)
					So(v.IsHead(), ShouldBeFalse)
					v, err = mset.GetCurrentVersion()
					So(err, ShouldBeNil)
					So(v.IsHead(), ShouldBeFalse)
					v, err = vset.GetLatestVersion()
					So(err, ShouldBeNil)
					So(uint64(v), ShouldEqual, 2)
					v, err = mset.GetLatestVersion()
					So(err, ShouldBeNil)
					So(uint64(v), ShouldEqual, 1)

				})
			})
		})
	})
}
