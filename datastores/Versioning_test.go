package datastore

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestVersioning(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore with a set manager,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		So(store.Begin(), ShouldBeNil)
		defer store.Commit()

		name := makeSetFromString("testset")
		mngr := NewVersionManager(name, store)

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
			So(value.IsValid(), ShouldBeTrue)

			//check map type
			set, err = mngr.GetDatabaseSet(MapType)
			So(err, ShouldBeNil)
			mset, ok := set.(*MapVersionedSet)
			So(ok, ShouldBeTrue)
			mp, err := mset.GetOrCreateMap(key1)
			So(err, ShouldBeNil)
			valid, _ := mp.IsValid()
			So(valid, ShouldBeTrue)

			Convey("and versioning shall be aligned.", func() {

				So(value.Write(1), ShouldBeNil)
				So(mp.HasKey(1), ShouldBeFalse)
				So(mp.Write(1, "test"), ShouldBeNil)
				So(mp.HasKey(1), ShouldBeTrue)

				So(mngr.HasUpdates(), ShouldBeTrue)
				version, err := mngr.FixStateAsVersion()
				So(err, ShouldBeNil)
				So(uint64(version), ShouldEqual, 1)
				So(mngr.HasUpdates(), ShouldBeFalse)

				has, _ := vset.HasUpdates()
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
			})

			Convey("Old data is accessible", func() {

				So(value.Write(12), ShouldBeNil)

				So(mngr.HasUpdates(), ShouldBeTrue)
				has, _ := vset.HasUpdates()
				So(has, ShouldBeTrue)
				has, err = mset.HasUpdates()
				So(err, ShouldBeNil)
				So(has, ShouldBeFalse)
				version, err := mngr.FixStateAsVersion()
				So(err, ShouldBeNil)
				So(uint64(version), ShouldEqual, 2)
				So(mngr.HasUpdates(), ShouldBeFalse)

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
}
