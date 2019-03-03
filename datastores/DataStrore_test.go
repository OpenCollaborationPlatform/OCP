//parser for the datastructure markup language
package datastore

import (
	"crypto/sha256"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func makeSetFromString(name string) [32]byte {

	data, err := json.Marshal(name)
	if err != nil {
		var data [32]byte
		return data
	}
	return sha256.Sum256(data)
}

func TestDataStore(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		So(store.Begin(), ShouldBeNil)
		defer So(store.Commit(), ShouldBeNil)

		Convey("a key-value database must be creatable,", func() {

			db, err := store.GetDatabase(ValueType, true)
			So(err, ShouldBeNil)
			So(db, ShouldNotBeNil)
			_, ok := db.(*ValueVersionedDatabase)
			So(ok, ShouldBeTrue)
		})
	})
}

func TestTransaction(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Creating a temporary datastore,", t, func() {

		store, err := NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		db, err := store.GetDatabase(ValueType, true)
		So(err, ShouldBeNil)

		Convey("It should not be possible to view or update without transaction", func() {

			set, err := db.GetOrCreateSet(makeSetFromString("testset"))
			So(err, ShouldNotBeNil)
			So(set, ShouldBeNil)
			has, err := db.HasSet(makeSetFromString("testset"))
			So(err, ShouldNotBeNil)
			So(has, ShouldBeFalse)
		})

		Convey("but should be possible once opened", func() {

			So(store.Begin(), ShouldBeNil)
			set, err := db.GetOrCreateSet(makeSetFromString("testset"))
			So(err, ShouldBeNil)
			So(set, ShouldNotBeNil)
			has, err := db.HasSet(makeSetFromString("testset"))
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			Convey("When rolled back the set should not be there anymore", func() {

				So(store.Rollback(), ShouldBeNil)
				So(store.Begin(), ShouldBeNil)
				has, err := db.HasSet(makeSetFromString("testset"))
				So(err, ShouldBeNil)
				So(has, ShouldBeFalse)
				So(store.Rollback(), ShouldBeNil)
			})

			Convey("When commited it shall not be possible to add a new one directly", func() {

				So(store.Commit(), ShouldBeNil)
				//accessing the set is only possible when in transaction
				So(store.Begin(), ShouldBeNil)

				has, err := db.HasSet(makeSetFromString("testset"))
				So(err, ShouldBeNil)
				So(has, ShouldBeTrue)
				So(store.Rollback(), ShouldBeNil)
			})
		})

	})
}
