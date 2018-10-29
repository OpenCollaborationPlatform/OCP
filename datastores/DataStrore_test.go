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

		db := store.GetDatabase(ValueType)
		Convey("a key-value database must be creatable,", func() {

			So(db, ShouldNotBeNil)
			_, ok := db.(*ValueDatabase)
			So(ok, ShouldBeTrue)
		})
	})
}
