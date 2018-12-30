//parser for the datastructure markup language
package dml

import (
	"CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBasics(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Setting up the basic required runtime,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		rntm := NewRuntime(store)

		Convey("a transaction manager shall be creatable", func() {

			mngr, err := NewTransactionManager(&rntm)
			So(err, ShouldBeNil)
			So(mngr.loadTransactions(), ShouldBeNil)

			Convey("which allows general transaction handling", func() {

				rntm.currentUser = "User1"
				So(mngr.IsOpen(), ShouldBeFalse)
				_, err := mngr.getOrCreateTransaction()
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeTrue)

				rntm.currentUser = "User2"
				So(mngr.IsOpen(), ShouldBeFalse)

			})

			Convey("and is accessible from Javascript", func() {

			})
		})
	})
}
