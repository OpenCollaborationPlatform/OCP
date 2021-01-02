package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/ickby/CollaborationNode/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMultistore(t *testing.T) {

	Convey("Loading dml code into runtime including dynamic object types,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		//create the datastore
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"

					Graph {
						.name: "IntGraph"
						.node: int
						.directed: true
					}
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Adding to directed int nodes should work", func() {

		})

	})
}
