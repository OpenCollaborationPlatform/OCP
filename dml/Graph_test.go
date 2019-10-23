package dml

import (
	datastore "github.com/ickby/CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODGraph(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including pod graph types,", t, func() {

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

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to directed int nodes should work", func() {
			
			code = `	toplevel.IntGraph.AddNode(1)
					toplevel.IntGraph.AddNode(2)
					toplevel.IntGraph.AddNode(4)`
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			
			code = `toplevel.IntGraph.Nodes()`
			val, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(val, ShouldResemble, []interface{}{int64(1),int64(2), int64(4)})
			
			Convey("And adding edges works as expected", func() {
				
				code = `	toplevel.IntGraph.NewEdge(1, 2)
					toplevel.IntGraph.NewEdge(2, 4)`
				_, err := rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)

				code = `toplevel.IntGraph.HasEdge(1,2)`
				val, err := rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeTrue)

				code = `toplevel.IntGraph.HasEdge(2,1)`
				val, err = rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeFalse)
			})
		})
	})
}