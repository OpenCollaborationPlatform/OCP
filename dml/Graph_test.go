package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODGraph(t *testing.T) {

	Convey("Loading dml code into runtime including pod graph types,", t, func() {

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

			code = `	toplevel.IntGraph.AddNode(1)
					toplevel.IntGraph.AddNode(2)
					toplevel.IntGraph.AddNode(4)`
			_, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `toplevel.IntGraph.Nodes()`
			val, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(val, ShouldResemble, []interface{}{int64(1), int64(2), int64(4)})

			Convey("And adding edges works as expected", func() {

				code = `	toplevel.IntGraph.NewEdge(1, 2)
					toplevel.IntGraph.NewEdge(2, 4)
					toplevel.IntGraph.NewEdge(4, 1)`
				_, _, err := rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)

				code = `toplevel.IntGraph.HasEdgeBetween(1,2)`
				val, _, err := rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeTrue)

				code = `toplevel.IntGraph.HasEdgeBetween(2,1)`
				val, _, err = rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeFalse)

				code = `toplevel.IntGraph.HasEdgeBetween(1,4)`
				val, _, err = rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeFalse)

				code = `toplevel.IntGraph.HasEdgeBetween(4,1)`
				val, _, err = rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeTrue)

				Convey("Connections are returned correctly", func() {

					code = `	toplevel.IntGraph.FromNode(1)`
					val, _, err = rntm.RunJavaScript(store, "", code)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(2)})

					code = `	toplevel.IntGraph.ToNode(1)`
					val, _, err = rntm.RunJavaScript(store, "", code)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(4)})

					code = `	toplevel.IntGraph.ToNode(7)`
					val, _, err = rntm.RunJavaScript(store, "", code)
					So(err, ShouldNotBeNil)
					So(val, ShouldBeNil)

					val, _, err = rntm.Call(store, "", "toplevel.IntGraph.ToNode", 1)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(4)})

					val, _, err = rntm.Call(store, "", "toplevel.IntGraph.ToNode", 7)
					So(err, ShouldNotBeNil)
					So(val, ShouldBeNil)
				})

				Convey("Removing a edge works as expected", func() {

					code = `	toplevel.IntGraph.RemoveEdgeBetween(1,2);
							toplevel.IntGraph.HasEdgeBetween(1,2)`
					val, _, err = rntm.RunJavaScript(store, "", code)
					So(err, ShouldBeNil)
					So(val, ShouldBeFalse)
				})

				Convey("Removing a node works", func() {

					code = `	toplevel.IntGraph.RemoveNode(2);
							toplevel.IntGraph.Nodes()`
					val, _, err = rntm.RunJavaScript(store, "", code)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(1), int64(4)})

					Convey("and does also remove all relevant edges", func() {

						code = `	toplevel.IntGraph.HasEdgeBetween(1,2)`
						_, _, err = rntm.RunJavaScript(store, "", code)
						So(err, ShouldNotBeNil)
						code = `	toplevel.IntGraph.HasEdgeBetween(2,4)`
						_, _, err = rntm.RunJavaScript(store, "", code)
						So(err, ShouldNotBeNil)
						code = `	toplevel.IntGraph.HasEdgeBetween(4,1)`
						val, _, err = rntm.RunJavaScript(store, "", code)
						So(err, ShouldBeNil)
						So(val, ShouldBeTrue)
					})
				})
			})
		})

		Convey("Topological sorting work well", func() {

			code = `	toplevel.IntGraph.AddNode(1)
					toplevel.IntGraph.AddNode(2)
					toplevel.IntGraph.AddNode(3)
					toplevel.IntGraph.AddNode(4)
					toplevel.IntGraph.AddNode(5)

					toplevel.IntGraph.NewEdge(2,1)
					toplevel.IntGraph.NewEdge(3,1)
					toplevel.IntGraph.NewEdge(4,2)
					toplevel.IntGraph.NewEdge(5,2)
					`
			_, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `toplevel.IntGraph.Sorted()`
			res, _, err := rntm.RunJavaScript(store, "", code)
			sorted := res.([]interface{})
			So(sorted[4], ShouldEqual, 1) //1 must be last, others can very a bit in order

			code = `toplevel.IntGraph.Cycles()`
			cycles, _, err := rntm.RunJavaScript(store, "", code)
			So(cycles, ShouldHaveLength, 0)

			Convey("and cycles are detected", func() {

				code = `
					toplevel.IntGraph.NewEdge(1,5)
					toplevel.IntGraph.NewEdge(4,3)
					`
				_, _, err := rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)

				code = `toplevel.IntGraph.Cycles()`
				cycles, _, err := rntm.RunJavaScript(store, "", code)
				So(cycles, ShouldHaveLength, 1)

				code = `toplevel.IntGraph.Sorted()`
				sorted, _, err := rntm.RunJavaScript(store, "", code)
				So(sorted, ShouldResemble, []interface{}{int64(4), int64(3), []interface{}{int64(1), int64(2), int64(5)}})
			})
		})
	})
}
