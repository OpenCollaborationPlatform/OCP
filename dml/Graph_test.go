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
					toplevel.IntGraph.NewEdge(2, 4)
					toplevel.IntGraph.NewEdge(4, 1)`
				_, err := rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)

				code = `toplevel.IntGraph.HasEdgeBetween(1,2)`
				val, err := rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeTrue)

				code = `toplevel.IntGraph.HasEdgeBetween(2,1)`
				val, err = rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeFalse)

				code = `toplevel.IntGraph.HasEdgeBetween(1,4)`
				val, err = rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeFalse)

				code = `toplevel.IntGraph.HasEdgeBetween(4,1)`
				val, err = rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)
				So(val, ShouldBeTrue)
				
				Convey("Conenctions are returned correctly", func() {
					
					code = `	toplevel.IntGraph.FromNode(1)`
					val, err = rntm.RunJavaScript("", code)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(2)})

					code = `	toplevel.IntGraph.ToNode(1)`
					val, err = rntm.RunJavaScript("", code)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(4)})
				})
				
				Convey("Removing a edge works as expected", func() {
					
					code = `	toplevel.IntGraph.RemoveEdgeBetween(1,2);
							toplevel.IntGraph.HasEdgeBetween(1,2)`
					val, err = rntm.RunJavaScript("", code)
					So(err, ShouldBeNil)
					So(val, ShouldBeFalse)
				})

				Convey("Removing a node works", func() {
					
					code = `	toplevel.IntGraph.RemoveNode(2);
							toplevel.IntGraph.Nodes()`
					val, err = rntm.RunJavaScript("", code)
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []interface{}{int64(1), int64(4)})
					
					Convey("and does also remove all relevant edges", func() {
						
						code = `	toplevel.IntGraph.HasEdgeBetween(1,2)`
						_, err = rntm.RunJavaScript("", code)
						So(err, ShouldNotBeNil)
						code = `	toplevel.IntGraph.HasEdgeBetween(2,4)`
						_, err = rntm.RunJavaScript("", code)
						So(err, ShouldNotBeNil)
						code = `	toplevel.IntGraph.HasEdgeBetween(4,1)`
						val, err = rntm.RunJavaScript("", code)
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
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			code = `toplevel.IntGraph.Sorted()`
			res, err := rntm.RunJavaScript("", code)
			sorted := res.([]interface{})
			So(sorted[4], ShouldEqual, 1) //1 must be last, others can very a bit in order

			code = `toplevel.IntGraph.Cycles()`
			cycles, err := rntm.RunJavaScript("", code)
			So(cycles, ShouldHaveLength, 0)
			
			Convey("and cycles are detected", func() {
				
				code = `
					toplevel.IntGraph.NewEdge(1,5)
					toplevel.IntGraph.NewEdge(4,3)
					`
				_, err := rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)

				code = `toplevel.IntGraph.Cycles()`
				cycles, err := rntm.RunJavaScript("", code)
				So(cycles, ShouldHaveLength, 1)

				code = `toplevel.IntGraph.Sorted()`
				sorted, err := rntm.RunJavaScript("", code)
				So(sorted, ShouldResemble, []interface{}{int64(4), int64(3), []interface{}{int64(1),int64(2),int64(5)}})
			})
		})
	})
}