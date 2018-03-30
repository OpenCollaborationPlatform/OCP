//parser for the datastructure markup language
package dml

import (
	"CollaborationNode/datastores"
	"io/ioutil"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDmlFile(t *testing.T) {

	Convey("Establishing a datastore and runtime,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		rntm := NewRuntime(store)
		rntm.RegisterObjectCreator("Group", NewGroup)

		err = rntm.ParseFile("/home/stefan/Projects/Go/src/CollaborationNode/dml/test.dml")
		So(err, ShouldBeNil)

		Convey("the properties shall be accessible via js", func() {

			code := `Document.testI`
			val, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 1)

			code = `Document.testI = 5`
			val, err = rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			value, ok = val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 5)

			code = `Document.testI = "hello"`
			val, err = rntm.RunJavaScript(code)
			So(err, ShouldNotBeNil)
		})

		Convey("and event handling should work", func() {

			code := `
					fnc = function(a, b) {
						if (a != 2 || b != "hello") {
							throw "wrong arguments"
						}
					}
					Document.testE.RegisterCallback(fnc)
					Document.testE.Emit(2, "hello")
				`
			_, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)

			code = `Document.testE.Emit("hello", "2")`
			_, err = rntm.RunJavaScript(code)
			So(err, ShouldNotBeNil)

		})
	})
}
