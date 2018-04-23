//parser for the datastructure markup language
package dml

import (
	"CollaborationNode/datastores"
	"io/ioutil"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDmlFile(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")

	Convey("Establishing a datastore and runtime,", t, func() {

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

		Convey("and event handling should work.", func() {

			code := `
					fnc = function(a, b) {
						if (a != 2 || b != "hello") {
							throw "wrong arguments"
						}
						Document.testI = 0
					}
					Document.testE.RegisterCallback(fnc)
					Document.testE.Emit(2, "hello")
				`
			_, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)

			code = `Document.testI`
			val, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 0)

			//check direct go access
			value, ok = rntm.GetObject().GetProperty(`testI`).GetValue().(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 0)

			code = `Document.testE.Emit("hello", "2")`
			_, err = rntm.RunJavaScript(code)
			So(err, ShouldNotBeNil)

			code = `Document.testB`
			val, err = rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			bvalue, ok := val.(bool)
			So(ok, ShouldBeTrue)
			So(bvalue, ShouldBeFalse)

		})

		Convey("Also functions should be callable", func() {

			code := `
			Document.testFnc(42)
			Document.testI`

			val, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 42)
		})

		Convey("Object hirarchy must be established", func() {

			code := `
				if (Document.children.length != 1) {
					throw "It must have exactly 1 child"
				}
				if (Document.children[0].id != "DocumentObject") {
					throw "child access seems not to work"
				}
				if (Document.parent != null) {
					throw "parent is not null, but should be"
				}
				if (Document.children[0].parent != Document) {
					throw "parent is not set correctly"
				}`

			_, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
		})
	})
}
