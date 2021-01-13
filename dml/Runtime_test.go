//parser for the datastructure markup language
package dml

import (
	"io/ioutil"
	"os"
	"testing"

	datastore "github.com/ickby/CollaborationNode/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDmlFile(t *testing.T) {

	Convey("Establishing a datastore and runtime,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		rntm := NewRuntime()

		//read in the file and parse
		filereader, err := os.Open("/home/stefan/Projects/Go/CollaborationNode/dml/test.dml")
		So(err, ShouldBeNil)
		err = rntm.Parse(filereader)
		So(err, ShouldBeNil)

		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("the properties shall be accessible via js", func() {

			code := `Document.testI`
			val, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 1)

			code = `Document.testI = 5`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			value, ok = val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 5)

			code = `Document.testI`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			value, ok = val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 5)

			code = `Document.testI = "hello"`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)

			code = `Document.testConst = 1`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)

			code = `Document.testConst = 1`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)

		})

		Convey("and event handling should work.", func() {

			code := `
					Document.testE.RegisterCallback(Document, "testEventCallback")
					Document.testE.Emit(2, "hello")
				`
			_, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `Document.testI`
			val, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 0)

			//check direct go access
			store.Begin()
			set, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			value, ok = set.obj.GetProperty(`testI`).GetValue(set.id).(int64)
			store.Rollback()
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 0)

			code = `Document.testE.Emit("hello", "2")`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)

			code = `Document.testB`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			bvalue, ok := val.(bool)
			So(ok, ShouldBeTrue)
			So(bvalue, ShouldBeFalse)

			code = `Document.testE2.Emit()`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `Document.testB`
			val, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			bvalue, ok = val.(bool)
			So(ok, ShouldBeTrue)
			So(bvalue, ShouldBeTrue)

			code = `
				Document.testF = 1.1
				if ( Math.abs(Document.testF - 1.1) > 1e-6 ) {
					console.log("error")
					throw "floating point number dosn't work"
				}
			`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			//testI must be one if the function was called correctly
			store.Begin()
			set, err = rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			value, ok = set.obj.GetProperty(`testI`).GetValue(set.id).(int64)
			store.Rollback()
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 1)

			Convey("with correct error capturing", func() {

				code = `Document.testErrorE.Emit()`
				_, err = rntm.RunJavaScript(store, "", code)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("Also functions should be callable", func() {

			code := `
			Document.testFnc(42)
			Document.testI`

			val, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 42)

			Convey("and print messages are collected", func() {
				msgs := rntm.GetMessages()
				So(len(msgs), ShouldEqual, 1)
				So(msgs[0], ShouldEqual, "test")
			})

			Convey("But errors are captured", func() {
				code := `Document.errorTestFnc()`
				_, err := rntm.RunJavaScript(store, "", code)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("Object hirarchy must be established", func() {

			code := `
				if (Document.children.length != 4) {
					throw "It must have exactly 4 child, not " + Document.children.length
				}
				if (Document.children[0].name != "DocumentObject") {
					throw "child access seems not to work"
				}
				
				if (Document.children[1].name != "ImportTest") {
					throw "Import seems not to work"
				}
				if (Document.parent != null) {
					throw "parent is not null, but should be"
				}
				if (!Document.children[0].parent.identifier.Equals(Document.identifier)) {
					throw "parent is not set correctly"
				}
				if (Document.DocumentObject.name != "DocumentObject") {
					throw "no direkt children access possible"
				}
				if (Document.ImportTest.name != "ImportTest") {
					throw "Import hirarchy somehowe corrupted"
				}
				`

			_, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
		})

		Convey("Imported object must be loaded", func() {

			store.Begin()
			defer store.Rollback()
			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			imp, err := main.obj.(Data).GetChildByName(main.id, "ImportTest")
			So(err, ShouldBeNil)
			So(imp, ShouldNotBeNil)

			Convey("and has its own childs correctly setup", func() {
				impchild, err := imp.obj.(Data).GetChildByName(imp.id, "ImportedChild")
				So(err, ShouldBeNil)
				So(impchild, ShouldNotBeNil)

				prop := impchild.obj.GetProperty("test")
				So(prop, ShouldNotBeNil)
				So(prop.GetValue(imp.id), ShouldEqual, 10)
			})

			Convey("and is extended wiith custom property and child", func() {

				prop := imp.obj.GetProperty("annothertest")
				So(prop, ShouldNotBeNil)
				So(prop.GetValue(imp.id), ShouldEqual, 4)

				newchild, err := imp.obj.(Data).GetChildByName(imp.id, "DefaultChild")
				So(err, ShouldBeNil)
				So(newchild, ShouldNotBeNil)
			})
		})

		Convey("This assignment in functions works as expected", func() {

			thiscode := `	Document.ThisTest.assign()
						 	Document.ThisTest.Sub.test.Emit()`

			_, err := rntm.RunJavaScript(store, "", thiscode)
			So(err, ShouldBeNil)
		})

		Convey("and const functions are recognized", func() {

			c, err := rntm.IsConstant(store, "Document.readString")
			So(err, ShouldBeNil)
			So(c, ShouldBeTrue)

			c, err = rntm.IsConstant(store, "Document.testFnc")
			So(err, ShouldBeNil)
			So(c, ShouldBeFalse)
		})

		/*	Convey("Behaviour Managers shall be callable", func() {

			c, err := rntm.Call(store, "", "Transaction.IsOpen")
			So(err, ShouldBeNil)
			So(c, ShouldBeFalse)
		})*/

		Convey("and created event was emitted", func() {

			res, err := rntm.Call(store, "", "Document.created")
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)
		})
	})
}
