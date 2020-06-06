//parser for the datastructure markup language
package dml

import (
	datastore "github.com/ickby/CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDmlFile(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Establishing a datastore and runtime,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		rntm := NewRuntime(store)

		//read in the file and parse
		filereader, err := os.Open("/home/stefan/Projects/Go/CollaborationNode/dml/test.dml")
		So(err, ShouldBeNil)
		err = rntm.Parse(filereader)
		So(err, ShouldBeNil)

		Convey("the properties shall be accessible via js", func() {

			code := `Document.testI`
			val, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 1)

			code = `Document.testI = 5`
			val, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			value, ok = val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 5)

			code = `Document.testI`
			val, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			value, ok = val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 5)

			code = `Document.testI = "hello"`
			val, err = rntm.RunJavaScript("", code)
			So(err, ShouldNotBeNil)

			code = `Document.testConst = 1`
			val, err = rntm.RunJavaScript("", code)
			So(err, ShouldNotBeNil)

			code = `Document.testConst = 1`
			val, err = rntm.RunJavaScript("", code)
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
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			code = `Document.testI`
			val, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 0)

			//check direct go access
			store.Begin()
			obj := rntm.mainObj
			value, ok = obj.GetProperty(`testI`).GetValue().(int64)
			store.Rollback()
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 0)

			code = `Document.testE.Emit("hello", "2")`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldNotBeNil)

			code = `Document.testB`
			val, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			bvalue, ok := val.(bool)
			So(ok, ShouldBeTrue)
			So(bvalue, ShouldBeFalse)

			code = `Document.testE2.Emit()`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			code = `Document.testB`
			val, err = rntm.RunJavaScript("", code)
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
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			//testI must be one if the function was called correctly
			store.Begin()
			obj = rntm.mainObj
			value, ok = obj.GetProperty(`testI`).GetValue().(int64)
			store.Rollback()
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 1)
		})

		Convey("Also functions should be callable", func() {

			code := `
			Document.testFnc(42)
			Document.testI`

			val, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			value, ok := val.(int64)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, 42)
		})

		Convey("Object hirarchy must be established", func() {

			code := `
				if (Document.children.length != 3) {
					throw "It must have exactly 3 child"
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
				if (Document.children[0].parent != Document) {
					throw "parent is not set correctly"
				}
				if (Document.DocumentObject.name != "DocumentObject") {
					throw "no direkt children access possible"
				}
				`

			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
		})

		Convey("And objects mus be accessbile by id", func() {

			code := `
				id = Document.DocumentObject.Identifier()
				
				if (!(id in Objects)) {
					throw "object is not know, but should be"
				}
				
				obj = Objects[id]
				
				if (obj.test != 10) {
					throw "unable to get correct object from identifier"
				}
			`
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
		})

		Convey("Imported object must be loaded", func() {

			imp, err := rntm.mainObj.GetChildByName("ImportTest")
			So(err, ShouldBeNil)
			So(imp, ShouldNotBeNil)

			Convey("and has its own childs correctly setup", func() {
				impchild, err := imp.GetChildByName("ImportedChild")
				So(err, ShouldBeNil)
				So(impchild, ShouldNotBeNil)

				prop := impchild.GetProperty("test")
				So(prop, ShouldNotBeNil)
				store.Begin()
				defer store.Rollback()
				So(prop.GetValue(), ShouldEqual, 10)
			})

			Convey("and is extended wiith custom property and child", func() {

				prop := imp.GetProperty("annothertest")
				So(prop, ShouldNotBeNil)
				store.Begin()
				defer store.Rollback()
				So(prop.GetValue(), ShouldEqual, 4)

				newchild, err := imp.GetChildByName("DefaultChild")
				So(err, ShouldBeNil)
				So(newchild, ShouldNotBeNil)
			})
		})

		Convey("This assignment in functions works as expected", func() {

			thiscode := `Document.ThisTest.assign()
						Document.ThisTest.Sub.test.Emit()`

			_, err := rntm.RunJavaScript("", thiscode)
			So(err, ShouldBeNil)
		})

		Convey("and const functions are recognized", func() {

			c, err := rntm.IsConstant("Document.readString")
			So(err, ShouldBeNil)
			So(c, ShouldBeTrue)

			c, err = rntm.IsConstant("Document.testFnc")
			So(err, ShouldBeNil)
			So(c, ShouldBeFalse)
		})
	})
}
