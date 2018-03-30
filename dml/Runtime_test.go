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
		})

		Convey("and eventhandling should work", func() {

			code := `
					fnc = function(a, b) {
						console.log("Should be int: ", a)
						console.log("Should be string: ", b)
					}
					Document.testE.RegisterCallback(fnc)
					Document.testE.Emit(2, "hello")
				`
			/*code := `
			var propValue;
			for(var propName in Document.testE) {
			    propValue = Document.testE[propName]

			    console.log(propName,propValue);
			}`*/

			_, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
		})
	})
}
