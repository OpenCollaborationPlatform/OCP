package dml

import (
	datastore "github.com/ickby/CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTypeProperty(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including a type property,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
				.id:"toplevel"
				
				const property type test: Data {
					property int test: 10
				}
				
				Transaction {
					.id: "trans"
					.recursive: true
				}
			}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("The property must be accessbile", func() {
			So(rntm.mainObj.HasProperty("test"), ShouldBeTrue)
			val := rntm.mainObj.GetProperty("test").GetValue()
			dt, ok := val.(DataType)
			So(ok, ShouldBeTrue)

			val, err := rntm.RunJavaScript( "", "toplevel.test")
			So(err, ShouldBeNil)
			dtJs, ok := val.(DataType)
			So(ok, ShouldBeTrue)
			So(dtJs.IsEqual(dt), ShouldBeTrue)
		})

		Convey("and the the type must be creatable.", func() {

			code = `
				obj = new Object(toplevel.test)
				if (obj == null) {
					throw "object could not be created"
				}
				if (obj.test != 10) {
					throw "object properties not correctly created"
				}`
			_, err := rntm.RunJavaScript( "", code)
			So(err, ShouldBeNil)
		})

		Convey("and afterwards be accessible in the global objects", func() {

			code = `
				obj = new Object(toplevel.test)
				id = obj.Identifier()
				if (!(id in Objects)) {
					throw "object is not accessible, but should be"
				}`
			_, err := rntm.RunJavaScript( "", code)
			So(err, ShouldBeNil)
		})

	})
}
