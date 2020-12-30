package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/ickby/CollaborationNode/datastores"

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
				.name:"toplevel"

				const property type test: Data {
					property int test: 10
				}

				property type test2: string

				function dtAsInt() {
					return new DataType("int")
				}

				function dtAsComplex() {
					var code = "Data{ property string test }"
					return new DataType(code)
				}
			}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("The property must be accessbile", func() {
			So(rntm.mainObj.obj.HasProperty("test"), ShouldBeTrue)
			val := rntm.mainObj.obj.GetProperty("test").GetValue(rntm.mainObj.id)
			dt, ok := val.(DataType)
			So(ok, ShouldBeTrue)

			val, err := rntm.RunJavaScript("", "toplevel.test")
			So(err, ShouldBeNil)
			dtJs, ok := val.(DataType)
			So(ok, ShouldBeTrue)
			So(dtJs.IsEqual(dt), ShouldBeTrue)

			Convey("and return a usable DataType object", func() {

				code = `
					if !toplevel.test2.IsString() {
						throw "expected string, is different datatype"
					}
					if !test2.IsPOD() {
						throw "Should be POD, but is not"
					}
					`
				_, err := rntm.RunJavaScript("", "toplevel.test")
				So(err, ShouldBeNil)
			})
		})

		Convey("Non-const type property is changeable", func() {
			code = `
				toplevel.test2 = toplevel.dtAsInt()
				if (!toplevel.test2.IsInt()) {
					throw "expected int, is different datatype"
				}
				if (!toplevel.test2.IsPOD()) {
					throw "Should be POD, but is not"
				}
			`
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
		})

		Convey("When set to a complex datatype", func() {
			code = `
				toplevel.test2 = toplevel.dtAsComplex()
				if (!toplevel.test2.IsComplex()) {
					throw "expected complex type, is different datatype"
				}
				if (toplevel.test2.IsPOD()) {
					throw "Should not be POD, but is"
				}
			`
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
		})

	})
}
