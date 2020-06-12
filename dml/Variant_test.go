package dml

import (
	datastore "github.com/ickby/CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODVariant(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including pod variant types,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"
					const property type other: bool
					
					Variant {
						.name: "Variant"
						.type: int
					}				
				}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to int variant should work", func() {

			code = `toplevel.Variant.SetValue(10)`
			_, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			res, err := rntm.Call("", "toplevel.Variant.GetValue")
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 10)

			Convey("but setting wrong type should fail", func() {

				code = `toplevel.Variant.SetValue("hello")`
				_, err := rntm.RunJavaScript("", code)
				So(err, ShouldNotBeNil)
				res, err := rntm.Call("", "toplevel.Variant.GetValue")
				So(err, ShouldBeNil)
				So(res, ShouldEqual, 10)
			})

			Convey("Changeing the datatype works", func() {

				code = `toplevel.Variant.type = toplevel.other`
				_, err := rntm.RunJavaScript("", code)
				So(err, ShouldBeNil)

				Convey("and initialized the value to the default of the new datatype", func() {
					res, err := rntm.Call("", "toplevel.Variant.GetValue")
					So(err, ShouldBeNil)
					So(res, ShouldBeFalse)
				})
			})
		})
	})
}

func TestTypeVariant(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including type variant,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"

					Variant {
						.name: "Variant"
						.type: int
					}
					
					Variant {
						.name: "TypeVariant"
						.type: Data {
							.name: "TestData"
						}	
					}
				}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("The default complex type should be created", func() {

			store.Begin()
			defer store.Rollback()

			vari, err := rntm.getObjectFromPath("toplevel.TypeVariant")
			So(err, ShouldBeNil)
			value, err := vari.(*variant).GetValue()
			So(value, ShouldNotBeNil)
			So(err, ShouldBeNil)

			So(value.(Object).DataType(), ShouldResemble, vari.GetProperty("type").GetValue())
		})

		Convey("Changing the datatype to a complex one should work", func() {

			code = `
				toplevel.Variant.type = new DataType("Data{property int test: 10}")
			`
			_, err := rntm.RunJavaScript("user3", code)
			So(err, ShouldBeNil)

			Convey("and the value should be a nicely initialized object", func() {

				code = `
					obj = toplevel.Variant.GetValue()
					if (obj == null) {
						throw "object not correctly initialized"
					}
					if (obj.test != 10) {
						throw "initialisation failed: value should be 10"
					}
					id = obj.Identifier()
					if (!(id in Objects)) {
						throw "object is not accessible, but should be"
					}
				`
				_, err := rntm.RunJavaScript("user3", code)
				So(err, ShouldBeNil)
			})
		})
	})
}
