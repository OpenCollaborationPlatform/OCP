package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODVariant(t *testing.T) {

	Convey("Loading dml code into runtime including pod variant types,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"
					const property type other: bool

					Variant {
						.name: "Variant"
						.type: int
						
						property string beforeChangeKeys
						property string changeKeys

						.onBeforeChange: function(key) {
							this.beforeChangeKeys += key
						}
						.onChanged: function(key) {
							this.changeKeys += key
						}
					}
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Adding to int variant should work", func() {

			code = `toplevel.Variant.SetValue(10)`
			_, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			res, err := rntm.Call(store, "", "toplevel.Variant.GetValue")
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 10)

			Convey("and emit correct events", func() {

				code = `toplevel.Variant.beforeChangeKeys`
				res, err := rntm.Call(store, "", code)
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "value")

				code = `toplevel.Variant.changeKeys`
				res, err = rntm.Call(store, "", code)
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "value")
			})

			Convey("but setting wrong type should fail", func() {

				code = `toplevel.Variant.SetValue("hello")`
				_, err := rntm.RunJavaScript(store, "", code)
				So(err, ShouldNotBeNil)
				res, err := rntm.Call(store, "", "toplevel.Variant.GetValue")
				So(err, ShouldBeNil)
				So(res, ShouldEqual, 10)

				Convey("and not add any events", func() {

					code = `toplevel.Variant.beforeChangeKeys`
					res, err := rntm.Call(store, "", code)
					So(err, ShouldBeNil)
					So(res, ShouldEqual, "value")

					code = `toplevel.Variant.changeKeys`
					res, err = rntm.Call(store, "", code)
					So(err, ShouldBeNil)
					So(res, ShouldEqual, "value")
				})
			})

			Convey("Changeing the datatype works", func() {

				code = `toplevel.Variant.type = toplevel.other`
				_, err := rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)

				Convey("and initialized the value to the default of the new datatype", func() {
					res, err := rntm.Call(store, "", "toplevel.Variant.GetValue")
					So(err, ShouldBeNil)
					So(res, ShouldBeFalse)
				})

				Convey("and emit aditional change events", func() {

					code = `toplevel.Variant.beforeChangeKeys`
					res, err := rntm.Call(store, "", code)
					So(err, ShouldBeNil)
					So(res, ShouldEqual, "valuevalue")

					code = `toplevel.Variant.changeKeys`
					res, err = rntm.Call(store, "", code)
					So(err, ShouldBeNil)
					So(res, ShouldEqual, "valuevalue")
				})
			})
		})
	})
}

func TestTypeVariant(t *testing.T) {

	Convey("Loading dml code into runtime including type variant,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
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

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("The default complex type should be created", func() {

			store.Begin()
			defer store.Rollback()

			vari, err := getObjectFromPath(rntm, "toplevel.TypeVariant")
			So(err, ShouldBeNil)
			value, err := vari.obj.(*variant).GetValue(vari.id)
			So(err, ShouldBeNil)
			So(value, ShouldNotBeNil)

			dt, err := value.(dmlSet).obj.GetDataType(value.(dmlSet).id)
			So(err, ShouldBeNil)
			val, _ := vari.obj.GetProperty("type").GetValue(vari.id)
			So(dt, ShouldResemble, val)
		})

		Convey("Changing the datatype to a complex one should work", func() {

			code = `
				toplevel.Variant.type = new DataType("Data{property int test: 10}")
			`
			_, err := rntm.RunJavaScript(store, "user3", code)
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
				`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)
			})
		})
	})
}
