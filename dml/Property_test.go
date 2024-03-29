package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTypeProperty(t *testing.T) {

	Convey("Loading dml code into runtime including a type property,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
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

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("The property must be accessbile", func() {

			store.Begin()
			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			So(main.obj.HasProperty("test"), ShouldBeTrue)
			val, err := main.obj.GetProperty("test").GetValue(main.id)
			So(err, ShouldBeNil)
			dt, ok := val.(DataType)
			So(ok, ShouldBeTrue)
			store.Rollback()

			val, _, err = rntm.RunJavaScript(store, "", "toplevel.test")
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
				_, _, err := rntm.RunJavaScript(store, "", "toplevel.test")
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
			_, _, err := rntm.RunJavaScript(store, "", code)
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
			_, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
		})

	})
}

func TestVarProperty(t *testing.T) {

	Convey("Loading dml code into runtime including a var property,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
				.name:"toplevel"

				const property var constVarProp: "Hello"
				property var varProp: 2
			}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("The properties must be accessbile", func() {

			res, _, err := rntm.Call(store, "", "toplevel.constVarProp", args(), kwargs())
			So(err, ShouldBeNil)
			So(res, ShouldEqual, "Hello")
			res, _, err = rntm.Call(store, "", "toplevel.varProp", args(), kwargs())
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 2)
		})

		Convey("and non-const var properties can be changed at runtime", func() {
			code := `toplevel.varProp = true
					if (toplevel.varProp != true) {
						throw "not boolean, but should be"
					}`
			_, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
		})

		Convey("but const var properties canot", func() {
			code := `toplevel.constVarProp = true
					`
			_, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestRawProperty(t *testing.T) {

	Convey("Loading dml code into runtime including a raw property,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
				.name:"toplevel"

				property raw rawProp
			}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("The default value must be undefined Cid", func() {

			res, _, err := rntm.Call(store, "", "toplevel.rawProp", args(), kwargs())
			utils.PrintWithStacktrace(err)
			So(err, ShouldBeNil)
			id, ok := res.(utils.Cid)
			So(ok, ShouldBeTrue)
			So(id.Defined(), ShouldBeFalse)
		})
	})
}

func TestPropertyTypeIdentification(t *testing.T) {

	Convey("Loading dml code into runtime including a raw property,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"
	
					property raw  	rawProp
					property type 	typeProp
					property string	stringProp
					property bool 	boolProp
					property var 	varProp 
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("The default values must be identifiable in JavaSript", func() {

			code := `
				if (typeof toplevel.stringProp == "object") {
					throw "is object"
				}
			`
			_, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
		})
	})
}

//Object.prototype.toString.call(t)
