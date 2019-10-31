package dml

import (
	datastore "github.com/ickby/CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODMap(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including pod map types,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		dmlcode := `Data {
					.name:"toplevel"
					
					Map {
						.name: "IntIntMap"
						.key: int
						.value: int
					}
									
					Map {
						.name: "StringBoolMap"
						.key: string
						.value: bool
					}
									
					Map {
						.name: "IntFloatMap"
						.key: int
						.value: float
					}					
				}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(dmlcode))
		So(err, ShouldBeNil)

		Convey("Adding to IntInt map should work", func() {
			store.Begin()
			child, _ := rntm.mainObj.GetChildByName("IntIntMap")
			intmap := child.(*mapImpl)
			length, err := intmap.Length()
			So(err, ShouldBeNil)
			So(length, ShouldEqual, 0)
			has, err := intmap.Has(10)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
			store.Rollback()

			code := `toplevel.IntIntMap.Set(10, 10)`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			store.Begin()
			length, err = intmap.Length()
			So(err, ShouldBeNil)
			has, err = intmap.Has(10)
			So(err, ShouldBeNil)
			store.Rollback()
			So(length, ShouldEqual, 1)
			So(has, ShouldBeTrue)

			code = `toplevel.IntIntMap.Get(10)`
			res, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 10)
		})

		Convey("Adding new values should work", func() {

			code := `toplevel.StringBoolMap.New("hey")`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			code = `toplevel.StringBoolMap.New("hey")`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldNotBeNil)

			code = `toplevel.StringBoolMap.Get("hey")`
			res, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeFalse)

			code = `toplevel.StringBoolMap.Set("ho", true)`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			code = `toplevel.StringBoolMap.Get("ho")`
			res, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)

			code = `toplevel.StringBoolMap.Length()`
			res, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 2)

			code = `toplevel.StringBoolMap.Has("ho")`
			res, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)

			code = `toplevel.StringBoolMap.Has("klö")`
			res, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeFalse)
		})

		Convey("Deleting values must be supported", func() {

			code := `toplevel.IntIntMap.Set(10, 10)
					toplevel.IntIntMap.Set(9, 9)
					`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			store.Begin()
			defer store.Commit()

			child, _ := rntm.mainObj.GetChildByName("IntIntMap")
			m := child.(*mapImpl)
			So(m.Remove(11), ShouldNotBeNil)
			length, _ := m.Length()
			So(length, ShouldEqual, 2)
			So(m.Remove(10), ShouldBeNil)
			length, _ = m.Length()
			So(length, ShouldEqual, 1)
			So(m.Remove(9), ShouldBeNil)
			length, _ = m.Length()
			So(length, ShouldEqual, 0)
		})

		Convey("Creating a new Runtime with the existing datastore", func() {

			code := `toplevel.IntFloatMap.Set(10, 5.5);
				    toplevel.IntFloatMap.Set(9, 4.4);
					`
			_, err = rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)

			rntm2 := NewRuntime(store)
			err = rntm2.Parse(strings.NewReader(dmlcode))
			So(err, ShouldBeNil)

			Convey("The vector should be setup correctly", func() {

				store.Begin()
				defer store.Rollback()

				child, _ := rntm2.mainObj.GetChildByName("IntFloatMap")
				m := child.(*mapImpl)

				length, _ := m.Length()
				So(length, ShouldEqual, 2)
				val, _ := m.Get(10)
				So(val, ShouldAlmostEqual, 5.5)
				val, _ = m.Get(9)
				So(val, ShouldAlmostEqual, 4.4)
			})
		})

	})
}

func TestComplexTypeMap(t *testing.T) {

	Convey("Loading dml code into runtime including complex map types,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"

					Map {
						.name: "TypeMap"
						.key: string
						.value: Data {
							property int test: 0
						}
					}

					function test(key) {
						return this.TypeMap.New(key)
					}
				}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to type map should work", func() {

			store.Begin()
			child, _ := rntm.mainObj.GetChildByName("TypeMap")
			vec := child.(*mapImpl)
			length, _ := vec.Length()		
			So(length, ShouldEqual, 0)
			store.Commit()

			code = `toplevel.TypeMap.New("test")`
			_, err := rntm.RunJavaScript("user3", code)
			So(err, ShouldBeNil)

			code = `toplevel.test("test2")`
			_, err = rntm.RunJavaScript("user3", code)
			So(err, ShouldBeNil)

			code = `obj = new Object(toplevel.TypeMap.value)
					toplevel.TypeMap.Set("new", obj)`

			_, err = rntm.RunJavaScript("user3", code)
			So(err, ShouldBeNil)

			Convey("Setting data to the new type is supported", func() {

				code = `
					obj = toplevel.TypeMap.Get("test")
					obj.test = 1
					toplevel.TypeMap.Get("new").test = 2
				`
				_, err := rntm.RunJavaScript("user3", code)
				So(err, ShouldBeNil)

				store.Begin()
				defer store.Rollback()

				child, _ := rntm.mainObj.GetChildByName("TypeMap")
				vec := child.(*mapImpl)

				entry, err := vec.Get("test")
				So(err, ShouldBeNil)
				obj := entry.(Object)
				So(obj.GetProperty("test").GetValue(), ShouldEqual, 1)

				entry, err = vec.Get("new")
				So(err, ShouldBeNil)
				obj = entry.(Object)
				So(obj.GetProperty("test").GetValue(), ShouldEqual, 2)
			})
			
			Convey("And accessing the object via path is possible", func() {
	
				store.Begin()
				defer store.Rollback()
				
				obj, err := rntm.getObjectFromPath("toplevel.TypeMap.test")
				So(err, ShouldBeNil)
				So(obj.GetProperty("test").GetValue(), ShouldEqual, 0)
			})
		})
	})
}
