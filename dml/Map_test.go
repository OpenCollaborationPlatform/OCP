package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/ickby/CollaborationNode/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODMap(t *testing.T) {

	Convey("Loading dml code into runtime including pod map types,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
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

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(dmlcode))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Adding to IntInt map should work", func() {
			store.Begin()
			rntm.datastore = store //set interal datastore as we do not use Runtime API
			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "IntIntMap")
			intmap := child.obj.(*mapImpl)
			length, err := intmap.Length(child.id)
			So(err, ShouldBeNil)
			So(length, ShouldEqual, 0)
			has, err := intmap.Has(child.id, 10)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
			store.Rollback()

			code := `toplevel.IntIntMap.Set(10, 10)`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			store.Begin()
			length, err = intmap.Length(child.id)
			So(err, ShouldBeNil)
			has, err = intmap.Has(child.id, 10)
			So(err, ShouldBeNil)
			store.Rollback()
			So(length, ShouldEqual, 1)
			So(has, ShouldBeTrue)

			code = `toplevel.IntIntMap.Get(10)`
			res, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 10)
		})

		Convey("Adding new values should work", func() {

			code := `toplevel.StringBoolMap.New("hey")`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `toplevel.StringBoolMap.New("hey")`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)

			code = `toplevel.StringBoolMap.Get("hey")`
			res, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeFalse)

			code = `toplevel.StringBoolMap.Set("ho", true)`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `toplevel.StringBoolMap.Get("ho")`
			res, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)

			code = `toplevel.StringBoolMap.Length()`
			res, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 2)

			code = `toplevel.StringBoolMap.Has("ho")`
			res, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)

			code = `toplevel.StringBoolMap.Has("klö")`
			res, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeFalse)
		})

		Convey("Deleting values must be supported", func() {

			code := `toplevel.IntIntMap.Set(10, 10)
					toplevel.IntIntMap.Set(9, 9)
					`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			store.Begin()
			defer store.Commit()

			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "IntIntMap")
			m := child.obj.(*mapImpl)
			So(m.Remove(child.id, 11), ShouldNotBeNil)
			length, _ := m.Length(child.id)
			So(length, ShouldEqual, 2)
			So(m.Remove(child.id, 10), ShouldBeNil)
			length, _ = m.Length(child.id)
			So(length, ShouldEqual, 1)
			So(m.Remove(child.id, 9), ShouldBeNil)
			length, _ = m.Length(child.id)
			So(length, ShouldEqual, 0)
		})

		Convey("Creating a new Runtime with the existing datastore", func() {

			code := `toplevel.IntFloatMap.Set(10, 5.5);
				    toplevel.IntFloatMap.Set(9, 4.4);
					`
			_, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			rntm2 := NewRuntime()
			err = rntm2.Parse(strings.NewReader(dmlcode))
			So(err, ShouldBeNil)

			rntm2.datastore = store //set interal datastore as we do not use Runtime API

			Convey("The vector should be setup correctly", func() {

				store.Begin()
				defer store.Rollback()

				main, err := rntm2.getMainObjectSet()
				So(err, ShouldBeNil)
				child, _ := main.obj.(Data).GetChildByName(main.id, "IntFloatMap")
				m := child.obj.(*mapImpl)

				length, _ := m.Length(child.id)
				So(length, ShouldEqual, 2)
				val, _ := m.Get(child.id, 10)
				So(val, ShouldAlmostEqual, 5.5)
				val, _ = m.Get(child.id, 9)
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
							property string created

							.onCreated: function() {
								this.created = this.identifier.Encode()
							}
						}
					}

					function test(key) {
						return this.TypeMap.New(key)
					}
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Adding to type map should work", func() {

			store.Begin()
			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "TypeMap")
			vec := child.obj.(*mapImpl)
			length, _ := vec.Length(child.id)
			So(length, ShouldEqual, 0)
			store.Commit()

			code = `toplevel.TypeMap.New("test")`
			_, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldBeNil)

			code = `toplevel.test("test2")`
			_, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldBeNil)

			code = `	toplevel.TypeMap.Set("new", toplevel.TypeMap.Get("test"))`
			_, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldNotBeNil) //setting complex objects should not be allowed (no doublication, clear hirarchy)

			Convey("The created event was called", func() {

				store.Begin()
				defer store.Rollback()

				obj, err := rntm.getObjectFromPath("toplevel.TypeMap.test")
				So(err, ShouldBeNil)
				So(obj.obj.GetProperty("created").GetValue(obj.id), ShouldEqual, obj.id.Encode())
				store.Rollback()

				code = `
					obj = toplevel.TypeMap.Get("test")
					if (obj.identifier.Encode() != obj.created) {
						throw "identifiers are not equal, but should be"
					}
				`
				_, err = rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)
			})

			Convey("Setting data to the new type is supported", func() {

				code = `
					obj = toplevel.TypeMap.Get("test")
					obj.test = 1
					toplevel.TypeMap.Get("test2").test = 2
				`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)

				store.Begin()
				defer store.Rollback()

				main, err := rntm.getMainObjectSet()
				So(err, ShouldBeNil)
				child, _ := main.obj.(Data).GetChildByName(main.id, "TypeMap")
				vec := child.obj.(*mapImpl)

				entry, err := vec.Get(child.id, "test")
				So(err, ShouldBeNil)
				set := entry.(dmlSet)
				So(set.obj.GetProperty("test").GetValue(set.id), ShouldEqual, 1)

				entry, err = vec.Get(child.id, "test2")
				So(err, ShouldBeNil)
				set = entry.(dmlSet)
				So(set.obj.GetProperty("test").GetValue(set.id), ShouldEqual, 2)
			})

			Convey("And accessing the object via path is possible", func() {

				store.Begin()
				defer store.Rollback()

				set, err := rntm.getObjectFromPath("toplevel.TypeMap.test")
				So(err, ShouldBeNil)
				So(set.obj.GetProperty("test").GetValue(set.id), ShouldEqual, 0)
			})

			Convey("The hirarchy is set correctl", func() {

				code = `
					obj = toplevel.TypeMap.Get("test")
					if (!obj.parent.identifier.Equals(toplevel.TypeMap.identifier)) {
						throw "parent not set correctly"
					}
				`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)
			})

			Convey("Removing the object works", func() {

				code = `
					l = toplevel.TypeMap.Length()
					toplevel.TypeMap.Remove("test")
					if (l != toplevel.TypeMap.Length() + 1) {
						throw "Object removal did not shorten the map"
					}
				`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)

			})
		})
	})
}
