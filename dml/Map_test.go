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

//little helper
func getObjectFromPath(rntm *Runtime, path string) (dmlSet, error) {
	res, _, err := rntm.resolvePath(path)
	if err != nil {
		return dmlSet{}, err
	}
	set, ok := res.(dmlSet)
	if !ok {
		return dmlSet{}, newInternalError(Error_Operation_Invalid, "Path is not Object")
	}
	return set, nil
}

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

						property string beforeChangeKeys
						property string changeKeys

						.onBeforeChange: function(key) {
							this.beforeChangeKeys += key
						}
						.onChanged: function(key) {
							this.changeKeys += key
						}
					}

					Map {
						.name: "StringBoolMap"
						.key: string
						.value: bool
						
						property string beforeChangeKeys
						property string changeKeys

						.onBeforeChange: function(key) {
							this.beforeChangeKeys += key
						}
						.onChanged: function(key) {
							this.changeKeys += key
						}
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
		utils.PrintWithStacktrace(err)
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
			_, _, err = rntm.RunJavaScript(store, "", code)
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
			res, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 10)

			Convey("and the relevant events with keys have been emitted", func() {

				code := `toplevel.IntIntMap.Set(10, 11)`
				_, _, err = rntm.RunJavaScript(store, "", code)
				So(err, ShouldBeNil)

				code = `toplevel.IntIntMap.beforeChangeKeys`
				res, _, err := rntm.Call(store, "", code, args(), kwargs())
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "1010")

				code = `toplevel.IntIntMap.changeKeys`
				res, _, err = rntm.Call(store, "", code, args(), kwargs())
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "1010")
			})
		})

		Convey("Adding new values should work", func() {

			code := `toplevel.StringBoolMap.New("hey")`
			_, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `toplevel.StringBoolMap.New("hey")`
			_, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldNotBeNil)

			code = `toplevel.StringBoolMap.Get("hey")`
			res, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeFalse)

			code = `toplevel.StringBoolMap.Set("ho", true)`
			_, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)

			code = `toplevel.StringBoolMap.Get("ho")`
			res, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)

			code = `toplevel.StringBoolMap.Length()`
			res, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 2)

			code = `toplevel.StringBoolMap.Has("ho")`
			res, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeTrue)

			code = `toplevel.StringBoolMap.Has("klö")`
			res, _, err = rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			So(res, ShouldBeFalse)

			Convey("and the relevant events with keys have been emitted", func() {

				code = `toplevel.StringBoolMap.beforeChangeKeys`
				res, _, err := rntm.Call(store, "", code, args(), kwargs())
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "heyho")

				code = `toplevel.StringBoolMap.changeKeys`
				res, _, err = rntm.Call(store, "", code, args(), kwargs())
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "heyho")
			})
		})

		Convey("Deleting values must be supported", func() {

			code := `toplevel.IntIntMap.Set(10, 10)
					toplevel.IntIntMap.Set(9, 9)
					`
			_, _, err = rntm.RunJavaScript(store, "", code)
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
			_, _, err = rntm.RunJavaScript(store, "", code)
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
			_, _, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldBeNil)

			code = `toplevel.test("test2")`
			_, _, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldBeNil)

			code = `	toplevel.TypeMap.Set("new", toplevel.TypeMap.Get("test"))`
			_, _, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldNotBeNil) //setting complex objects should not be allowed (no doublication, clear hirarchy)

			Convey("Accessing the object via path is possible", func() {

				store.Begin()
				defer store.Rollback()

				path := "toplevel.TypeMap.test"
				set, err := getObjectFromPath(rntm, path)
				So(err, ShouldBeNil)
				val, err := set.obj.GetProperty("test").GetValue(set.id)
				So(err, ShouldBeNil)
				So(val, ShouldEqual, 0)

				Convey("and the same path is set in the object", func() {

					res, err := set.obj.GetObjectPath(set.id)
					So(err, ShouldBeNil)
					So(res, ShouldEqual, path)
				})
			})

			Convey("The created event was called", func() {

				store.Begin()
				defer store.Rollback()

				obj, err := getObjectFromPath(rntm, "toplevel.TypeMap.test")
				So(err, ShouldBeNil)
				val, _ := obj.obj.GetProperty("created").GetValue(obj.id)
				So(val, ShouldEqual, obj.id.Encode())
				store.Rollback()

				code = `
					obj = toplevel.TypeMap.Get("test")
					if (obj.identifier.Encode() != obj.created) {
						throw "identifiers are not equal, but should be"
					}
				`
				_, _, err = rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)
			})

			Convey("Setting data to the new type is supported", func() {

				code = `
					obj = toplevel.TypeMap.Get("test")
					obj.test = 1
					toplevel.TypeMap.Get("test2").test = 2
				`
				_, _, err := rntm.RunJavaScript(store, "user3", code)
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
				val, _ := set.obj.GetProperty("test").GetValue(set.id)
				So(val, ShouldEqual, 1)

				entry, err = vec.Get(child.id, "test2")
				So(err, ShouldBeNil)
				set = entry.(dmlSet)
				val, _ = set.obj.GetProperty("test").GetValue(set.id)
				So(val, ShouldEqual, 2)
			})

			Convey("The hirarchy is set correct", func() {

				code = `
					obj = toplevel.TypeMap.Get("test")
					if (!obj.parent.identifier.Equals(toplevel.TypeMap.identifier)) {
						throw "parent not set correctly"
					}
				`
				_, _, err := rntm.RunJavaScript(store, "user3", code)
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
				_, _, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)

			})
		})
	})
}
