package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/ickby/CollaborationNode/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODVector(t *testing.T) {

	Convey("Loading dml code into runtime including pod vector types,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)
		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"

					Vector {
						.name: "IntVec"
						.type: int
					}

					Vector {
						.name: "BoolVec"
						.type: bool
					}

					Vector {
						.name: "StringVec"
						.type: string
					}

					Vector {
						.name: "FloatVec"
						.type: float
					}
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Adding to int vector should work", func() {
			store.Begin()
			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "IntVec")
			intvec := child.obj.(*vector)
			length, err := intvec.Length(child.id)
			So(err, ShouldBeNil)
			So(length, ShouldEqual, 0)
			store.Rollback()
			code = `toplevel.IntVec.Append(0)`
			val, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			idx := val.(int64)
			So(idx, ShouldEqual, 0)
			store.Begin()
			length, _ = intvec.Length(child.id)
			store.Rollback()
			So(length, ShouldEqual, 1)
		})

		Convey("Setting and reading entries of vector shall work", func() {
			store.Begin()
			defer store.Commit()

			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "StringVec")
			strvec := child.obj.(*vector)
			val, err := strvec.AppendNew(child.id)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "")
			val, err = strvec.AppendNew(child.id)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "")
			length, _ := strvec.Length(child.id)
			So(length, ShouldEqual, 2)
			val, err = strvec.Get(child.id, 0)
			So(err, ShouldBeNil)
			str, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, "")

			So(strvec.Set(child.id, 1, "hello"), ShouldBeNil)
			val, err = strvec.Get(child.id, 1)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "hello")

			So(strvec.Set(child.id, 2, "test"), ShouldNotBeNil)
		})

		Convey("Creating a int vector", func() {

			store.Begin()
			defer store.Commit()

			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "IntVec")
			vec := child.obj.(*vector)
			vec.Append(child.id, 0)
			vec.Append(child.id, 1)
			vec.Append(child.id, 2)
			vec.Append(child.id, 3)
			vec.Append(child.id, 4)
			vec.Append(child.id, 5)
			vec.Append(child.id, 6)
			length, _ := vec.Length(child.id)
			So(length, ShouldEqual, 7)

			Convey("moving Values within the vector shall work", func() {

				//[0 1 2 3 4 5 6]
				// e.g. old: 2, new: 5 [0 1 3 4 5 2 6] (3->2, 4->3, 5->4, 2->5)
				So(vec.Move(child.id, 2, 5), ShouldBeNil)
				val, _ := vec.Get(child.id, 0)
				So(val, ShouldEqual, 0)
				val, _ = vec.Get(child.id, 1)
				So(val, ShouldEqual, 1)
				val, _ = vec.Get(child.id, 2)
				So(val, ShouldEqual, 3)
				val, _ = vec.Get(child.id, 3)
				So(val, ShouldEqual, 4)
				val, _ = vec.Get(child.id, 4)
				So(val, ShouldEqual, 5)
				val, _ = vec.Get(child.id, 5)
				So(val, ShouldEqual, 2)
				val, _ = vec.Get(child.id, 6)
				So(val, ShouldEqual, 6)
			})

			Convey("and deleting values from the vector must be supported", func() {

				main, err := rntm.getMainObjectSet()
				So(err, ShouldBeNil)
				child, _ := main.obj.(Data).GetChildByName(main.id, "IntVec")
				vec := child.obj.(*vector)
				So(vec.Remove(child.id, 1), ShouldBeNil)

				//[0 1 2 3 4 5 6] --> [0 2 3 4 5 6]
				length, _ := vec.Length(child.id)
				So(length, ShouldEqual, 6)
				val, _ := vec.Get(child.id, 0)
				So(val, ShouldEqual, 0)
				val, _ = vec.Get(child.id, 1)
				So(val, ShouldEqual, 2)
				val, _ = vec.Get(child.id, 5)
				So(val, ShouldEqual, 6)

				So(vec.Remove(child.id, 6), ShouldNotBeNil)
				So(vec.Remove(child.id, 5), ShouldBeNil)
				So(vec.Remove(child.id, 0), ShouldBeNil)
				So(vec.Remove(child.id, 0), ShouldBeNil)
				So(vec.Remove(child.id, 0), ShouldBeNil)
				So(vec.Remove(child.id, 0), ShouldBeNil)
				So(vec.Remove(child.id, 0), ShouldBeNil)

				length, _ = vec.Length(child.id)
				So(length, ShouldEqual, 0)
			})

			Convey("inserting works on each position (start, end , inbetween)", func() {

				main, err := rntm.getMainObjectSet()
				So(err, ShouldBeNil)
				child, _ := main.obj.(Data).GetChildByName(main.id, "FloatVec")
				vec := child.obj.(*vector)

				//insert should work only at existing indices
				So(vec.Insert(child.id, 0, 9.5), ShouldNotBeNil)
				vec.AppendNew(child.id)                       //[0]
				So(vec.Insert(child.id, 0, 0.5), ShouldBeNil) //[0.5 0]
				val, err := vec.InsertNew(child.id, 0)        //[0 0.5 0]
				So(err, ShouldBeNil)
				So(val, ShouldAlmostEqual, 0.0)

				So(vec.Insert(child.id, 1, 7.1), ShouldBeNil) //[0 7.1 0.5 0]

				length, _ := vec.Length(child.id)
				So(length, ShouldEqual, 4)
				val, _ = vec.Get(child.id, 0)
				So(val, ShouldEqual, 0)
				val, _ = vec.Get(child.id, 1)
				So(val, ShouldAlmostEqual, 7.1)
				val, _ = vec.Get(child.id, 2)
				So(val, ShouldAlmostEqual, 0.5)
				val, _ = vec.Get(child.id, 3)
				So(val, ShouldAlmostEqual, 0.0)

				Convey("and swapping entries is a thing", func() {

					main, err := rntm.getMainObjectSet()
					So(err, ShouldBeNil)
					child, _ := main.obj.(Data).GetChildByName(main.id, "FloatVec")
					vec := child.obj.(*vector)
					vec.Swap(child.id, 0, 2)

					length, _ := vec.Length(child.id)
					So(length, ShouldEqual, 4)
					val, _ := vec.Get(child.id, 0)
					So(val, ShouldAlmostEqual, 0.5)
					val, _ = vec.Get(child.id, 1)
					So(val, ShouldAlmostEqual, 7.1)
					val, _ = vec.Get(child.id, 2)
					So(val, ShouldAlmostEqual, 0)
					val, _ = vec.Get(child.id, 3)
					So(val, ShouldAlmostEqual, 0.0)
				})
			})
			Convey("Creating a new Runtime with the existing datastore", func() {

				rntm2 := NewRuntime()
				err = rntm2.Parse(strings.NewReader(code))
				So(err, ShouldBeNil)

				rntm2.datastore = store //set internal datastore as we do not use Runtime API

				Convey("The vector should be setup correctly", func() {

					main, err := rntm2.getMainObjectSet()
					So(err, ShouldBeNil)
					child, _ := main.obj.(Data).GetChildByName(main.id, "IntVec")
					vec := child.obj.(*vector)

					//[0 1 2 3 4 5 6]
					length, _ := vec.Length(child.id)
					So(length, ShouldEqual, 7)
					So(vec.Move(child.id, 2, 5), ShouldBeNil)
					val, _ := vec.Get(child.id, 0)
					So(val, ShouldEqual, 0)
					val, _ = vec.Get(child.id, 1)
					So(val, ShouldEqual, 1)
					val, _ = vec.Get(child.id, 2)
					So(val, ShouldEqual, 3)
					val, _ = vec.Get(child.id, 3)
					So(val, ShouldEqual, 4)
					val, _ = vec.Get(child.id, 4)
					So(val, ShouldEqual, 5)
					val, _ = vec.Get(child.id, 5)
					So(val, ShouldEqual, 2)
					val, _ = vec.Get(child.id, 6)
					So(val, ShouldEqual, 6)
				})
			})
		})
	})
}

func TestTypeVector(t *testing.T) {

	Convey("Loading dml code into runtime including pod vector types,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
					.name:"toplevel"

					Vector {
						.name: "TypeVec"
						.type: Data {
							property int test: 0
						}
					}
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Adding to type vector should work", func() {

			store.Begin()
			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "TypeVec")
			vec := child.obj.(*vector)
			length, _ := vec.Length(child.id)
			So(length, ShouldEqual, 0)
			store.Commit()

			code = `toplevel.TypeVec.AppendNew()
					toplevel.TypeVec.AppendNew()
			`
			_, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldBeNil)

			store.Begin()
			main, err = rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ = main.obj.(Data).GetChildByName(main.id, "TypeVec")
			vec = child.obj.(*vector)
			length, _ = vec.Length(child.id)
			So(length, ShouldEqual, 2)
			store.Commit()

			Convey("Setting data to the new type is supported", func() {

				code = `
					obj = toplevel.TypeVec.Get(0)
					obj.test = 1
				`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)

				store.Begin()
				defer store.Rollback()

				main, err := rntm.getMainObjectSet()
				So(err, ShouldBeNil)
				child, _ := main.obj.(Data).GetChildByName(main.id, "TypeVec")
				vec := child.obj.(*vector)

				entry, err := vec.Get(child.id, 0)
				So(err, ShouldBeNil)
				obj := entry.(dmlSet).obj.(Object)

				So(obj.GetProperty("test").GetValue(entry.(dmlSet).id), ShouldEqual, 1)

				Convey("and accessing new set value via path works", func() {

					set, err := rntm.getObjectFromPath("toplevel.TypeVec.0")
					So(err, ShouldBeNil)

					So(set.obj.GetProperty("test").GetValue(set.id), ShouldEqual, 1)
				})
			})

			Convey("but setting complex types is forbidden", func() {

				code = `	toplevel.TypeVec.Set(0, toplevel.TypeVec.Get(1))`
				_, err = rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldNotBeNil) //setting complex objects should not be allowed (no doublication, clear hirarchy)

			})

			Convey("The hirarchy is set correct", func() {

				code = `
						obj = toplevel.TypeVec.Get(0)
						if (!obj.parent.identifier.Equals(toplevel.TypeVec.identifier)) {
							throw "parent not set correctly"
						}
					`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)
			})

			Convey("Removing the object works", func() {

				code = `
						var l =  toplevel.TypeVec.Length()
						toplevel.TypeVec.Remove(0)
						if (l != toplevel.TypeVec.Length()+1) {
							throw "Object removal failed"
						}
					`
				_, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)

			})
		})
	})
}
