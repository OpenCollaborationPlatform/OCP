package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

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
						
						property string beforeChangeKeys
						property string changeKeys

						.onBeforeChange: function(key) {
							this.beforeChangeKeys += key
						}
						.onChanged: function(key) {
							this.changeKeys += key
						}
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
			val, _, err := rntm.RunJavaScript(store, "", code)
			So(err, ShouldBeNil)
			idx := val.(int64)
			So(idx, ShouldEqual, 0)
			store.Begin()
			length, _ = intvec.Length(child.id)
			store.Rollback()
			So(length, ShouldEqual, 1)

			Convey("and the relevant events with keys have been emitted", func() {

				code = `toplevel.IntVec.beforeChangeKeys`
				res, _, err := rntm.Call(store, "", code, args(), kwargs())
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "0")

				code = `toplevel.IntVec.changeKeys`
				res, _, err = rntm.Call(store, "", code, args(), kwargs())
				So(err, ShouldBeNil)
				So(res, ShouldEqual, "0")
			})
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

			//emits events
			res, id, _ := rntm.resolvePath(`toplevel.IntVec.beforeChangeKeys`)
			val, _ := res.(Property).GetValue(id)
			So(val, ShouldEqual, "0123456")
			res, id, _ = rntm.resolvePath(`toplevel.IntVec.changeKeys`)
			val, _ = res.(Property).GetValue(id)
			So(val, ShouldEqual, "0123456")

			Convey("moving Values within the vector shall work", func() {

				p, i, _ := rntm.resolvePath(`toplevel.IntVec.beforeChangeKeys`)
				p.(Property).SetValue(i, "")
				p, i, _ = rntm.resolvePath(`toplevel.IntVec.changeKeys`)
				p.(Property).SetValue(i, "")

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

				//emits events
				res, id, _ := rntm.resolvePath(`toplevel.IntVec.beforeChangeKeys`)
				val, _ = res.(Property).GetValue(id)
				So(val, ShouldEqual, "5234")
				res, id, _ = rntm.resolvePath(`toplevel.IntVec.changeKeys`)
				val, _ = res.(Property).GetValue(id)
				So(val, ShouldEqual, "2345")

			})

			Convey("and deleting values from the vector must be supported", func() {

				p, i, _ := rntm.resolvePath(`toplevel.IntVec.beforeChangeKeys`)
				p.(Property).SetValue(i, "")
				p, i, _ = rntm.resolvePath(`toplevel.IntVec.changeKeys`)
				p.(Property).SetValue(i, "")

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

				//emits events
				res, id, _ := rntm.resolvePath(`toplevel.IntVec.beforeChangeKeys`)
				val, _ = res.(Property).GetValue(id)
				So(val, ShouldEqual, "123456")
				res, id, _ = rntm.resolvePath(`toplevel.IntVec.changeKeys`)
				val, _ = res.(Property).GetValue(id)
				So(val, ShouldEqual, "123456")

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

		Convey("Inserting works on each position (start, end , inbetween) for float vector", func() {

			store.Begin()
			defer store.Commit()

			main, err := rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ := main.obj.(Data).GetChildByName(main.id, "FloatVec")
			vec := child.obj.(*vector)

			//insert should work only at existing indices
			So(vec.Insert(child.id, 0, 9.5), ShouldNotBeNil)
			vec.AppendNew(child.id)                       //[0]
			So(vec.Insert(child.id, 0, 0.5), ShouldBeNil) //[0.5 0]

			val, err := vec.InsertNew(child.id, 0) //[0 0.5 0]
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

			//emits events
			res, id, _ := rntm.resolvePath(`toplevel.FloatVec.beforeChangeKeys`)
			val, _ = res.(Property).GetValue(id)
			So(val, ShouldEqual, "001021132")
			res, id, _ = rntm.resolvePath(`toplevel.FloatVec.changeKeys`)
			val, _ = res.(Property).GetValue(id)
			So(val, ShouldEqual, "010210321")

			Convey("and swapping entries is a thing", func() {

				p, i, _ := rntm.resolvePath(`toplevel.FloatVec.beforeChangeKeys`)
				p.(Property).SetValue(i, "")
				p, i, _ = rntm.resolvePath(`toplevel.FloatVec.changeKeys`)
				p.(Property).SetValue(i, "")

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
			_, _, err = rntm.RunJavaScript(store, "user3", code)
			So(err, ShouldBeNil)

			store.Begin()
			main, err = rntm.getMainObjectSet()
			So(err, ShouldBeNil)
			child, _ = main.obj.(Data).GetChildByName(main.id, "TypeVec")
			vec = child.obj.(*vector)
			length, _ = vec.Length(child.id)
			So(length, ShouldEqual, 2)
			store.Commit()

			Convey("Accessing the object via path is possible", func() {

				store.Begin()
				defer store.Rollback()

				path := "toplevel.TypeVec.0"
				set, err := getObjectFromPath(rntm, path)
				So(err, ShouldBeNil)
				val, _ := set.obj.GetProperty("test").GetValue(set.id)
				So(val, ShouldEqual, 0)

				Convey("and the same path is set in the object", func() {

					res, err := set.obj.GetObjectPath(set.id)
					So(err, ShouldBeNil)
					So(res, ShouldEqual, path)
				})
			})

			Convey("Setting data to the new type is supported", func() {

				code = `
					obj = toplevel.TypeVec.Get(0)
					obj.test = 1
				`
				_, _, err := rntm.RunJavaScript(store, "user3", code)
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

				val, _ := obj.GetProperty("test").GetValue(entry.(dmlSet).id)
				So(val, ShouldEqual, 1)

				Convey("and accessing new set value via path works", func() {

					set, err := getObjectFromPath(rntm, "toplevel.TypeVec.0")
					So(err, ShouldBeNil)
					val, _ = set.obj.GetProperty("test").GetValue(set.id)
					So(val, ShouldEqual, 1)
				})
			})

			Convey("but setting complex types is forbidden", func() {

				code = `	toplevel.TypeVec.Set(0, toplevel.TypeVec.Get(1))`
				_, _, err = rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldNotBeNil) //setting complex objects should not be allowed (no doublication, clear hirarchy)

			})

			Convey("The hirarchy is set correct", func() {

				code = `
						obj = toplevel.TypeVec.Get(0)
						if (!obj.parent.identifier.Equals(toplevel.TypeVec.identifier)) {
							throw "parent not set correctly"
						}
					`
				_, _, err := rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)
			})

			Convey("Removing the object works", func() {

				store.Begin()
				path := "toplevel.TypeVec.1"
				set, err := getObjectFromPath(rntm, path)
				So(err, ShouldBeNil)
				store.Rollback()

				code = `
						var l =  toplevel.TypeVec.Length()
						toplevel.TypeVec.Remove(0)
						if (l != toplevel.TypeVec.Length()+1) {
							throw "Object removal failed"
						}
					`
				_, _, err = rntm.RunJavaScript(store, "user3", code)
				So(err, ShouldBeNil)

				store.Begin()
				path = "toplevel.TypeVec.0"
				newSet, err := getObjectFromPath(rntm, path)
				So(err, ShouldBeNil)
				So(newSet.id.Equals(set.id), ShouldBeTrue)
				store.Rollback()

				Convey("and the objects path is updated accordingly", func() {

					store.Begin()
					defer store.Rollback()
					objPath, err := set.obj.GetObjectPath(set.id)
					So(err, ShouldBeNil)
					So(objPath, ShouldEqual, path)
				})
			})

			Convey("Swaping works and updates the paths", func() {

				store.Begin()
				defer store.Rollback()
				setV, _ := getObjectFromPath(rntm, "toplevel.TypeVec")

				set0, _ := getObjectFromPath(rntm, "toplevel.TypeVec.0")
				path0, _ := set0.obj.GetObjectPath(set0.id)
				set1, _ := getObjectFromPath(rntm, "toplevel.TypeVec.1")
				path1, _ := set1.obj.GetObjectPath(set1.id)

				vec := setV.obj.(*vector)
				So(vec.Swap(setV.id, 0, 1), ShouldBeNil)

				//new sets after swap
				setS0, _ := getObjectFromPath(rntm, "toplevel.TypeVec.0")
				setS1, _ := getObjectFromPath(rntm, "toplevel.TypeVec.1")

				//new paths after swap of the old objects (Note: not new sets)
				pathS0, _ := set0.obj.GetObjectPath(set0.id)
				pathS1, _ := set1.obj.GetObjectPath(set1.id)

				So(set0.id.Equals(setS1.id), ShouldBeTrue)
				So(set1.id.Equals(setS0.id), ShouldBeTrue)
				So(path0, ShouldEqual, pathS1)
				So(path1, ShouldEqual, pathS0)
			})

			Convey("Moving works and updates the paths", func() {

				store.Begin()
				defer store.Rollback()
				setV, _ := getObjectFromPath(rntm, "toplevel.TypeVec")

				vec := setV.obj.(*vector)
				//[0 1 2 3]
				// e.g. old: 1, new: 2 [0 2 1 3]
				vec.AppendNew(setV.id) //2
				vec.AppendNew(setV.id) //3
				set0, _ := getObjectFromPath(rntm, "toplevel.TypeVec.0")
				path0, _ := set0.obj.GetObjectPath(set0.id)
				set1, _ := getObjectFromPath(rntm, "toplevel.TypeVec.1")
				path1, _ := set1.obj.GetObjectPath(set1.id)
				set2, _ := getObjectFromPath(rntm, "toplevel.TypeVec.2")
				path2, _ := set2.obj.GetObjectPath(set2.id)
				set3, _ := getObjectFromPath(rntm, "toplevel.TypeVec.3")
				path3, _ := set3.obj.GetObjectPath(set3.id)
				So(vec.Move(child.id, 1, 2), ShouldBeNil)

				//paths after move
				pathS0, _ := set0.obj.GetObjectPath(set0.id)
				pathS1, _ := set1.obj.GetObjectPath(set1.id)
				pathS2, _ := set2.obj.GetObjectPath(set2.id)
				pathS3, _ := set3.obj.GetObjectPath(set3.id)

				So(path0, ShouldEqual, pathS0)
				So(path1, ShouldEqual, pathS2)
				So(path2, ShouldEqual, pathS1)
				So(path3, ShouldEqual, pathS3)
			})
		})
	})
}
