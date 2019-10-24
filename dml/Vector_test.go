package dml

import (
	datastore "github.com/ickby/CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPODVector(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including pod vector types,", t, func() {

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

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to int vector should work", func() {
			store.Begin()
			child, _ := rntm.mainObj.GetChildByName("IntVec")
			intvec := child.(*vector)
			length, err := intvec.Length()
			So(err, ShouldBeNil)
			So(length, ShouldEqual, 0)
			store.Rollback()
			code = `toplevel.IntVec.Append(0)`
			val, err := rntm.RunJavaScript("", code)
			So(err, ShouldBeNil)
			idx := val.(int64)
			So(idx, ShouldEqual, 0)
			store.Begin()
			length, _ = intvec.Length()
			store.Rollback()
			So(length, ShouldEqual, 1)
		})

		Convey("Setting and reading entries of vector shall work", func() {
			store.Begin()
			defer store.Commit()

			child, _ := rntm.mainObj.GetChildByName("StringVec")
			strvec := child.(*vector)
			val, err := strvec.AppendNew()
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "")
			val, err = strvec.AppendNew()
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "")
			length, _ := strvec.Length()
			So(length, ShouldEqual, 2)
			val, err = strvec.Get(0)
			So(err, ShouldBeNil)
			str, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldEqual, "")

			So(strvec.Set(1, "hello"), ShouldBeNil)
			val, err = strvec.Get(1)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "hello")

			So(strvec.Set(2, "test"), ShouldNotBeNil)
		})

		Convey("Moving Values within the vector shall work", func() {

			store.Begin()
			defer store.Commit()

			child, _ := rntm.mainObj.GetChildByName("IntVec")
			vec := child.(*vector)
			vec.Set(0, 0)
			vec.Append(1)
			vec.Append(2)
			vec.Append(3)
			vec.Append(4)
			vec.Append(5)
			vec.Append(6)
			length, _ := vec.Length()
			So(length, ShouldEqual, 7)

			//[0 1 2 3 4 5 6]
			// e.g. old: 2, new: 5 [0 1 3 4 5 2 6] (3->2, 4->3, 5->4, 2->5)
			// e.g. old: 5, new: 2 [0 1 5 2 3 4 6] (2->3, 3->4, 4->5, 5->2)
			So(vec.Move(2, 5), ShouldBeNil)
			val, _ := vec.Get(0)
			So(val, ShouldEqual, 0)
			val, _ = vec.Get(1)
			So(val, ShouldEqual, 1)
			val, _ = vec.Get(2)
			So(val, ShouldEqual, 3)
			val, _ = vec.Get(3)
			So(val, ShouldEqual, 4)
			val, _ = vec.Get(4)
			So(val, ShouldEqual, 5)
			val, _ = vec.Get(5)
			So(val, ShouldEqual, 2)
			val, _ = vec.Get(6)
			So(val, ShouldEqual, 6)
		})

		Convey("Deleting values must be supported", func() {

			store.Begin()
			defer store.Commit()

			child, _ := rntm.mainObj.GetChildByName("IntVec")
			vec := child.(*vector)
			So(vec.Remove(1), ShouldBeNil)

			length, _ := vec.Length()
			So(length, ShouldEqual, 6)
			val, _ := vec.Get(0)
			So(val, ShouldEqual, 0)
			val, _ = vec.Get(1)
			So(val, ShouldEqual, 3)
			val, _ = vec.Get(5)
			So(val, ShouldEqual, 6)

			So(vec.Remove(6), ShouldNotBeNil)
			So(vec.Remove(5), ShouldBeNil)
			So(vec.Remove(0), ShouldBeNil)
			So(vec.Remove(0), ShouldBeNil)
			So(vec.Remove(0), ShouldBeNil)
			So(vec.Remove(0), ShouldBeNil)
			So(vec.Remove(0), ShouldBeNil)

			length, _ = vec.Length()
			So(length, ShouldEqual, 0)
		})

		Convey("Inserting works on each position (start, end , inbetween)", func() {

			store.Begin()
			defer store.Commit()

			child, _ := rntm.mainObj.GetChildByName("FloatVec")
			vec := child.(*vector)

			//insert should work only at existing indices
			So(vec.Insert(0, 9.5), ShouldNotBeNil)
			vec.AppendNew()                     //[0]
			So(vec.Insert(0, 0.5), ShouldBeNil) //[0.5 0]
			val, err := vec.InsertNew(0)        //[0 0.5 0]
			So(err, ShouldBeNil)
			So(val, ShouldAlmostEqual, 0.0)

			So(vec.Insert(1, 7.1), ShouldBeNil) //[0 7.1 0.5 0]

			length, _ := vec.Length()
			So(length, ShouldEqual, 4)
			val, _ = vec.Get(0)
			So(val, ShouldEqual, 0)
			val, _ = vec.Get(1)
			So(val, ShouldAlmostEqual, 7.1)
			val, _ = vec.Get(2)
			So(val, ShouldAlmostEqual, 0.5)
			val, _ = vec.Get(3)
			So(val, ShouldAlmostEqual, 0.0)
		})

		Convey("And swapping entries is a thing", func() {

			store.Begin()
			defer store.Commit()

			child, _ := rntm.mainObj.GetChildByName("FloatVec")
			vec := child.(*vector)
			vec.Swap(0, 2)

			length, _ := vec.Length()
			So(length, ShouldEqual, 4)
			val, _ := vec.Get(0)
			So(val, ShouldAlmostEqual, 0.5)
			val, _ = vec.Get(1)
			So(val, ShouldAlmostEqual, 7.1)
			val, _ = vec.Get(2)
			So(val, ShouldAlmostEqual, 0)
			val, _ = vec.Get(3)
			So(val, ShouldAlmostEqual, 0.0)
		})

		Convey("Creating a new Runtime with the existing datastore", func() {

			rntm2 := NewRuntime(store)
			err = rntm2.Parse(strings.NewReader(code))
			So(err, ShouldBeNil)

			Convey("The vector should be setup correctly", func() {

				store.Begin()
				defer store.Rollback()

				child, _ := rntm2.mainObj.GetChildByName("FloatVec")
				vec := child.(*vector)

				length, _ := vec.Length()
				So(length, ShouldEqual, 4)
				val, _ := vec.Get(0)
				So(val, ShouldAlmostEqual, 0.5)
				val, _ = vec.Get(1)
				So(val, ShouldAlmostEqual, 7.1)
				val, _ = vec.Get(2)
				So(val, ShouldAlmostEqual, 0)
				val, _ = vec.Get(3)
				So(val, ShouldAlmostEqual, 0.0)
			})
		})

	})
}

func TestTypeVector(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including pod vector types,", t, func() {

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

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to type vector should work", func() {

			store.Begin()
			child, _ := rntm.mainObj.GetChildByName("TypeVec")
			vec := child.(*vector)
			length, _ := vec.Length()
			So(length, ShouldEqual, 0)
			store.Commit()

			code = `toplevel.TypeVec.AppendNew()`
			_, err := rntm.RunJavaScript("user3", code)
			So(err, ShouldBeNil)
			/*
				store.Begin()
				data, ok := val.(Data)
				So(ok, ShouldBeTrue)
				So(data.HasProperty("test"), ShouldBeTrue)
				store.Commit()

				_, ok = rntm.objects[data.Id()]
				So(ok, ShouldBeTrue)*/

		})

		Convey("Setting data to the new type is supported", func() {

			code = `
				obj = toplevel.TypeVec.Get(0)
				obj.test = 1
			`
			_, err := rntm.RunJavaScript("user3", code)
			So(err, ShouldBeNil)

			store.Begin()
			defer store.Rollback()

			child, _ := rntm.mainObj.GetChildByName("TypeVec")
			vec := child.(*vector)

			entry, err := vec.Get(0)
			So(err, ShouldBeNil)
			obj := entry.(Object)

			So(obj.GetProperty("test").GetValue(), ShouldEqual, 1)
		})
	})
}
