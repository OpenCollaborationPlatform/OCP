package dml

import (
	datastore "CollaborationNode/datastores"
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
					.id:"toplevel"
					
					Vector {
						.id: "IntVec"
						.type: int
					}
					
					Vector {
						.id: "BoolVec"
						.type: bool
					}
					
					Vector {
						.id: "StringVec"
						.type: string
					}
					
					Vector {
						.id: "FloatVec"
						.type: float
					}					
				}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to int vector should work", func() {
			child, _ := rntm.mainObj.GetChildByName("IntVec")
			intvec := child.(*vector)
			length, _ := intvec.Length()
			So(length, ShouldEqual, 0)
			code = `toplevel.IntVec.Append(0)`
			val, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			idx := val.(int64)
			So(idx, ShouldEqual, 0)
			length, _ = intvec.Length()
			So(length, ShouldEqual, 1)
		})

		Convey("Setting and reading entries of vector shall work", func() {

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
					.id:"toplevel"
					
					Vector {
						.id: "TypeVec"
						.type: Data {
							property int test: 0
						}
					}	
				}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("Adding to type vector should work", func() {

			child, _ := rntm.mainObj.GetChildByName("TypeVec")
			intvec := child.(*vector)

			length, _ := intvec.Length()
			So(length, ShouldEqual, 0)
			code = `toplevel.TypeVec.AppendNew()`
			val, err := rntm.RunJavaScript(code)
			So(err, ShouldBeNil)
			data, ok := val.(Data)
			So(ok, ShouldBeTrue)
			So(data.HasProperty("test"), ShouldBeTrue)
		})
	})
}
