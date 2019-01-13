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

		Convey("Reading results from vector shall work", func() {

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

		})

	})
}
