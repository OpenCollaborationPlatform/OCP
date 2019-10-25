package document

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ickby/CollaborationNode/dml"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	dmlContent = ` Data {
				.name: "Test"

			    property string 	testS: "Hallo"
			    property int 	testI: 1
			}`
)

func TestStateSnapshot(t *testing.T) {

	Convey("Setting up a new state", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "datastructure")
		defer os.RemoveAll(path)

		//copy the dml file in
		dmlpath := filepath.Join(path, "Dml")
		os.MkdirAll(dmlpath, os.ModePerm)
		ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlContent), os.ModePerm)

		state, err := newState(path)
		defer state.Close()
		So(err, ShouldBeNil)
		So(state, ShouldNotBeNil)

		Convey("A empty snapshot is createable", func() {

			snap1, err := state.Snapshot()
			So(err, ShouldBeNil)
			So(len(snap1), ShouldNotEqual, 0)

			s1, err := state.dml.ReadProperty(dml.User("test"), "Test", "testS")
			So(err, ShouldBeNil)
			i1, err := state.dml.ReadProperty(dml.User("test"), "Test", "testI")
			So(err, ShouldBeNil)

			Convey("as well as reloadable", func() {
				err := state.LoadSnapshot(snap1)
				So(err, ShouldBeNil)

				s2, err := state.dml.ReadProperty(dml.User("test"), "Test", "testS")
				So(err, ShouldBeNil)
				i2, err := state.dml.ReadProperty(dml.User("test"), "Test", "testI")
				So(err, ShouldBeNil)

				So(s1, ShouldEqual, s2)
				So(i1, ShouldEqual, i2)
			})

			Convey("A snapshot from changed data", func() {

				_, err := state.dml.RunJavaScript(dml.User("test"), "Test.testS = \"yeah\"")
				So(err, ShouldBeNil)
				_, err = state.dml.RunJavaScript(dml.User("test"), "Test.testI = 25")
				So(err, ShouldBeNil)

				snap2, err := state.Snapshot()
				So(err, ShouldBeNil)
				So(len(snap2), ShouldNotEqual, 0)

				s3, err := state.dml.ReadProperty(dml.User("test"), "Test", "testS")
				So(err, ShouldBeNil)
				i3, err := state.dml.ReadProperty(dml.User("test"), "Test", "testI")
				So(err, ShouldBeNil)

				Convey("is reloadable as well", func() {

					err := state.LoadSnapshot(snap2)
					So(err, ShouldBeNil)

					s4, _ := state.dml.ReadProperty(dml.User("test"), "Test", "testS")
					i4, _ := state.dml.ReadProperty(dml.User("test"), "Test", "testI")

					So(s3, ShouldEqual, s4)
					So(i3, ShouldEqual, i4)
					So(i3, ShouldEqual, 25)
				})

				Convey("but the state can also load snapshots with different data", func() {

					err := state.LoadSnapshot(snap1)
					So(err, ShouldBeNil)

					s5, _ := state.dml.ReadProperty(dml.User("test"), "Test", "testS")
					i5, _ := state.dml.ReadProperty(dml.User("test"), "Test", "testI")

					So(s5, ShouldEqual, s1)
					So(i5, ShouldEqual, i1)
				})
			})
		})
	})
}
