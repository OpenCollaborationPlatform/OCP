package document

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/OpenCollaborationPlatform/OCP/dml"
	"github.com/gammazero/nexus/v3/wamp"
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

			s1, _, err := state.dml.Call(state.store, dml.User("test"), "Test.testS", args(), kwargs())
			So(err, ShouldBeNil)
			i1, _, err := state.dml.Call(state.store, dml.User("test"), "Test.testI", args(), kwargs())
			So(err, ShouldBeNil)

			Convey("as well as reloadable", func() {
				err := state.LoadSnapshot(snap1)
				So(err, ShouldBeNil)

				s2, _, err := state.dml.Call(state.store, dml.User("test"), "Test.testS", args(), kwargs())
				So(err, ShouldBeNil)
				i2, _, err := state.dml.Call(state.store, dml.User("test"), "Test.testI", args(), kwargs())
				So(err, ShouldBeNil)

				So(s1, ShouldEqual, s2)
				So(i1, ShouldEqual, i2)
			})

			Convey("A snapshot from changed data", func() {

				_, _, err := state.dml.RunJavaScript(state.store, dml.User("test"), "Test.testS = \"yeah\"")
				So(err, ShouldBeNil)
				_, _, err = state.dml.RunJavaScript(state.store, dml.User("test"), "Test.testI = 25")
				So(err, ShouldBeNil)

				snap2, err := state.Snapshot()
				So(err, ShouldBeNil)
				So(len(snap2), ShouldNotEqual, 0)

				s3, _, err := state.dml.Call(state.store, dml.User("test"), "Test.testS", args(), kwargs())
				So(err, ShouldBeNil)
				i3, _, err := state.dml.Call(state.store, dml.User("test"), "Test.testI", args(), kwargs())
				So(err, ShouldBeNil)

				Convey("is reloadable as well", func() {

					err := state.LoadSnapshot(snap2)
					So(err, ShouldBeNil)

					s4, _, _ := state.dml.Call(state.store, dml.User("test"), "Test.testS", args(), kwargs())
					i4, _, _ := state.dml.Call(state.store, dml.User("test"), "Test.testI", args(), kwargs())

					So(s3, ShouldEqual, s4)
					So(i3, ShouldEqual, i4)
					So(i3, ShouldEqual, 25)
				})

				Convey("but the state can also load snapshots with different data", func() {

					err := state.LoadSnapshot(snap1)
					So(err, ShouldBeNil)

					s5, _, _ := state.dml.Call(state.store, dml.User("test"), "Test.testS", args(), kwargs())
					i5, _, _ := state.dml.Call(state.store, dml.User("test"), "Test.testI", args(), kwargs())

					So(s5, ShouldEqual, s1)
					So(i5, ShouldEqual, i1)
				})
			})
		})
	})
}

func TestStateView(t *testing.T) {

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

		id1 := wamp.ID(1)
		id2 := wamp.ID(2)

		Convey("some data can be added.", func() {

			_, _, err := state.dml.RunJavaScript(state.store, dml.User("test"), "Test.testS = \"yeah\"")
			So(err, ShouldBeNil)
			_, _, err = state.dml.RunJavaScript(state.store, dml.User("test"), "Test.testI = 25")
			So(err, ShouldBeNil)

			Convey("No view exist for any session by default", func() {
				So(state.HasView(id1), ShouldBeFalse)
				So(state.HasView(id2), ShouldBeFalse)
			})

			Convey("A view can be opened", func() {
				err := state.OpenView(id1)
				So(err, ShouldBeNil)

				So(state.HasView(id1), ShouldBeTrue)
				So(state.HasView(id2), ShouldBeFalse)

				Convey("which creates the appropriate folder", func() {

					path := state.views.views[id1].path
					_, err := os.Stat(path)
					So(err, ShouldBeNil)
				})

				Convey("and allows local access with view", func() {

					_, err := state.CanCallLocal(id1, "Test.testI")
					So(err, ShouldBeNil)
					val, err := state.CallLocal(id1, "", "Test.testI")
					So(err, ShouldBeNil)
					So(val, ShouldEqual, 25)
				})

				Convey("Chaning the state does not change the View", func() {

					_, _, err = state.dml.RunJavaScript(state.store, dml.User("test"), "Test.testI = 10")
					So(err, ShouldBeNil)

					val, err := state.CallLocal(id2, "", "Test.testI")
					So(err, ShouldBeNil)
					So(val, ShouldEqual, 10)

					val, err = state.CallLocal(id1, "", "Test.testI")
					So(err, ShouldBeNil)
					So(val, ShouldEqual, 25)
				})

				Convey("Closing the view works", func() {

					err := state.CloseView(id1)
					So(err, ShouldBeNil)
					So(state.HasView(id1), ShouldBeFalse)
					So(state.HasView(id2), ShouldBeFalse)

					Convey("and removes the folder", func() {
						path := state.views.views[id1].path
						_, err := os.Stat(path)
						So(err, ShouldNotBeNil)
						So(os.IsNotExist(err), ShouldBeTrue)
					})
				})
			})

		})
	})
}
