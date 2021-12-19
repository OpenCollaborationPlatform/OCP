package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestContinuityDefault(t *testing.T) {

	Convey("Setting up the basic runtime,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"
					property int value
					
					Data {
						.name: "sub"
						property int value
					}
					
					Continuity {
						.name: "continuity"
						.recursive: false
						.automatic: false
					}
					
					function multiIncrement() {
						this.continuity.Increment()
						this.continuity.Increment()
					}
				}`

		rntm := NewRuntime()
		rntm.currentUser = "User1"
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Default continuity should have correct setup", func() {

			state, _, err := rntm.Call(store, "user1", "Document.continuity.state", args(), kwargs())
			So(err, ShouldBeNil)
			So(state, ShouldEqual, 0)
		})

		Convey("and users do not know the default state", func() {

			has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
		})

		Convey("Changeing a value will fail", func() {

			_, _, err := rntm.Call(store, "user1", "Document.value", args(1), kwargs())
			So(err, ShouldNotBeNil)
		})

		Convey("Changeing child object works and does not trigger the behaviour", func() {

			_, _, err := rntm.Call(store, "user1", "Document.sub.value", args(1), kwargs())
			So(err, ShouldBeNil)
		})

		Convey("Making the state known by user works", func() {

			_, _, err := rntm.Call(store, "user1", "Document.continuity.SetKnownState", args(0), kwargs())
			So(err, ShouldBeNil)
			has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			Convey("and allows for change in the object", func() {

				_, _, err := rntm.Call(store, "user1", "Document.value", args(2), kwargs())
				So(err, ShouldBeNil)
			})

			Convey("Other users still do not know the state", func() {

				has, _, err := rntm.Call(store, "user2", "Document.continuity.HasLatestState", args(), kwargs())
				So(err, ShouldBeNil)
				So(has, ShouldBeFalse)
				_, _, err = rntm.Call(store, "user2", "Document.value", args(2), kwargs())
				So(err, ShouldNotBeNil)
			})

			Convey("Increment state is possible now", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.continuity.Increment", args(), kwargs())
				So(err, ShouldBeNil)

				Convey("and sends the correct event", func() {
					So(evts, ShouldHaveLength, 3)
					So(evts[2].Path, ShouldEqual, ".onStateUpdate")
					So(evts[2].Args, ShouldHaveLength, 1)
					So(evts[2].Args[0], ShouldEqual, 1)
				})

				Convey("Afterwards state is set to new value", func() {

					state, _, err := rntm.Call(store, "user1", "Document.continuity.state", args(), kwargs())
					So(err, ShouldBeNil)
					So(state, ShouldEqual, 1)
				})

				Convey("but users do not know the new state", func() {

					has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)
				})
			})

			Convey("Multi increment state only increments once", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.multiIncrement", args(), kwargs())
				So(err, ShouldBeNil)

				Convey("and sends the correct event", func() {
					So(evts, ShouldHaveLength, 3)
					So(evts[2].Path, ShouldEqual, ".onStateUpdate")
					So(evts[2].Args, ShouldHaveLength, 1)
					So(evts[2].Args[0], ShouldEqual, 1)
				})

				Convey("Afterwards state is set to new value", func() {

					state, _, err := rntm.Call(store, "user1", "Document.continuity.state", args(), kwargs())
					So(err, ShouldBeNil)
					So(state, ShouldEqual, 1)
				})

				Convey("but users do not know the new state", func() {

					has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)
				})
			})
		})

		Convey("Incrementing state is not possible with unknown state", func() {

			_, _, err := rntm.Call(store, "user1", "Document.continuity.Increment", args(), kwargs())
			So(err, ShouldNotBeNil)
		})

		Convey("Keyword argument works for state definition", func() {

			_, _, err := rntm.Call(store, "user1", "Document.value", args(1), kwargs("based_on_state", 0))
			So(err, ShouldBeNil)

			Convey("Which marks the user as knowing the state", func() {

				has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
				So(err, ShouldBeNil)
				So(has, ShouldBeTrue)
			})
		})
	})
}

func TestContinuityAutomatic(t *testing.T) {

	Convey("Setting up the basic runtime,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"
					property int value:  0
					
					Data {
						.name: "sub"
						property int value
					}
					
					Continuity {
						.name: "continuity"
						.recursive: false
						.automatic: true
					}
					
					function multiChange() {
						this.value = 1
						this.value = 2
					}
				}`

		rntm := NewRuntime()
		rntm.currentUser = "User1"
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Changeing a value will fail", func() {

			_, _, err := rntm.Call(store, "user1", "Document.value", args(1), kwargs())
			So(err, ShouldNotBeNil)
		})

		Convey("Making the state known by user works", func() {

			_, _, err := rntm.Call(store, "user1", "Document.continuity.SetKnownState", args(0), kwargs())
			So(err, ShouldBeNil)
			has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			Convey("and allows for change in the object", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.value", args(2), kwargs())
				So(err, ShouldBeNil)

				Convey("after which the state was automatically incremented", func() {
					//value update, state update, onStateUpdate
					So(evts, ShouldHaveLength, 5)
					So(evts[4].Path, ShouldEqual, ".onStateUpdate")
					So(evts[4].Args, ShouldHaveLength, 1)
					So(evts[4].Args[0], ShouldEqual, 1)
				})
			})

			Convey("Other users still do not know the state", func() {

				has, _, err := rntm.Call(store, "user2", "Document.continuity.HasLatestState", args(), kwargs())
				So(err, ShouldBeNil)
				So(has, ShouldBeFalse)
				_, _, err = rntm.Call(store, "user2", "Document.value", args(2), kwargs())
				So(err, ShouldNotBeNil)
			})

			Convey("Multiple changes lead to single state update", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.multiChange", args(), kwargs())
				So(err, ShouldBeNil)
				//2xvalue, 2xvalue, 2xstate, onStateUpdate
				So(evts, ShouldHaveLength, 7)
				So(evts[6].Path, ShouldEqual, ".onStateUpdate")
				So(evts[6].Args, ShouldHaveLength, 1)
				So(evts[6].Args[0], ShouldEqual, 1)
			})

			Convey("Changeing child object works and does not trigger the behaviour", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.sub.value", args(1), kwargs())
				So(err, ShouldBeNil)
				So(evts, ShouldHaveLength, 2)
			})
		})

		Convey("Changeing child object works and does not trigger the behaviour", func() {

			_, evts, err := rntm.Call(store, "user1", "Document.sub.value", args(1), kwargs())
			So(err, ShouldBeNil)
			So(evts, ShouldHaveLength, 2)
		})
	})
}

func TestContinuityRecursive(t *testing.T) {

	Convey("Setting up the basic runtime,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"
					property int value:  0
					
					Data {
						.name: "sub"
						property int value
					}
					
					Continuity {
						.name: "continuity"
						.recursive: true
						.automatic: true
					}
				}`

		rntm := NewRuntime()
		rntm.currentUser = "User1"
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("Changeing a value will fail", func() {

			_, _, err := rntm.Call(store, "user1", "Document.value", args(1), kwargs())
			So(err, ShouldNotBeNil)
		})

		Convey("Changeing a value in subobject will fail", func() {

			_, _, err := rntm.Call(store, "user1", "Document.value", args(1), kwargs())
			So(err, ShouldNotBeNil)
		})

		Convey("Making the state known by user works", func() {

			_, _, err := rntm.Call(store, "user1", "Document.continuity.SetKnownState", args(0), kwargs())
			So(err, ShouldBeNil)
			has, _, err := rntm.Call(store, "user1", "Document.continuity.HasLatestState", args(), kwargs())
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			Convey("and allows for change in the object", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.value", args(2), kwargs())
				So(err, ShouldBeNil)

				Convey("after which the state was automatically incremented", func() {
					//value update, state update, onStateUpdate
					So(evts, ShouldHaveLength, 5)
					So(evts[4].Path, ShouldEqual, ".onStateUpdate")
					So(evts[4].Args, ShouldHaveLength, 1)
					So(evts[4].Args[0], ShouldEqual, 1)
				})
			})

			Convey("Other users still do not know the state", func() {

				has, _, err := rntm.Call(store, "user2", "Document.continuity.HasLatestState", args(), kwargs())
				So(err, ShouldBeNil)
				So(has, ShouldBeFalse)
				_, _, err = rntm.Call(store, "user2", "Document.value", args(2), kwargs())
				So(err, ShouldNotBeNil)
			})

			Convey("Changeing child object works and does trigger the behaviour", func() {

				_, evts, err := rntm.Call(store, "user1", "Document.sub.value", args(1), kwargs())
				So(err, ShouldBeNil)

				Convey("after which the state was automatically incremented", func() {
					//value update, state update, onStateUpdate
					So(evts, ShouldHaveLength, 5)
					So(evts[4].Path, ShouldEqual, ".onStateUpdate")
					So(evts[4].Args, ShouldHaveLength, 1)
					So(evts[4].Args[0], ShouldEqual, 1)
				})
			})
		})

		Convey("Changeing child object works with keyword", func() {

			_, evts, err := rntm.Call(store, "user1", "Document.sub.value", args(1), kwargs("based_on_state", 0))
			So(err, ShouldBeNil)

			Convey("after which the state was automatically incremented", func() {
				//value update, state update, onStateUpdate
				So(evts, ShouldHaveLength, 5)
				So(evts[4].Path, ShouldEqual, ".onStateUpdate")
				So(evts[4].Args, ShouldHaveLength, 1)
				So(evts[4].Args[0], ShouldEqual, 1)
			})
		})
	})
}
