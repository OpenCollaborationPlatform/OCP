package p2p

import (
	"time"
	"context"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

//simple state that adds the cmd (a bytes version of a int) to a value
type testState struct {
	value int64
}

func (self *testState) Apply(cmd []byte) interface{} {

	vl, _ := binary.Varint(cmd)
	self.value = self.value + vl
	return self.value
}

func (self *testState) Snapshot() ([]byte, error) {
	
	return toByte(self.value), nil
}

func (self *testState) LoadSnapshot(snap []byte) error {

	vl, _ := binary.Varint(snap)
	self.value = vl
	return nil
}

func toByte(val int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, val)
	return buf
}

func TestSingleNodeSharedState(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	h1, _ := temporaryHost(path)
	defer h1.Stop()

	Convey("Creating swarms and sharing services shall be possible", t, func() {

		//create the swarm rigth away so that they get the correct directory to us
		st1 := &testState{0}
		sw1, err := h1.CreateSwarm(SwarmStates(st1))
		So(err, ShouldBeNil)
		So(sw1, ShouldNotBeNil)

		So(sw1.HasPeer(h1.ID()), ShouldBeTrue)

		Convey("adding command works", func() {

			ctx := context.Background()
			res, err := sw1.State.AddCommand(ctx, "testState", toByte(1))
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 1)

			Convey("and the state is updated", func() {

				So(st1.value, ShouldEqual, 1)
			})
		})
	})
}

func TestBasicSharedState(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	Convey("Setup swarms on two different hosts,", t, func() {

		h1, _ := temporaryHost(path)
		defer h1.Stop()
		st1 := &testState{0}
		sw1, err := h1.CreateSwarm(SwarmStates(st1))
		So(err, ShouldBeNil)

		h2, _ := temporaryHost(path)
		defer h2.Stop()
		st2 := &testState{0}
		sw2, err := h2.JoinSwarm(sw1.ID, SwarmStates(st2))
		So(err, ShouldBeNil)
		
		//for now make sure they know each other
		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())	

		Convey("Adding a peer to the joind swarm should fail", func() {
			
			err := sw2.AddPeer(ctx, h1.ID(), AUTH_READWRITE)
			So(err, ShouldNotBeNil)
		})
		
		Convey("but adding it to the initial swarm should work", func() {

			err := sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
			So(err, ShouldBeNil)

			So(sw1.HasPeer(h1.ID()), ShouldBeTrue)
			So(sw1.HasPeer(h2.ID()), ShouldBeTrue)
			So(sw2.HasPeer(h1.ID()), ShouldBeTrue)
			So(sw2.HasPeer(h2.ID()), ShouldBeTrue)

			Convey("adding command works from both swarms", func() {

				res, err := sw1.State.AddCommand(ctx, "testState", toByte(1))
				So(err, ShouldBeNil)
				So(res, ShouldEqual, 1)
				res, err = sw2.State.AddCommand(ctx, "testState", toByte(1))
				So(err, ShouldBeNil)
				So(res, ShouldEqual, 2)

				Convey("and the states are updated", func() {

					So(st1.value, ShouldEqual, 2)
					So(st2.value, ShouldEqual, 2)
				})
			})
		})
	})
}
