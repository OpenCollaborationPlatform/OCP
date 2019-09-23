package p2p

import (
	"context"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"
	"time"

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

	h0, _ := temporaryHost(path)
	defer h0.Stop(context.Background())

	h1, _ := temporaryHost(path)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	defer h1.Stop(ctx)

	h1.SetMultipleAdress(h0.ID(), h0.OwnAddresses())
	h1.Connect(context.Background(), h0.ID())

	Convey("Creating swarms and sharing services shall be possible", t, func() {

		//create the swarm rigth away so that they get the correct directory to us
		st1 := &testState{0}
		sw1, err := h1.CreateSwarm(context.Background(), SwarmStates(st1))
		So(err, ShouldBeNil)
		So(sw1, ShouldNotBeNil)
		time.Sleep(50 * time.Millisecond)

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

	Convey("Setup swarms on two different hosts,", t, func() {

		h1, _ := temporaryHost(path)
		defer h1.Stop(context.Background())

		h2, _ := temporaryHost(path)
		defer h2.Stop(context.Background())
		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h1.Connect(context.Background(), h2.ID())

		st1 := &testState{0}
		sw1, err := h1.CreateSwarm(context.Background(), SwarmStates(st1))
		So(err, ShouldBeNil)
		time.Sleep(50 * time.Millisecond)

		st2 := &testState{0}

		Convey("Joining from different host without adding it as peer before should fail", func() {

			sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, SwarmStates(st2), SwarmPeers(h1.ID()))
			So(err, ShouldNotBeNil)
			So(sw2, ShouldBeNil)
		})

		Convey("Adding the new to the initial swarm", func() {

			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			err := sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
			So(err, ShouldBeNil)
			So(sw1.HasPeer(h1.ID()), ShouldBeTrue)
			So(sw1.HasPeer(h2.ID()), ShouldBeTrue)

			Convey("Joining of the new swarm should work", func() {

				sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, SwarmStates(st2), SwarmPeers(h1.ID()))
				time.Sleep(500 * time.Millisecond) //wait till replication finished
				So(err, ShouldBeNil)
				So(sw2.HasPeer(h1.ID()), ShouldBeTrue)
				So(sw2.HasPeer(h2.ID()), ShouldBeTrue)

				Convey("and adding command works from both swarms", func() {

					res, err := sw1.State.AddCommand(ctx, "testState", toByte(1))
					So(err, ShouldBeNil)
					So(res, ShouldEqual, 1)
					res, err = sw2.State.AddCommand(ctx, "testState", toByte(1))
					So(err, ShouldBeNil)
					So(res, ShouldEqual, 2)
					time.Sleep(500 * time.Millisecond) //wait till replication finished

					Convey("which updates the states correctly", func() {

						So(st1.value, ShouldEqual, 2)
						So(st2.value, ShouldEqual, 2)
					})
				})

				Convey("Closing one swarm", func() {

					sw1.Close(ctx)

					Convey("Should keep the other one alive", func() {

						_, err := sw2.State.AddCommand(ctx, "testState", toByte(1))
						So(err, ShouldBeNil)
					})
				})

				Convey("The other way around works too", func() {

					sw2.Close(ctx)

					Convey("Should keep the other one alive", func() {

						_, err := sw1.State.AddCommand(ctx, "testState", toByte(1))
						So(err, ShouldBeNil)
					})
				})
			})
		})
	})
}

func TestConnectionStrategy(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	Convey("Setup 3 hosts, 2 and 3 connected to 1", t, func() {

		closeCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		h1, _ := temporaryHost(path)
		defer h1.Stop(closeCtx)

		h2, _ := temporaryHost(path)
		defer h2.Stop(closeCtx)

		h3, _ := temporaryHost(path)
		defer h3.Stop(closeCtx)

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h3.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h2.Connect(context.Background(), h1.ID())
		h3.Connect(context.Background(), h1.ID())

		Convey("Creating a swarm on host 2 (with host 3 as peer)", func() {

			st2 := &testState{0}
			sw2, err := h2.CreateSwarm(context.Background(), SwarmStates(st2))
			So(err, ShouldBeNil)
			So(sw2, ShouldNotBeNil)
			time.Sleep(50 * time.Millisecond)

			err = sw2.AddPeer(closeCtx, h3.ID(), AUTH_READWRITE)
			So(err, ShouldBeNil)
			//we wait till provide is done!
			time.Sleep(50 * time.Millisecond)

			Convey("Host 3 should be able to join without knowing host 2", func() {

				st3 := &testState{0}
				sw3, err := h3.JoinSwarm(context.Background(), sw2.ID, SwarmStates(st3), NoPeers())
				So(err, ShouldBeNil)
				So(sw3, ShouldNotBeNil)
				time.Sleep(50 * time.Millisecond)

				Convey("And states work as expected", func() {
					ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
					res, err := sw2.State.AddCommand(ctx, "testState", toByte(1))
					So(err, ShouldBeNil)
					So(res, ShouldEqual, 1)
					res, err = sw3.State.AddCommand(ctx, "testState", toByte(1))
					So(err, ShouldBeNil)
					So(res, ShouldEqual, 2)
					time.Sleep(500 * time.Millisecond) //wait till replication finished
					So(st2.value, ShouldEqual, 2)
					So(st3.value, ShouldEqual, 2)
				})

				Convey("Adding a fourth host to the swarm", func() {

					h4, _ := temporaryHost(path)
					defer h4.Stop(closeCtx)
					h4.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
					h4.Connect(closeCtx, h1.ID())

					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					st4 := &testState{0}
					err = sw2.AddPeer(ctx, h4.ID(), AUTH_READWRITE)
					So(err, ShouldBeNil)
					time.Sleep(50 * time.Millisecond)

					sw4, err := h4.JoinSwarm(context.Background(), sw2.ID, SwarmStates(st4), NoPeers())
					So(err, ShouldBeNil)

					Convey("should also connect everything together well", func() {
						ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
						res, err := sw2.State.AddCommand(ctx, "testState", toByte(1))
						So(err, ShouldBeNil)
						So(res, ShouldEqual, 1)
						res, err = sw3.State.AddCommand(ctx, "testState", toByte(1))
						So(err, ShouldBeNil)
						So(res, ShouldEqual, 2)
						res, err = sw4.State.AddCommand(ctx, "testState", toByte(1))
						So(err, ShouldBeNil)
						So(res, ShouldEqual, 3)
						time.Sleep(500 * time.Millisecond) //wait till replication finished
						So(st2.value, ShouldEqual, 3)
						So(st3.value, ShouldEqual, 3)
						So(st4.value, ShouldEqual, 3)
					})
				})
			})
		})
	})
}
