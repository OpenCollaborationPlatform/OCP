package p2p

import (
	"context"
	"encoding/binary"
	"fmt"
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

func (self *testState) Apply(data []byte) error {

	vl, _ := binary.Varint(data)
	self.value = self.value + vl
	return nil
}

func (self *testState) Reset() error {
	self.value = 0
	return nil
}

func (self *testState) Snapshot() ([]byte, error) {

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, self.value)
	return buf, nil
}

func (self *testState) LoadSnapshot(data []byte) error {
	val, _ := binary.Varint(data)
	self.value = val

	return nil
}

func (self *testState) EnsureSnapshot(data []byte) error {

	val, _ := binary.Varint(data)
	if self.value != val {
		return fmt.Errorf("Snapshots does not represent current state")
	}

	return nil
}

func toByte(val int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, val)
	return buf
}

func TestBasicSharedState(t *testing.T) {

	//clear all registered replicas in the overlord
	overlord.Clear()

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	swid := SwarmID("myswarm")

	h1, _ := temporaryHost(path)
	defer h1.Stop()
	//create the swarm rigth away so that they get the correct directory to us
	sw1 := h1.CreateSwarm(swid)

	h2, _ := temporaryHost(path)
	defer h2.Stop()
	sw2 := h2.CreateSwarm(swid)

	h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
	h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

	fmt.Printf("\n\nHost 1: %v\n", h1.ID().String())
	fmt.Printf("Host 2: %v\n", h2.ID().String())

	Convey("Giving the swarm write rigths,", t, func() {

		ctx := context.Background()
		sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
		sw2.AddPeer(ctx, h1.ID(), AUTH_READWRITE)

		Convey("sharing services shall be possible", func() {

			st1 := &testState{0}
			sst1 := sw1.State.Share(st1)
			So(sst1, ShouldEqual, 0)

			st2 := &testState{0}
			sst2 := sw2.State.Share(st2)
			So(sst2, ShouldEqual, 0)

			Convey("adding command works from both swarms", func() {

				So(sw1.State.AddCommand(ctx, sst1, toByte(1)), ShouldBeNil)
				So(sw2.State.AddCommand(ctx, sst2, toByte(1)), ShouldBeNil)

				Convey("and the states are updated", func() {

					time.Sleep(500 * time.Millisecond)
					So(st1.value, ShouldEqual, 2)
					So(st2.value, ShouldEqual, 2)
				})
			})
		})
	})
}
