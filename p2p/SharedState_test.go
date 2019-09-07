package p2p

import (
	"fmt"
	"time"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"testing"

	//"time"

	"github.com/ickby/CollaborationNode/p2p/replica"

	. "github.com/smartystreets/goconvey/convey"
)

//simple state that adds the cmd (a bytes version of a int) to a value
type testState struct {
	value int64
}

func (self *testState) Apply(log *replica.Log) interface{} {

	vl, _ := binary.Varint(log.Data)
	self.value = self.value + vl
	return self.value
}

func (self *testState) Snapshot() (replica.Snapshot, error) {
	return testSnapshot{self.value}, nil
}

func (self *testState) Restore(reader io.ReadCloser) error {

	buf := make([]byte, binary.MaxVarintLen64)
	reader.Read(buf)
	vl, _ := binary.Varint(buf)
	self.value = vl
	return nil
}

//simple snapshot to work with testState
type testSnapshot struct {
	value int64
}

func (self testSnapshot) Persist(sink replica.SnapshotSink) error {
	sink.Write(toByte(self.value))
	return sink.Close()
}

func (self testSnapshot) Release() {}

func toByte(val int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, val)
	return buf
}

func TestSingleNodeSharedState(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	swid := SwarmID("myswarm")

	h1, _ := temporaryHost(path)
	defer h1.Stop()
	//create the swarm rigth away so that they get the correct directory to us
	sw1 := h1.CreateSwarm(swid)

	Convey("Sharing services shall be possible for a single node", t, func() {

		st1 := &testState{0}
		err := sw1.State.Share(st1)
		So(err, ShouldBeNil)
		//time.Sleep(5000 * time.Millisecond)

		Convey("adding command works", func() {

			ctx := context.Background()
			res, err := sw1.State.AddCommand(ctx, toByte(1))
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

	ctx := context.Background()
	swid := SwarmID("myswarm")

	h1, _ := temporaryHost(path)
	defer h1.Stop()
	h2, _ := temporaryHost(path)
	defer h2.Stop()
	h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
	h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
	
	//create the swarm rigth away so that they get the correct directory to us
	sw1 := h1.CreateSwarm(swid)
	sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
	
	time.Sleep(3*time.Second)
	fmt.Println("start swarm 2")
	
	sw2 := h2.CreateSwarm(swid, WithSwarmPeers(h1.ID(), AUTH_READWRITE))



	Convey("Giving the swarm write rigths,", t, func() {

		st1 := &testState{0}
		err := sw1.State.Share(st1)
		So(err, ShouldBeNil)

		st2 := &testState{0}
		err = sw2.State.Share(st2)
		So(err, ShouldBeNil)

		Convey("adding command works from both swarms", func() {

			res, err := sw1.State.AddCommand(ctx, toByte(1))
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 1)
			res, err = sw2.State.AddCommand(ctx, toByte(1))
			So(err, ShouldBeNil)
			So(res, ShouldEqual, 2)

			Convey("and the states are updated", func() {

				So(st1.value, ShouldEqual, 2)
				So(st2.value, ShouldEqual, 2)
			})
		})

	})
}
