package p2p

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBasicLog(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	Convey("Setting up two random hosts with swarm,", t, func() {

		swid := SwarmID("myswarm")

		h1, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h1.Stop()
		sw1 := h1.CreateSwarm(swid)
		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h2.Stop()
		sw2 := h2.CreateSwarm(swid)

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h1.Connect(context.Background(), h2.ID())
		sw1.AddPeer(h2.ID(), AUTH_READWRITE)
		sw2.AddPeer(h1.ID(), AUTH_READWRITE)

		Convey("Creating a log is possible on both swarms", func() {

			log1, err := newSwarmReplicatedLog("mylog", sw1)
			So(err, ShouldBeNil)
			So(log1, ShouldNotBeNil)
			defer log1.Stop()

			log2, err := newSwarmReplicatedLog("mylog", sw2)
			So(err, ShouldBeNil)
			So(log2, ShouldNotBeNil)
			defer log2.Stop()

			Convey("and must be possible to add logs", func() {

				block := randomBlock(512)
				log1.AppendEntry(block.RawData())

				time.Sleep(100 * time.Millisecond)
				So(len(log2.voteManager.Votes), ShouldEqual, 1)
				So(len(log2.voteManager.Votes[1]), ShouldEqual, 2)
			})
		})
	})
}
