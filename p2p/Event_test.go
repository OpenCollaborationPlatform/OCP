package p2p

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBasicEvent(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	Convey("Setting up two random hosts,", t, func() {

		h1, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h1.Stop(context.Background())
		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h2.Stop(context.Background())

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Subscribing to a topic after connecting works", func() {

			h2.Connect(context.Background(), h1.ID(), true)
			So(h1.Event.RegisterTopic("testtopic"), ShouldBeNil)
			So(h2.Event.RegisterTopic("testtopic"), ShouldBeNil)
			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)

			Convey("and must be publishable from the other host", func() {

				cctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

				h1.Event.Publish("testtopic", "data")
				evt, err := sub.Next(cctx)
				So(err, ShouldBeNil)
				So(evt.Arguments, ShouldHaveLength, 1)
				So(evt.Arguments, ShouldContain, "data")

				h2.Event.Publish("testtopic", 1, 2.2)
				evt, err = sub.Next(cctx)
				So(err, ShouldBeNil)
				So(evt.Arguments, ShouldHaveLength, 2)
				So(evt.Arguments[0], ShouldEqual, 1)
				So(evt.Arguments[1], ShouldEqual, 2.2)

				sub.Cancel()
				_, err = sub.Next(cctx)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("as well as subscribing to a topic before connecting", func() {

			So(h1.Event.RegisterTopic("testtopic"), ShouldBeNil)
			So(h2.Event.RegisterTopic("testtopic"), ShouldBeNil)
			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)
			h2.Connect(context.Background(), h1.ID(), true)
			time.Sleep(500 * time.Millisecond)

			Convey("and must be publishable from the other host", func() {

				cctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

				h2.Event.Publish("testtopic", "data")
				evt, err := sub.Next(cctx)
				So(err, ShouldBeNil)
				So(evt.Arguments, ShouldHaveLength, 1)
				So(evt.Arguments, ShouldContain, "data")

				h2.Event.Publish("testtopic", 1, 2.2)
				evt, err = sub.Next(cctx)
				So(err, ShouldBeNil)
				So(evt.Arguments, ShouldHaveLength, 2)
				So(evt.Arguments[0], ShouldEqual, 1)
				So(evt.Arguments[1], ShouldEqual, 2.2)

				sub.Cancel()
				_, err = sub.Next(cctx)
				So(err, ShouldNotBeNil)
			})
		})

	})
}

func TestSwarmEvent(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	Convey("Setting up two random hosts,", t, func() {

		h1, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h1.Stop(context.Background())
		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h2.Stop(context.Background())

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h2.Connect(context.Background(), h1.ID(), true)

		Convey("Creating a swarm on the first host", func() {

			sw1, err := h1.CreateSwarm(context.Background(), NoStates())
			So(err, ShouldBeNil)
			So(sw1, ShouldNotBeNil)
			defer sw1.Close(context.Background())

			So(sw1.Event.RegisterTopic("testtopic", AUTH_READONLY), ShouldBeNil)
			time.Sleep(500 * time.Millisecond)

			Convey("Registering with ReadOnly requirement should work", func() {

				cctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
				sub1, err := sw1.Event.Subscribe("testtopic")
				So(err, ShouldBeNil)

				Convey("as well as publishing without adding a peers to the swarm", func() {

					So(h1.Event.Publish("testtopic", "data"), ShouldNotBeNil)
					So(sw1.Event.Publish("testtopic", 1), ShouldBeNil)
					time.Sleep(500 * time.Millisecond)

					evt, err := sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, 1)

					sub1.Cancel()
					_, err = sub1.Next(cctx)
					So(err, ShouldNotBeNil)
				})

				Convey("Adding one ReadOnly peer to the swarm shall allow this one to publish", func() {

					err := sw1.AddPeer(cctx, h2.ID(), AUTH_READONLY)
					So(err, ShouldBeNil)
					sw2, err := h2.JoinSwarm(cctx, sw1.ID, NoStates(), SwarmPeers(h1.ID()))
					So(sw2.Event.RegisterTopic("testtopic", AUTH_READONLY), ShouldBeNil)
					So(err, ShouldBeNil)
					time.Sleep(500 * time.Millisecond)

					sub2, err := sw2.Event.Subscribe("testtopic")
					So(err, ShouldBeNil)

					sw1.Event.Publish("testtopic", "data")
					evt, err := sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")
					evt, err = sub2.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")

					sw2.Event.Publish("testtopic", 1)
					evt, err = sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, 1)
					evt, err = sub2.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, 1)

					sub1.Cancel()
					_, err = sub1.Next(cctx)
					So(err, ShouldNotBeNil)
					sub2.Cancel()
					_, err = sub2.Next(cctx)
					So(err, ShouldNotBeNil)
				})

			})

			Convey("Registering with ReadWrite requirement should work", func() {

				cctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
				So(sw1.Event.RegisterTopic("RWtesttopic", AUTH_READWRITE), ShouldBeNil)
				sub1, err := sw1.Event.Subscribe("RWtesttopic")
				So(err, ShouldBeNil)

				Convey("and the peer itself shall be able to publish", func() {

					sw1.Event.Publish("RWtesttopic", "data")
					evt, err := sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")

					sub1.Cancel()
					_, err = sub1.Next(cctx)
					So(err, ShouldNotBeNil)
				})

				Convey("If a second peer with correct authorisation exists, events for ReadWrite shall pass", func() {

					err := sw1.AddPeer(cctx, h2.ID(), AUTH_READWRITE)
					So(err, ShouldBeNil)
					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
					So(err, ShouldBeNil)
					So(sw2.Event.RegisterTopic("RWtesttopic", AUTH_READWRITE), ShouldBeNil)
					time.Sleep(500 * time.Millisecond)

					sub2, err := sw2.Event.Subscribe("RWtesttopic")
					So(err, ShouldBeNil)

					sw1.Event.Publish("RWtesttopic", "data")
					sw2.Event.Publish("RWtesttopic", "data")

					evt, err := sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")
					evt, err = sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")

					evt, err = sub2.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")
					evt, err = sub2.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")

					sub1.Cancel()
					sub2.Cancel()
				})

				Convey("If a second peer with read only authorisation exists, events from this peer shall not pass", func() {

					err := sw1.AddPeer(cctx, h2.ID(), AUTH_READONLY)
					So(err, ShouldBeNil)
					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
					So(err, ShouldBeNil)
					time.Sleep(500 * time.Millisecond)

					So(sw2.Event.RegisterTopic("RWtesttopic", AUTH_READWRITE), ShouldBeNil)
					sub2, err := sw2.Event.Subscribe("RWtesttopic")
					So(err, ShouldBeNil)

					sw1.Event.Publish("RWtesttopic", "data")
					sw2.Event.Publish("RWtesttopic", "data")

					evt, err := sub1.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")
					fctx, _ := context.WithTimeout(cctx, 1*time.Second)
					evt, err = sub1.Next(fctx)
					So(err, ShouldNotBeNil)

					evt, err = sub2.Next(cctx)
					So(err, ShouldBeNil)
					So(evt.Arguments, ShouldHaveLength, 1)
					So(evt.Arguments[0], ShouldEqual, "data")
					fctx, _ = context.WithTimeout(cctx, 1*time.Second)
					evt, err = sub2.Next(fctx)
					So(err, ShouldNotBeNil)

					sub1.Cancel()
					sub2.Cancel()
				})
			})
		})
	})
}
