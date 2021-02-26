package p2p

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func asyncCatchEvents(sub Subscription, num *int, arguments *[][]interface{}, closed *bool) sync.Mutex {
	mutex := sync.Mutex{}
	*closed = false
	go func() {
		for {
			//timeout must be higher than waiting in test functions
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			evt, err := sub.Next(ctx)
			if err != nil {
				break
			}
			mutex.Lock()
			(*num)++
			*arguments = append(*arguments, evt.Arguments)
			mutex.Unlock()
		}
		mutex.Lock()
		*closed = true
		mutex.Unlock()
	}()
	return mutex
}

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

			h2.Connect(context.Background(), h1.ID())
			So(h1.Event.RegisterTopic("testtopic"), ShouldBeNil)
			So(h2.Event.RegisterTopic("testtopic"), ShouldBeNil)
			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)

			Convey("and must be publishable from the other host", func() {

				num := 0
				data := make([][]interface{}, 0)
				closed := false
				asyncCatchEvents(sub, &num, &data, &closed)

				h1.Event.Publish("testtopic", "data")
				h2.Event.Publish("testtopic", 1, 2.2)
				time.Sleep(100 * time.Millisecond)
				sub.Cancel()
				time.Sleep(100 * time.Millisecond)
				So(closed, ShouldBeTrue)
				So(num, ShouldEqual, 2)
				So(data[0][0], ShouldEqual, "data")
				So(data[1][0], ShouldEqual, 1)
				So(data[1][1], ShouldAlmostEqual, 2.2)
			})
		})

		Convey("as well as subscribing to a topic before connecting", func() {

			So(h1.Event.RegisterTopic("testtopic"), ShouldBeNil)
			So(h2.Event.RegisterTopic("testtopic"), ShouldBeNil)
			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)
			h2.Connect(context.Background(), h1.ID())
			time.Sleep(100 * time.Millisecond)

			Convey("and must be publishable from the other host", func() {

				num := 0
				data := make([][]interface{}, 0)
				closed := false
				asyncCatchEvents(sub, &num, &data, &closed)

				h2.Event.Publish("testtopic", "data")
				h2.Event.Publish("testtopic", 1, 2.2)
				time.Sleep(100 * time.Millisecond)
				sub.Cancel()
				time.Sleep(100 * time.Millisecond)
				So(closed, ShouldBeTrue)
				So(num, ShouldEqual, 2)
				So(data[0][0], ShouldEqual, "data")
				So(data[1][0], ShouldEqual, 1)
				So(data[1][1], ShouldAlmostEqual, 2.2)
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
		h2.Connect(context.Background(), h1.ID())

		Convey("Creating a swarm on the first host", func() {

			sw1, err := h1.CreateSwarm(context.Background(), NoStates())
			defer sw1.Close(context.Background())
			So(err, ShouldBeNil)
			So(sw1.Event.RegisterTopic("testtopic", AUTH_READONLY), ShouldBeNil)
			time.Sleep(50 * time.Millisecond)

			Convey("Registering with ReadOnly requirement should work", func() {

				sub1, err := sw1.Event.Subscribe("testtopic")
				So(err, ShouldBeNil)

				Convey("as well as publishing without adding a peers to the swarm", func() {
					num1 := 0
					data1 := make([][]interface{}, 0)
					closed1 := false
					m1 := asyncCatchEvents(sub1, &num1, &data1, &closed1)

					So(h1.Event.Publish("testtopic", "data"), ShouldNotBeNil)
					So(sw1.Event.Publish("testtopic", 1), ShouldBeNil)
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					So(num1, ShouldEqual, 1)
					m1.Unlock()

					sub1.Cancel()
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					So(num1, ShouldEqual, 1)
					So(closed1, ShouldBeTrue)
					m1.Unlock()
				})

				Convey("Adding one ReadOnly peer to the swarm shall allow this one to publish", func() {

					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READONLY)
					So(err, ShouldBeNil)
					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
					So(sw2.Event.RegisterTopic("testtopic", AUTH_READONLY), ShouldBeNil)
					So(err, ShouldBeNil)
					time.Sleep(50 * time.Millisecond)

					sub2, err := sw2.Event.Subscribe("testtopic")
					So(err, ShouldBeNil)

					num1 := 0
					data1 := make([][]interface{}, 0)
					closed1 := false
					m1 := asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]interface{}, 0)
					closed2 := false
					m2 := asyncCatchEvents(sub2, &num2, &data2, &closed2)

					sw1.Event.Publish("testtopic", "data")
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 1)
					So(num2, ShouldEqual, 1)
					m1.Unlock()
					m2.Unlock()

					sw2.Event.Publish("testtopic", 1)
					time.Sleep(100 * time.Millisecond)
					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 2)
					So(num2, ShouldEqual, 2)
					So(closed1, ShouldBeTrue)
					So(closed2, ShouldBeTrue)
					m1.Unlock()
					m2.Unlock()
				})

			})

			Convey("Registering with ReadWrite requirement should work", func() {

				So(sw1.Event.RegisterTopic("RWtesttopic", AUTH_READWRITE), ShouldBeNil)
				sub1, err := sw1.Event.Subscribe("RWtesttopic")
				So(err, ShouldBeNil)

				Convey("and the peer itself shall be able to publish", func() {

					num1 := 0
					data1 := make([][]interface{}, 0)
					closed1 := false
					m1 := asyncCatchEvents(sub1, &num1, &data1, &closed1)

					sw1.Event.Publish("RWtesttopic", "data")
					time.Sleep(100 * time.Millisecond)

					sub1.Cancel()
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					So(num1, ShouldEqual, 1) //you can always call yourself
					So(closed1, ShouldBeTrue)
					m1.Unlock()
				})

				Convey("If a second peer witth correct authorisation exists, events for ReadWrite shall pass", func() {

					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
					So(err, ShouldBeNil)
					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
					So(err, ShouldBeNil)
					So(sw2.Event.RegisterTopic("RWtesttopic", AUTH_READWRITE), ShouldBeNil)
					time.Sleep(50 * time.Millisecond)

					sub2, err := sw2.Event.Subscribe("RWtesttopic")
					So(err, ShouldBeNil)

					num1 := 0
					data1 := make([][]interface{}, 0)
					closed1 := false
					asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]interface{}, 0)
					closed2 := false
					asyncCatchEvents(sub2, &num2, &data2, &closed2)

					sw1.Event.Publish("RWtesttopic", []byte("data"))
					sw2.Event.Publish("RWtesttopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)
					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					So(closed1, ShouldBeTrue)
					So(num1, ShouldEqual, 2)
					So(closed2, ShouldBeTrue)
					So(num2, ShouldEqual, 2)
				})

				Convey("If a second peer with read only authorisation exists, events from this peer shall not pass", func() {

					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READONLY)
					So(err, ShouldBeNil)
					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
					So(err, ShouldBeNil)
					time.Sleep(50 * time.Millisecond)

					So(sw2.Event.RegisterTopic("RWtesttopic", AUTH_READWRITE), ShouldBeNil)
					sub2, err := sw2.Event.Subscribe("RWtesttopic")
					So(err, ShouldBeNil)

					num1 := 0
					data1 := make([][]interface{}, 0)
					closed1 := false
					asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]interface{}, 0)
					closed2 := false
					asyncCatchEvents(sub2, &num2, &data2, &closed2)

					sw1.Event.Publish("RWtesttopic", []byte("data"))
					sw2.Event.Publish("RWtesttopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)
					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					So(closed1, ShouldBeTrue)
					So(num1, ShouldEqual, 1)
					So(closed2, ShouldBeTrue)
					So(num2, ShouldEqual, 1)
				})
			})
		})
	})
}
