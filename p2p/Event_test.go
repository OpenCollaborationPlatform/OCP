package p2p

/*
import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func asyncCatchEvents(sub Subscription, num *int, data *[][]byte, closed *bool) sync.Mutex {
	mutex := sync.Mutex{}
	*closed = false
	go func() {
		for {
			//timeout must be higher than waiting in test functions
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			msg, err := sub.Next(ctx)
			if err != nil {
				break
			}
			mutex.Lock()
			(*num)++
			*data = append(*data, msg.Data)
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
		defer h1.Stop()
		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h2.Stop()

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Subscribing to a topic after connecting works", func() {

			h2.Connect(context.Background(), h1.ID())
			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)

			Convey("and must be publishable from the other host", func() {

				num := 0
				data := make([][]byte, 0)
				closed := false
				asyncCatchEvents(sub, &num, &data, &closed)

				h1.Event.Publish("testtopic", []byte("data"))
				h2.Event.Publish("testtopic", []byte("data"))
				time.Sleep(100 * time.Millisecond)
				sub.Cancel()
				time.Sleep(100 * time.Millisecond)
				So(closed, ShouldBeTrue)
				So(num, ShouldEqual, 2)
				So(bytes.Equal(data[0], []byte("data")), ShouldBeTrue)
				So(bytes.Equal(data[1], []byte("data")), ShouldBeTrue)
			})
		})

		Convey("as well as unscribing to a topic before connecting", func() {

			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)
			h2.Connect(context.Background(), h1.ID())

			Convey("and must be publishable from the other host", func() {

				num := 0
				data := make([][]byte, 0)
				closed := false
				asyncCatchEvents(sub, &num, &data, &closed)

				h2.Event.Publish("testtopic", []byte("data"))
				h2.Event.Publish("testtopic", []byte("data"))
				time.Sleep(100 * time.Millisecond)
				sub.Cancel()
				time.Sleep(100 * time.Millisecond)
				So(closed, ShouldBeTrue)
				So(num, ShouldEqual, 2)
				So(bytes.Equal(data[0], []byte("data")), ShouldBeTrue)
				So(bytes.Equal(data[1], []byte("data")), ShouldBeTrue)
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
		defer h1.Stop()
		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h2.Stop()

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h2.Connect(context.Background(), h1.ID())

		Convey("Having a swarm for each host", func() {

			swid := SwarmID("myswarm")
			sw1 := h1.CreateSwarm(swid)
			sw2 := h2.CreateSwarm(swid)

			Convey("registering with ReadOnly requirement should work", func() {

				sub1, err := sw1.Event.Subscribe("testtopic", AUTH_READONLY)
				So(err, ShouldBeNil)
				sub2, err := sw2.Event.Subscribe("testtopic", AUTH_READONLY)
				So(err, ShouldBeNil)

				Convey("Calling wthout adding the peers will fail (except for itself)", func() {
					num1 := 0
					data1 := make([][]byte, 0)
					closed1 := false
					m1 := asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]byte, 0)
					closed2 := false
					m2 := asyncCatchEvents(sub2, &num2, &data2, &closed2)

					h1.Event.Publish("testtopic", []byte("data"))
					sw1.Event.Publish("testtopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 1)
					So(num2, ShouldEqual, 0)
					m1.Unlock()
					m2.Unlock()

					h2.Event.Publish("testtopic", []byte("data"))
					sw2.Event.Publish("testtopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)
					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 1)
					So(num2, ShouldEqual, 1)
					So(closed1, ShouldBeTrue)
					So(closed2, ShouldBeTrue)
					m1.Unlock()
					m2.Unlock()
				})

				Convey("Adding one peer to the other swarm shall allow this one to publish", func() {

					num1 := 0
					data1 := make([][]byte, 0)
					closed1 := false
					m1 := asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]byte, 0)
					closed2 := false
					m2 := asyncCatchEvents(sub2, &num2, &data2, &closed2)

					sw1.AddPeer(context.Background(), h2.ID(), AUTH_READONLY)

					sw1.Event.Publish("testtopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 1)
					So(num2, ShouldEqual, 0)
					m1.Unlock()
					m2.Unlock()

					sw2.Event.Publish("testtopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)
					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 2)
					So(num2, ShouldEqual, 1)
					So(closed1, ShouldBeTrue)
					So(closed2, ShouldBeTrue)
					m1.Unlock()
					m2.Unlock()
				})

			})

			Convey("Registering with ReadWrite requirement should work", func() {

				sub1, err := sw1.Event.Subscribe("testtopic", AUTH_READWRITE)
				So(err, ShouldBeNil)
				sub2, err := sw2.Event.Subscribe("testtopic", AUTH_READWRITE)
				So(err, ShouldBeNil)

				Convey("and unadded as well as unauthorized peers shall not be to publish", func() {

					sw1.AddPeer(context.Background(), h2.ID(), AUTH_READONLY)

					num1 := 0
					data1 := make([][]byte, 0)
					closed1 := false
					m1 := asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]byte, 0)
					closed2 := false
					m2 := asyncCatchEvents(sub2, &num2, &data2, &closed2)

					sw1.Event.Publish("testtopic", []byte("data"))
					sw2.Event.Publish("testtopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)

					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					m1.Lock()
					m2.Lock()
					So(num1, ShouldEqual, 1) //you can always call yourself
					So(num2, ShouldEqual, 1) //you can always call yourself
					So(closed1, ShouldBeTrue)
					So(closed2, ShouldBeTrue)
					m1.Unlock()
					m2.Unlock()
				})

				Convey("If a authorisation exists, events for ReadWrite shall pass", func() {

					//h2 is allowed to post ReadWrite events to swarm 1, but not the other
					//way around
					sw1.AddPeer(context.Background(), h2.ID(), AUTH_READWRITE)
					sw2.AddPeer(context.Background(), h1.ID(), AUTH_READONLY)

					num1 := 0
					data1 := make([][]byte, 0)
					closed1 := false
					asyncCatchEvents(sub1, &num1, &data1, &closed1)

					num2 := 0
					data2 := make([][]byte, 0)
					closed2 := false
					asyncCatchEvents(sub2, &num2, &data2, &closed2)

					sw1.Event.Publish("testtopic", []byte("data"))
					sw2.Event.Publish("testtopic", []byte("data"))
					time.Sleep(100 * time.Millisecond)
					sub1.Cancel()
					sub2.Cancel()
					time.Sleep(100 * time.Millisecond)

					So(closed1, ShouldBeTrue)
					So(num1, ShouldEqual, 2)
					So(closed2, ShouldBeTrue)
					So(num2, ShouldEqual, 1)
				})
			})
		})
	})
}
*/
