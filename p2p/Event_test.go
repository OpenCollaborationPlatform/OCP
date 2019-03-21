package p2p

/*
import (
	"bytes"
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
				var data []byte
				closed := false
				go func() {
					for num < 2 {
						ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
						msg, err := sub.Next(ctx)
						if err != nil {
							break
						}
						num++
						data = msg.Data
					}
					closed = true
				}()

				h2.Event.Publish("testtopic", []byte("data"))
				h2.Event.Publish("testtopic", []byte("data"))
				time.Sleep(200 * time.Millisecond)
				sub.Cancel()
				So(closed, ShouldBeTrue)
				So(num, ShouldEqual, 2)
				So(bytes.Equal(data, []byte("data")), ShouldBeTrue)
			})
		})

		Convey("as well as ubscribing to a topic before connecting", func() {

			sub, err := h1.Event.Subscribe("testtopic")
			So(err, ShouldBeNil)
			So(sub.sub, ShouldNotBeNil)
			h2.Connect(context.Background(), h1.ID())

			Convey("and must be publishable from the other host", func() {

				num := 0
				var data []byte
				closed := false
				go func() {
					for num < 2 {
						ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
						msg, err := sub.Next(ctx)
						if err != nil {
							break
						}
						num++
						data = msg.Data
					}
					closed = true
				}()

				h2.Event.Publish("testtopic", []byte("data"))
				h2.Event.Publish("testtopic", []byte("data"))
				time.Sleep(200 * time.Millisecond)
				sub.Cancel()
				So(closed, ShouldBeTrue)
				So(num, ShouldEqual, 2)
				So(bytes.Equal(data, []byte("data")), ShouldBeTrue)
			})
		})

	})
}
*/
