package p2p

// import (
// 	"context"
// 	"fmt"
// 	"io/ioutil"
// 	"os"
// 	"testing"
// 	"time"

// 	. "github.com/smartystreets/goconvey/convey"
// )

// type Service struct {
// 	count int
// }

// func (self *Service) Add(ctx context.Context, val int, ret *int) error {
// 	self.count = self.count + val
// 	*ret = self.count
// 	return nil
// }

// func (self *Service) FailingAdd(ctx context.Context, val int, ret *int) error {
// 	return fmt.Errorf("Unable to add anything")
// }

// func TestBasicRPC(t *testing.T) {

// 	//make temporary folder for the data
// 	path, _ := ioutil.TempDir("", "p2p")
// 	defer os.RemoveAll(path)

// 	Convey("Setting up two random hosts,", t, func() {

// 		h1, err := temporaryHost(path)
// 		So(err, ShouldBeNil)
// 		defer h1.Stop(context.Background())
// 		h2, err := temporaryHost(path)
// 		So(err, ShouldBeNil)
// 		defer h2.Stop(context.Background())

// 		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
// 		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

// 		Convey("Registering services shall be possible", func() {

// 			service := Service{0}
// 			err := h1.Rpc.Register(&service)
// 			So(err, ShouldBeNil)

// 			Convey("and must be callable from the other host", func() {

// 				var res int
// 				err := h2.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 				So(err, ShouldBeNil)
// 				So(res, ShouldEqual, 3)

// 				err = h2.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 				So(err, ShouldBeNil)
// 				So(res, ShouldEqual, 6)
// 			})

// 			Convey("as well as from outself", func() {

// 				var res int
// 				err := h1.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 				So(err, ShouldBeNil)
// 				So(res, ShouldEqual, 3)

// 				err = h1.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 				So(err, ShouldBeNil)
// 				So(res, ShouldEqual, 6)
// 			})

// 			Convey("but not on the other hot", func() {

// 				var res int
// 				err := h1.Rpc.Call(h2.ID().pid(), "Service", "Add", 3, &res)
// 				So(err, ShouldNotBeNil)
// 			})
// 		})

// 	})
// }

// func TestSwarmRPC(t *testing.T) {

// 	//make temporary folder for the data
// 	path, _ := ioutil.TempDir("", "p2p")
// 	defer os.RemoveAll(path)

// 	Convey("Setting up two random hosts with swarms,", t, func() {

// 		h1, err := temporaryHost(path)
// 		So(err, ShouldBeNil)
// 		defer h1.Stop(context.Background())
// 		h2, err := temporaryHost(path)
// 		So(err, ShouldBeNil)
// 		defer h2.Stop(context.Background())

// 		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
// 		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
// 		h1.Connect(context.Background(), h2.ID())

// 		Convey("Setting up a swarm without any peers", func() {

// 			sw1, err := h1.CreateSwarm(context.Background(), NoStates())
// 			So(err, ShouldBeNil)
// 			time.Sleep(50 * time.Millisecond)

// 			Convey("registering a read only reguirement service must work", func() {

// 				service := Service{0}
// 				err := sw1.Rpc.Register(&service, AUTH_READONLY)
// 				So(err, ShouldBeNil)

// 				Convey("With default ReadOnly peer added this should be callable", func() {

// 					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
// 					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READONLY)
// 					So(err, ShouldBeNil)
// 					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
// 					So(err, ShouldBeNil)

// 					var res int
// 					err = sw2.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 					So(err, ShouldBeNil)
// 					So(res, ShouldEqual, 3)
// 				})

// 				Convey("as well as with ReadWrite peer", func() {

// 					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
// 					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
// 					So(err, ShouldBeNil)
// 					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
// 					So(err, ShouldBeNil)

// 					var res int
// 					err = sw2.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 					So(err, ShouldBeNil)
// 					So(res, ShouldEqual, 3)
// 				})
// 			})

// 			Convey("Registering a service that requires readwrite permissions works as well", func() {

// 				service := Service{0}
// 				err := sw1.Rpc.Register(&service, AUTH_READWRITE)
// 				So(err, ShouldBeNil)

// 				Convey("With default ReadOnly peer added this should not be callable", func() {

// 					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
// 					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READONLY)
// 					So(err, ShouldBeNil)
// 					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
// 					So(err, ShouldBeNil)

// 					var res int
// 					err = sw2.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 					So(err, ShouldNotBeNil)
// 					So(res, ShouldEqual, 0)
// 				})

// 				Convey("but finally with ReadWrite peer it shall succeed", func() {

// 					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
// 					err := sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
// 					So(err, ShouldBeNil)
// 					sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
// 					So(err, ShouldBeNil)

// 					var res int
// 					err = sw2.Rpc.Call(h1.ID().pid(), "Service", "Add", 3, &res)
// 					So(err, ShouldBeNil)
// 					So(res, ShouldEqual, 3)
// 				})
// 			})
// 		})
// 	})
// }
