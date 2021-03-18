package document

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenCollaborationPlatform/OCP/connection"
	"github.com/OpenCollaborationPlatform/OCP/p2p"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	hclog "github.com/hashicorp/go-hclog"
	. "github.com/smartystreets/goconvey/convey"
)

//logger to discard all output
var testLogger hclog.Logger

func init() {
	testLogger = hclog.New(&hclog.LoggerOptions{
		Output: ioutil.Discard,
	})
}

type eventcatcher struct {
	events []string
}

func (self *eventcatcher) subscribeEvent(cl *nxclient.Client, event string) {
	cl.Subscribe(event, self.eventCallback, wamp.Dict{})
}

func (self *eventcatcher) eventCallback(event *wamp.Event) {
	uri := wamp.OptionURI(event.Details, "procedure")
	self.events = append(self.events, string(uri))
}

const (
	dmlDocContent = ` Data {
				.name: "Test"

			    property string 	testS: "Hallo"
			    property int 	testI: 1
				
				event TestEventZeroArgs
				event TestEventTwoArgs
				
				function TestFncZeroArgs() {
					this.testI = 0
				}
				
				function TestFncTwoArgs(a, b) {
					this.testI = a+b
				}
				
				Vector {
					.name: "Vector"
					.type: Data {
								property int testI: 10
						   }
				}
			}`
)

func TestDocumentSingleNode(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "document")
	defer os.RemoveAll(path)

	//setup the dml file to be accessbile for the document
	dmlpath := filepath.Join(path, "Dml")
	os.MkdirAll(dmlpath, os.ModePerm)
	ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlDocContent), os.ModePerm)

	Convey("Setting up a document handler", t, func() {

		//make a wamp router (and little test client)
		router, _ := connection.MakeTemporaryRouter()
		client, _ := router.GetLocalClient("testClient")

		//make a p2p host for communication (second one to mimic the network)
		host, baseHost, _ := p2p.MakeTemporaryTwoHostNetwork(path)
		defer baseHost.Stop(context.Background())
		defer host.Stop(context.Background())

		//setup the document handler
		handler, err := NewDocumentHandler(router, host, testLogger)
		So(err, ShouldBeNil)
		So(handler, ShouldNotBeNil)
		defer handler.Close(context.Background())

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		Convey("initially the list of documents is empty", func() {

			res, err := client.Call(ctx, "ocp.documents.list", wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			list, ok := res.Arguments[0].([]string)
			So(ok, ShouldBeTrue)
			So(len(list), ShouldEqual, 0)
		})

		Convey("creating a document is possible", func() {

			evtC := &eventcatcher{make([]string, 0)}
			evtC.subscribeEvent(client, "ocp.documents.created")

			res, err := client.Call(ctx, "ocp.documents.create", wamp.Dict{}, wamp.List{dmlpath}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(res.Arguments, ShouldHaveLength, 1)
			So(evtC.events, ShouldHaveLength, 1)

			docID, ok := res.Arguments[0].(string)
			So(ok, ShouldBeTrue)

			Convey("which raises the document list count", func() {

				res, err := client.Call(ctx, "ocp.documents.list", wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				list, ok := res.Arguments[0].([]string)
				So(ok, ShouldBeTrue)
				So(len(list), ShouldEqual, 1)
				So(list[0], ShouldEqual, docID)
			})

			Convey("and makes the document editable", func() {

				uri := "ocp.documents." + docID + ".content.Test.testI"
				res, err := client.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				So(len(res.Arguments), ShouldEqual, 1)
				So(res.Arguments[0], ShouldEqual, 1)

				res, err = client.Call(ctx, uri, wamp.Dict{}, wamp.List{20}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				So(len(res.Arguments), ShouldEqual, 1)
				So(res.Arguments[0], ShouldEqual, 20)

				res, err = client.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				So(len(res.Arguments), ShouldEqual, 1)
				So(res.Arguments[0], ShouldEqual, 20)
			})
		})
	})
}

func TestDocumentTwoNodes(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "document")
	defer os.RemoveAll(path)

	//setup the dml file to be accessbile for the document
	dmlpath := filepath.Join(path, "Dml")
	os.MkdirAll(dmlpath, os.ModePerm)
	ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlDocContent), os.ModePerm)

	Convey("Setting up a document handlers on all hosts/routers", t, func() {

		//make two wamp routers (and little test client)
		router1, router2, _ := connection.MakeTwoTemporaryRouters()
		client1, _ := router1.GetLocalClient("testClient")
		client2, _ := router2.GetLocalClient("testClient")

		//make two p2p host for communication
		host1, host2, err := p2p.MakeTemporaryTwoHostNetwork(path)
		defer host1.Stop(context.Background())
		defer host2.Stop(context.Background())
		So(err, ShouldBeNil)

		//setup the document handlers
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		handler1, err := NewDocumentHandler(router1, host1, testLogger)
		So(err, ShouldBeNil)
		So(handler1, ShouldNotBeNil)
		defer handler1.Close(ctx)

		handler2, err := NewDocumentHandler(router2, host2, testLogger)
		So(err, ShouldBeNil)
		So(handler2, ShouldNotBeNil)
		defer handler2.Close(ctx)

		Convey("creating a document is possible on host 1", func() {

			evtC := &eventcatcher{make([]string, 0)}
			evtC.subscribeEvent(client1, "ocp.documents.created")

			res, err := client1.Call(ctx, "ocp.documents.create", wamp.Dict{}, wamp.List{dmlpath}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(res.Arguments, ShouldHaveLength, 1)
			So(evtC.events, ShouldHaveLength, 1)

			docID, ok := res.Arguments[0].(string)
			So(ok, ShouldBeTrue)

			//wait a bit for announcement to finish
			time.Sleep(100 * time.Millisecond)

			Convey("but is not joinable by host2 directly to due missing authorisation", func() {

				evtC2 := &eventcatcher{make([]string, 0)}
				evtC2.subscribeEvent(client1, "ocp.documents.opened")

				_, err := client2.Call(ctx, "ocp.documents.open", wamp.Dict{}, wamp.List{docID}, wamp.Dict{}, nil)
				So(err, ShouldNotBeNil)
				So(evtC2.events, ShouldHaveLength, 0)
			})

			Convey("Adding the second host as peer to the document", func() {

				evtC2 := &eventcatcher{make([]string, 0)}
				evtC2.subscribeEvent(client1, "ocp.documents."+docID+".peerAdded")

				evtC21 := &eventcatcher{make([]string, 0)}
				evtC21.subscribeEvent(client2, "ocp.documents.invited")

				uri := "ocp.documents." + docID + ".addPeer"
				_, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{host2.ID().Pretty(), "write"}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				time.Sleep(50 * time.Millisecond)
				So(evtC2.events, ShouldHaveLength, 1)
				So(evtC21.events, ShouldHaveLength, 1)

				uri = "ocp.documents." + docID + ".listPeers"
				res, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				peers, ok := res.Arguments[0].([]string)
				So(ok, ShouldBeTrue)
				So(len(peers), ShouldEqual, 2)
				So(peers, ShouldContain, host1.ID().Pretty())
				So(peers, ShouldContain, host2.ID().Pretty())

				uri = "ocp.documents.invitations"
				res, err = client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				So(res.Arguments, ShouldHaveLength, 1)
				So(res.Arguments[0], ShouldResemble, []string{docID})

				Convey("makes the document joinable by host2", func() {

					evtC3 := &eventcatcher{make([]string, 0)}
					evtC3.subscribeEvent(client2, "ocp.documents.opened")

					evtC4 := &eventcatcher{make([]string, 0)}
					evtC4.subscribeEvent(client1, "ocp.documents."+docID+".peerActivityChanged")

					_, err := client2.Call(ctx, "ocp.documents.open", wamp.Dict{}, wamp.List{docID}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					time.Sleep(50 * time.Millisecond)
					So(evtC3.events, ShouldHaveLength, 1)
					So(evtC4.events, ShouldHaveLength, 1)

					Convey("which adds it to its document list", func() {
						res, err := client2.Call(ctx, "ocp.documents.list", wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						list, ok := res.Arguments[0].([]string)
						So(ok, ShouldBeTrue)
						So(len(list), ShouldEqual, 1)
						So(list[0], ShouldEqual, docID)
					})

					Convey("and editable by both hosts", func() {

						uri := "ocp.documents." + docID + ".content.Test.testI"
						_, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{10}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						//check if set on both nodes
						res, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						So(len(res.Arguments), ShouldEqual, 1)
						So(res.Arguments[0], ShouldEqual, 10)
						res, err = client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						So(len(res.Arguments), ShouldEqual, 1)
						So(res.Arguments[0], ShouldEqual, 10)

						_, err = client2.Call(ctx, uri, wamp.Dict{}, wamp.List{20}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						//check if set on both nodes
						res, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						So(len(res.Arguments), ShouldEqual, 1)
						So(res.Arguments[0], ShouldEqual, 20)
						res, err = client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						So(len(res.Arguments), ShouldEqual, 1)
						So(res.Arguments[0], ShouldEqual, 20)
					})
				})

				Convey("Removing the peer does uninvite him too", func() {

					uri := "ocp.documents." + docID + ".removePeer"
					_, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{host2.ID().Pretty()}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					time.Sleep(100 * time.Millisecond)
					So(evtC21.events, ShouldHaveLength, 2)

					uri = "ocp.documents.invitations"
					res, err = client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					So(res.Arguments, ShouldHaveLength, 1)
					So(res.Arguments[0], ShouldResemble, []string{})
				})

				Convey("Closing the doc as last peer should uninvite the other one", func() {

					uri := "ocp.documents.close"
					_, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{docID}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					time.Sleep(100 * time.Millisecond)
					So(evtC21.events, ShouldHaveLength, 2)

					uri = "ocp.documents.invitations"
					res, err = client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					So(res.Arguments, ShouldHaveLength, 1)
					So(res.Arguments[0], ShouldResemble, []string{})
				})
			})
		})
	})
}

func TestDocumentViews(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "document")
	defer os.RemoveAll(path)

	//setup the dml file to be accessbile for the document
	dmlpath := filepath.Join(path, "Dml")
	os.MkdirAll(dmlpath, os.ModePerm)
	ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlDocContent), os.ModePerm)

	Convey("Setting up a document with data", t, func() {

		//make a wamp router (and two  little test clients)
		router, _ := connection.MakeTemporaryRouter()
		client1, _ := router.GetLocalClient("testClient1")
		client2, _ := router.GetLocalClient("testClient2")
		client3, _ := router.GetLocalClient("testClient3")

		//make a p2p host for communication (second one to mimic the network)
		host, baseHost, _ := p2p.MakeTemporaryTwoHostNetwork(path)
		defer baseHost.Stop(context.Background())
		defer host.Stop(context.Background())

		//setup the document handler
		handler, err := NewDocumentHandler(router, host, testLogger)
		So(err, ShouldBeNil)
		So(handler, ShouldNotBeNil)
		defer handler.Close(context.Background())

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		res, err := client1.Call(ctx, "ocp.documents.create", wamp.Dict{}, wamp.List{dmlpath}, wamp.Dict{}, nil)
		So(err, ShouldBeNil)
		So(len(res.Arguments), ShouldNotBeNil)

		docID, ok := res.Arguments[0].(string)
		So(ok, ShouldBeTrue)

		uri := "ocp.documents." + docID + ".content.Test.testI"
		_, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{20}, wamp.Dict{}, nil)
		So(err, ShouldBeNil)

		Convey("Initial closing a view creates an error", func() {

			uri := "ocp.documents." + docID + ".view"
			open, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(open.Arguments, ShouldHaveLength, 1)
			So(open.Arguments[0], ShouldBeFalse)

			_, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{false}, wamp.Dict{}, nil)
			So(err, ShouldNotBeNil)
		})

		Convey("Creating a view is possible", func() {

			uri := "ocp.documents." + docID + ".view"
			_, err := client2.Call(ctx, uri, wamp.Dict{}, wamp.List{true}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)

			open, err := client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(open.Arguments, ShouldHaveLength, 1)
			So(open.Arguments[0], ShouldBeTrue)

			open, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(open.Arguments, ShouldHaveLength, 1)
			So(open.Arguments[0], ShouldBeFalse)

			evt1 := &eventcatcher{make([]string, 0)}
			evt1.subscribeEvent(client1, "ocp.documents."+docID+".content.Test.onPropertyChanged")
			evt2 := &eventcatcher{make([]string, 0)}
			evt2.subscribeEvent(client2, "ocp.documents."+docID+".content.Test.onPropertyChanged")
			evt3 := &eventcatcher{make([]string, 0)}
			evt3.subscribeEvent(client3, "ocp.documents."+docID+".content.Test.onPropertyChanged")

			Convey("Editing from the view creating session is not possible", func() {

				uri := "ocp.documents." + docID + ".content.Test.testI"
				_, err := client2.Call(ctx, uri, wamp.Dict{}, wamp.List{10}, wamp.Dict{}, nil)
				So(err, ShouldNotBeNil)

				So(len(evt1.events), ShouldEqual, 0) //no event as it's the source of change
				So(len(evt2.events), ShouldEqual, 0) //no event due to view
				So(len(evt3.events), ShouldEqual, 0) //no event as error raised
			})

			Convey("Editing from the other sessions is possible", func() {

				uri := "ocp.documents." + docID + ".content.Test.testI"
				_, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{30}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)

				Convey("Only non view sessions receive the event", func() {

					So(len(evt1.events), ShouldEqual, 0) //no event as it's the source of change
					So(len(evt2.events), ShouldEqual, 0) //no event due to view
					So(len(evt3.events), ShouldEqual, 1) //event
				})

				Convey("Reading from the view session returns value before the change", func() {

					uri := "ocp.documents." + docID + ".content.Test.testI"
					val, err := client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					So(val.Arguments[0], ShouldEqual, 20)

					val, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					So(val.Arguments[0], ShouldEqual, 30)
				})

				Convey("Closing the view after change", func() {

					uri := "ocp.documents." + docID + ".view"
					_, err := client2.Call(ctx, uri, wamp.Dict{}, wamp.List{false}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)

					open, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					So(open.Arguments, ShouldHaveLength, 1)
					So(open.Arguments[0], ShouldBeFalse)

					open, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)
					So(open.Arguments, ShouldHaveLength, 1)
					So(open.Arguments[0], ShouldBeFalse)

					Convey("applies all events that were issued while view was open to the relevant client", func() {

						So(len(evt1.events), ShouldEqual, 0) //no event as it's the source of change
						So(len(evt2.events), ShouldEqual, 1) //closed view, events are send
						So(len(evt3.events), ShouldEqual, 1) //event
					})

					Convey("and allows to access the newest state", func() {

						uri = "ocp.documents." + docID + ".content.Test.testI"
						val, err := client2.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						So(val.Arguments[0], ShouldEqual, 30)

						val, err = client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						So(val.Arguments[0], ShouldEqual, 30)
					})
				})
			})
		})
	})
}
