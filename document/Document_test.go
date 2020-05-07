package document

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"

	wamp "github.com/gammazero/nexus/v3/wamp"
	. "github.com/smartystreets/goconvey/convey"
)

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
		handler, err := NewDocumentHandler(router, host)
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

			res, err := client.Call(ctx, "ocp.documents.create", wamp.Dict{}, wamp.List{dmlpath}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldNotBeNil)

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

				uri := "ocp.documents.edit." + docID + ".call.Test.testI"
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
		host1, host2, _ := p2p.MakeTemporaryTwoHostNetwork(path)
		defer host1.Stop(context.Background())
		defer host2.Stop(context.Background())

		//setup the document handlers
		handler1, err := NewDocumentHandler(router1, host1)
		So(err, ShouldBeNil)
		So(handler1, ShouldNotBeNil)
		defer handler1.Close(context.Background())

		handler2, err := NewDocumentHandler(router2, host2)
		So(err, ShouldBeNil)
		So(handler2, ShouldNotBeNil)
		defer handler2.Close(context.Background())

		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		Convey("creating a document is possible on host 1", func() {

			res, err := client1.Call(ctx, "ocp.documents.create", wamp.Dict{}, wamp.List{dmlpath}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldNotBeNil)

			docID, ok := res.Arguments[0].(string)
			So(ok, ShouldBeTrue)

			//wait a bit for announcement to finish
			time.Sleep(100 * time.Millisecond)

			Convey("but is not joinable by host2 directly to due missing authorisation", func() {

				_, err := client2.Call(ctx, "ocp.documents.open", wamp.Dict{}, wamp.List{docID}, wamp.Dict{}, nil)
				So(err, ShouldNotBeNil)
			})

			Convey("Adding the second host as peer to the document", func() {

				uri := "ocp.documents." + docID + ".addPeer"
				_, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{host2.ID().Pretty(), "write"}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)

				uri = "ocp.documents." + docID + ".listPeers"
				res, err := client1.Call(ctx, uri, wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				peers, ok := res.Arguments[0].([]string)
				So(ok, ShouldBeTrue)
				So(len(peers), ShouldEqual, 2)
				So(peers, ShouldContain, host1.ID().Pretty())
				So(peers, ShouldContain, host2.ID().Pretty())

				Convey("makes the document joinable by host2", func() {

					_, err := client2.Call(ctx, "ocp.documents.open", wamp.Dict{}, wamp.List{docID}, wamp.Dict{}, nil)
					So(err, ShouldBeNil)

					Convey("which adds it to its document list", func() {
						res, err := client2.Call(ctx, "ocp.documents.list", wamp.Dict{}, wamp.List{}, wamp.Dict{}, nil)
						So(err, ShouldBeNil)
						list, ok := res.Arguments[0].([]string)
						So(ok, ShouldBeTrue)
						So(len(list), ShouldEqual, 1)
						So(list[0], ShouldEqual, docID)
					})

					Convey("and editable by both hosts", func() {

						uri := "ocp.documents.edit." + docID + ".call.Test.testI"
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
			})
		})
	})
}
