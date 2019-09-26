package datastructure

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"

	wamp "github.com/gammazero/nexus/wamp"
	uuid "github.com/satori/go.uuid"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	dmlDsContent = ` Data {
				.id: "Test"

			    property string 	testS: "Hallo"
			    property int 	testI: 1
				
				event TestEventZeroArgs()
				event TestEventTwoArgs(string, int)
			}`
)

func TestDatastructure(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "datastructure")
	defer os.RemoveAll(path)

	//and for the swarm!
	id := uuid.NewV4().String()
	swarmpath := filepath.Join(path, id)
	os.MkdirAll(swarmpath, os.ModePerm)

	//make a wamp router to be used for local clients
	router, _ := connection.MakeTemporaryRouter()
	dsClient, _ := router.GetLocalClient("dsClient")
	testClient, _ := router.GetLocalClient("testClient")

	//make a p2p host for communication
	baseHost, _ := p2p.MakeTemporaryTestingHost(path)
	defer baseHost.Stop(context.Background())
	host, _ := p2p.MakeTemporaryTestingHost(path)
	defer host.Stop(context.Background())
	baseHost.SetMultipleAdress(host.ID(), host.OwnAddresses())
	host.SetMultipleAdress(baseHost.ID(), baseHost.OwnAddresses())
	host.Connect(context.Background(), baseHost.ID())

	//setup the dml file to be accessbile for the datastrcuture
	dmlpath := filepath.Join(swarmpath, "Dml")
	os.MkdirAll(dmlpath, os.ModePerm)
	ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlDsContent), os.ModePerm)

	Convey("Setting up a new datastructure works", t, func() {

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		
		ds, err := NewDatastructure(swarmpath, "/ocp/test", dsClient)
		So(err, ShouldBeNil)
		So(ds, ShouldNotBeNil)

		swarm, err := host.CreateSwarmWithID(ctx, p2p.SwarmID(id), p2p.SwarmStates(ds.GetState()))
		So(err, ShouldBeNil)
		ds.Start(swarm)

		Convey("And events are forwarded to all clients", func() {

			called := false
			var err error = nil
			handlerZeroArgs := func(args wamp.List, kwargs, details wamp.Dict) {
				called = true
				if len(args) != 0 {
					err = fmt.Errorf("Argument was provided for zero arg event")
				}
				if len(kwargs) != 0 {
					err = fmt.Errorf("Keywork arguments provided for event, which is not supportet")
				}
			}

			testClient.Subscribe("/ocp/test/events/Test/TestEventZeroArgs", handlerZeroArgs, make(wamp.Dict, 0))
			_, err = ds.dml.RunJavaScript("TestUser", "Test.TestEventZeroArgs.Emit()")
			So(err, ShouldBeNil)
			//wait a bit to be sure it reached us
			time.Sleep(50 * time.Millisecond)
			So(called, ShouldBeTrue)
			So(err, ShouldBeNil)

			called = false
			var args = make([]interface{}, 2)
			handlerTwoArgs := func(a wamp.List, kwargs, details wamp.Dict) {
				called = true
				if len(a) != 2 {
					err = fmt.Errorf("Argument was provided for zero arg event")
					return
				}
				if len(kwargs) != 0 {
					err = fmt.Errorf("Keywork arguments provided for event, which is not supportet")
					return
				}
				args[0] = a[0]
				args[1] = a[1]
			}
			testClient.Subscribe("/ocp/test/events/Test/TestEventTwoArgs", handlerTwoArgs, make(wamp.Dict, 0))
			_, err = ds.dml.RunJavaScript("TestUser", "Test.TestEventTwoArgs.Emit(\"Hello\", 42)")
			So(err, ShouldBeNil)
			//wait a bit to be sure it reached us
			time.Sleep(50 * time.Millisecond)
			So(called, ShouldBeTrue)
			So(err, ShouldBeNil)
			So(args[0], ShouldEqual, "Hello")
			So(args[1], ShouldEqual, 42)
		})
	})
}
