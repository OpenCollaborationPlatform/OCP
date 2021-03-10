package document

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	uuid "github.com/satori/go.uuid"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	dmlDsContent = ` Data {
				.name: "Test"

			    property string 	testS: "Hallo"
			    property int 	testI: 1

				//the two raw dat properties
				property raw RawData
				property raw RawData2

				event TestEventZeroArgs
				event TestEventTwoArgs

				function TestFncZeroArgs() {
					this.testI = 0
				}

				function TestFncTwoArgs(a, b) {
					this.testI = a+b
				}
				
				function TestList() {
					return []	
				}

				Vector {
					.name: "Vector"
					.type: Data {
								property int testI: 10
						   }
				}
				
				Graph {
					.name: "Graph"
					.node: string
					.edge: none	
				}
			}`
)

func TestDatastructure(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "datastructure")
	defer os.RemoveAll(path)

	Convey("Setting up a new datastructure works", t, func() {

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
		host.Connect(context.Background(), baseHost.ID(), false)

		//and for the swarm!
		id := uuid.NewV4().String()
		swarmpath := filepath.Join(host.GetPath(), id)
		os.MkdirAll(swarmpath, os.ModePerm)

		//setup the dml file to be accessbile for the datastrcuture
		dmlpath := filepath.Join(swarmpath, "Dml")
		os.MkdirAll(dmlpath, os.ModePerm)
		ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlDsContent), os.ModePerm)

		ds, err := NewDatastructure(swarmpath, "ocp.test", dsClient)
		So(err, ShouldBeNil)
		So(ds, ShouldNotBeNil)
		defer ds.Close()

		swarm, err := host.CreateSwarmWithID(context.Background(), p2p.SwarmID(id), p2p.SwarmStates(ds.GetState()))
		So(err, ShouldBeNil)
		ds.Start(swarm)
		defer swarm.Close(context.Background())

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		Convey("events are forwarded to all clients,", func() {

			called := false
			var err error = nil
			handlerZeroArgs := func(evt *wamp.Event) {
				called = true
				if len(evt.Arguments) != 0 {
					err = fmt.Errorf("Argument was provided for zero arg event")
				}
				if len(evt.ArgumentsKw) != 0 {
					err = fmt.Errorf("Keywork arguments provided for event, which is not supportet")
				}
			}

			testClient.Subscribe("ocp.test.content.Test.TestEventZeroArgs", handlerZeroArgs, make(wamp.Dict, 0))
			_, err = ds.dmlState.dml.RunJavaScript(ds.dmlState.store, "TestUser", "Test.TestEventZeroArgs.Emit()")
			So(err, ShouldBeNil)
			//wait a bit to be sure it reached us
			time.Sleep(100 * time.Millisecond)
			So(called, ShouldBeTrue)
			So(err, ShouldBeNil)

			called = false
			var args = make([]interface{}, 2)
			handlerTwoArgs := func(evt *wamp.Event) {
				called = true
				if len(evt.Arguments) != 2 {
					err = fmt.Errorf("Argument number wrong")
					return
				}
				if len(evt.ArgumentsKw) != 0 {
					err = fmt.Errorf("Keywork arguments provided for event, which is not supportet")
					return
				}
				args[0] = evt.Arguments[0]
				args[1] = evt.Arguments[1]
			}
			testClient.Subscribe("ocp.test.content.Test.TestEventTwoArgs", handlerTwoArgs, make(wamp.Dict, 0))
			_, err = ds.dmlState.dml.RunJavaScript(ds.dmlState.store, "TestUser", "Test.TestEventTwoArgs.Emit(\"Hello\", 42)")
			So(err, ShouldBeNil)
			//wait a bit to be sure it reached us
			time.Sleep(50 * time.Millisecond)
			So(called, ShouldBeTrue)
			So(err, ShouldBeNil)
			So(args[0], ShouldEqual, "Hello")
			So(args[1], ShouldEqual, 42)
		})

		Convey("methods are callable by clients,", func() {

			opts := make(wamp.Dict, 0)
			res, err := testClient.Call(ctx, "ocp.test.content.Test.TestFncZeroArgs", opts, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldEqual, 1)
			err, ok := res.Arguments[0].(error)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			time.Sleep(100 * time.Millisecond)

			val, _ := ds.dmlState.dml.Call(ds.dmlState.store, dml.User("test"), "Test.testI")
			So(val, ShouldEqual, 0)

			_, err = testClient.Call(ctx, "ocp.test.content.Test.TestFncTwoArgs", opts, wamp.List{1, 2}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			time.Sleep(100 * time.Millisecond)

			val, _ = ds.dmlState.dml.Call(ds.dmlState.store, dml.User("test"), "Test.testI")
			So(val, ShouldEqual, 3)

			val, err = testClient.Call(ctx, "ocp.test.content.Test.TestList", opts, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			args := val.(*wamp.Result)
			So(args.Arguments[0], ShouldResemble, []interface{}{})

			testClient.Call(ctx, "ocp.test.content.Test.Graph.AddNode", opts, wamp.List{"node"}, wamp.Dict{}, nil)
			val, err = testClient.Call(ctx, "ocp.test.content.Test.Graph.ToNode", opts, wamp.List{"node"}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			args = val.(*wamp.Result)
			So(args.Arguments[0], ShouldResemble, []interface{}{})
		})

		Convey("javascript code can be excecuted,", func() {

			code := `Test.testI = 120`

			opts := make(wamp.Dict, 0)
			res, err := testClient.Call(ctx, "ocp.test.execute", opts, wamp.List{code}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldEqual, 1)
			err, ok := res.Arguments[0].(error)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			time.Sleep(100 * time.Millisecond)

			val, _ := ds.dmlState.dml.Call(ds.dmlState.store, dml.User("test"), "Test.testI")
			So(val, ShouldEqual, 120)
		})

		Convey("and properties are read/writable,", func() {

			opts := make(wamp.Dict, 0)
			res, err := testClient.Call(ctx, "ocp.test.content.Test.testI", opts, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldEqual, 1)
			err, ok := res.Arguments[0].(error)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			So(res.Arguments[0], ShouldEqual, 1)

			res, err = testClient.Call(ctx, "ocp.test.content.Test.testI", opts, wamp.List{42}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldEqual, 1)
			err, ok = res.Arguments[0].(error)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			So(res.Arguments[0], ShouldEqual, 42)

			res, err = testClient.Call(ctx, "ocp.test.content.Test.testI", opts, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldEqual, 1)
			err, ok = res.Arguments[0].(error)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			So(res.Arguments[0], ShouldEqual, 42)
		})

		Convey("Also access via identifier work", func() {

			opts := make(wamp.Dict, 0)
			res, err := testClient.Call(ctx, "ocp.test.content.Test.Vector.AppendNew", opts, wamp.List{}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)

			id := res.Arguments[0].(string)
			uri := "ocp.test.content." + id + ".testI"
			res, err = testClient.Call(ctx, uri, opts, wamp.List{20}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)
			So(len(res.Arguments), ShouldEqual, 1)
			err, ok := res.Arguments[0].(error)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
			So(res.Arguments[0], ShouldEqual, 20)

			val, _ := ds.dmlState.dml.Call(ds.dmlState.store, dml.User("test"), id+".testI")
			So(val, ShouldEqual, 20)
		})
	})
}

func TestDatastructureData(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "datastructure")
	defer os.RemoveAll(path)

	Convey("Setting up a new datastructure works", t, func() {

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
		host.Connect(context.Background(), baseHost.ID(), false)

		//and for the swarm!
		id := uuid.NewV4().String()
		swarmpath := filepath.Join(host.GetPath(), id)
		os.MkdirAll(swarmpath, os.ModePerm)

		//setup the dml file to be accessbile for the datastrcuture
		dmlpath := filepath.Join(swarmpath, "Dml")
		os.MkdirAll(dmlpath, os.ModePerm)
		ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlDsContent), os.ModePerm)

		ds, err := NewDatastructure(swarmpath, "ocp.test", dsClient)
		So(err, ShouldBeNil)
		So(ds, ShouldNotBeNil)
		defer ds.Close()

		swarm, err := host.CreateSwarmWithID(context.Background(), p2p.SwarmID(id), p2p.SwarmStates(ds.GetState()))
		So(err, ShouldBeNil)
		ds.Start(swarm)
		defer swarm.Close(context.Background())

		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		Convey("RawData object can load file data", func() {

			//generate a testfile
			filedata := p2p.RepeatableData(555)
			testfilepath := filepath.Join(path, "testfile")
			ioutil.WriteFile(testfilepath, filedata, 0644)

			//load into object
			opts := make(wamp.Dict, 0)
			_, err := testClient.Call(ctx, "ocp.test.raw.Test.RawData.SetByPath", opts, wamp.List{testfilepath}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)

			Convey("which makes the properties cid Defined()", func() {

				res, err := testClient.Call(ctx, "ocp.test.content.Test.RawData", opts, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				strid, ok := res.Arguments[0].(string)
				So(ok, ShouldBeTrue)
				cid, err := utils.CidDecode(strid)
				So(err, ShouldBeNil)
				So(cid.Defined(), ShouldBeTrue)
			})

			Convey("and lets access and use the cid", func() {

				code := `Test.RawData2 = Test.RawData`
				_, err := testClient.Call(ctx, "ocp.test.execute", opts, wamp.List{code}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)

				res1, err := testClient.Call(ctx, "ocp.test.content.Test.RawData", opts, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				res2, err := testClient.Call(ctx, "ocp.test.content.Test.RawData2", opts, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				So(res1.Arguments[0], ShouldResemble, res2.Arguments[0])
				So(res1.Arguments, ShouldNotEqual, "")
			})

			Convey("The data can be taken out of the datastructure into a file again", func() {

				os.Remove(testfilepath)
				filepath := filepath.Join(path, "resultfile")
				res, err := testClient.Call(ctx, "ocp.test.raw.Test.RawData.WriteIntoPath", opts, wamp.List{filepath}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)

				stat, err := os.Stat(res.Arguments[0].(string))
				So(err, ShouldBeNil)
				So(stat.Size(), ShouldEqual, 555)
			})
		})

		Convey("RawData object can also receive stream data", func() {

			//generate a testfile and make it readable by uri
			filedata := p2p.RepeatableData(4.2e6)
			handler := func(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {
				if len(inv.Arguments) != 1 {
					return nxclient.InvokeResult{Args: wamp.List{"Wrong number of arguments in binary fetch"}, Err: wamp.URI("ocp.error")}
				}
				arg, ok := inv.Arguments[0].(int)
				if !ok {
					return nxclient.InvokeResult{Args: wamp.List{"Wrong type sent as argument in binary fetch"}, Err: wamp.URI("ocp.error")}
				}
				if arg != 42 {
					return nxclient.InvokeResult{Args: wamp.List{"Wrong argument number in binary fetch"}, Err: wamp.URI("ocp.error")}
				}

				//send the data
				for i := 0; i < 5; i++ {

					start := int64(i) * 1e6
					end := int64(i+1) * 1e6
					if end > int64(len(filedata)) {
						end = int64(len(filedata))
					}

					testClient.SendProgress(ctx, wamp.List{filedata[start:end]}, nil)
				}
				//finish
				return nxclient.InvokeResult{}
			}
			testClient.Register("fc.myfiles.load", handler, wamp.Dict{})

			//load into object
			opts := make(wamp.Dict, 0)
			_, err := testClient.Call(ctx, "ocp.test.raw.Test.RawData.SetByBinary", opts, wamp.List{"fc.myfiles.load", 42}, wamp.Dict{}, nil)
			So(err, ShouldBeNil)

			Convey("which markes the stored cid Defined() in dml", func() {

				res, err := testClient.Call(ctx, "ocp.test.content.Test.RawData", opts, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				strid, ok := res.Arguments[0].(string)
				So(ok, ShouldBeTrue)
				cid, err := utils.CidDecode(strid)
				So(err, ShouldBeNil)
				So(cid.Defined(), ShouldBeTrue)
			})

			Convey("and lets access and use the cid", func() {

				code := `Test.RawData2 = Test.RawData`
				_, err := testClient.Call(ctx, "ocp.test.execute", opts, wamp.List{code}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)

				res1, err := testClient.Call(ctx, "ocp.test.content.Test.RawData", opts, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				res2, err := testClient.Call(ctx, "ocp.test.content.Test.RawData2", opts, wamp.List{}, wamp.Dict{}, nil)
				So(err, ShouldBeNil)
				So(res1.Arguments[0], ShouldResemble, res2.Arguments[0])
				So(res1.Arguments, ShouldNotEqual, "")
			})

			Convey("The data can be taken out of the datastructure by stream", func() {

				received := make([]byte, 0)
				handler := func(result *wamp.Result) {
					data := result.Arguments[0].([]byte)
					received = append(received, data...)
				}

				_, err := testClient.Call(ctx, "ocp.test.raw.Test.RawData.ReadBinary", opts, wamp.List{}, wamp.Dict{}, handler)
				So(err, ShouldBeNil)
				So(bytes.Equal(filedata, received), ShouldBeTrue)
			})
		})
	})
}
