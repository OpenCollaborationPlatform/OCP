package p2p

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	. "github.com/smartystreets/goconvey/convey"
)

//simple state machine that adds the cmd (a bytes version  of a int) to a value
type testFSM struct {
	value int64
}

func (self *testFSM) Apply(log *raft.Log) interface{} {

	vl, _ := binary.Varint(log.Data)
	self.value = self.value + vl
	return self.value
}

func (self *testFSM) Snapshot() (raft.FSMSnapshot, error) {

	fmt.Printf("\nSnapshot testFSM\n")
	return &testSnapshot{self.value}, nil
}

func (self *testFSM) Restore(reader io.ReadCloser) error {
	fmt.Printf("\nRestore testFSM\n")
	data, _ := ioutil.ReadAll(reader)
	val, _ := binary.Varint(data)
	self.value = val

	return nil
}

type testSnapshot struct {
	state int64
}

func (self *testSnapshot) Persist(sink raft.SnapshotSink) error {

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, self.state)
	_, err := sink.Write(buf[:n])

	return err
}

func (self *testSnapshot) Release() {}

func TestBasicSharedState(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	Convey("Setting up two random hosts with a swarm,", t, func() {

		swid := SwarmID("myswarm")

		h1, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h1.Stop()
		//create the swarm rigth away so that they get the correct directory to us
		sw1 := h1.CreateSwarm(swid)

		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		defer h2.Stop()
		sw2 := h2.CreateSwarm(swid)

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Sharing services shall be possible", func() {

			st1 := &testFSM{0}
			sst1, err := sw1.State.Share(st1, true)
			So(err, ShouldBeNil)
			So(sst1, ShouldNotBeNil)

			st2 := &testFSM{0}
			sst2, err := sw2.State.Share(st2, true)
			So(err, ShouldBeNil)
			So(sst2, ShouldNotBeNil)

			//start observations
			obsChan1 := make(chan raft.Observation)
			obsChan2 := make(chan raft.Observation)
			obsFunc := func(channel chan raft.Observation) {
				for {
					obs, more := <-channel
					if !more {
						fmt.Println("Drop observer")
						return
					}

					fmt.Printf("Received observation of type %T: %v\n", obs.Data, obs.Data)
				}
			}

			go obsFunc(obsChan1)
			go obsFunc(obsChan2)
			sst1.raftServer.RegisterObserver(raft.NewObserver(obsChan1, true, nil))
			sst2.raftServer.RegisterObserver(raft.NewObserver(obsChan2, true, nil))

			//wait for leader
			time.Sleep(2 * time.Second)
			fmt.Printf("State of server 1: %v\n", sst1.raftServer.State().String())
			fmt.Printf("State of server 2: %v\n", sst2.raftServer.State().String())
			fmt.Printf("Value of fsm1: %v\n", st1.value)
			fmt.Printf("Value of fsm2: %v\n", st2.value)

			cmd := make([]byte, binary.MaxVarintLen64)
			binary.PutVarint(cmd, 1)

			for i := 0; i < 100; i++ {
				future := sst1.raftServer.Apply(cmd, time.Second)
				if err := future.Error(); err != nil {
					fmt.Printf("Error with command: %v\n", err)
				}
			}
			time.Sleep(1 * time.Second)
			fmt.Printf("Value of fsm1: %v\n", st1.value)
			fmt.Printf("Value of fsm2: %v\n", st2.value)

			//try to connect
			serverID := raft.ServerID(sw2.host.ID().pid().Pretty())
			serverAdr := raft.ServerAddress(sw2.host.ID().pid().Pretty())
			future := sst1.raftServer.AddVoter(serverID, serverAdr, 0, 3*time.Second)
			err = future.Error()
			So(err, ShouldBeNil)

			time.Sleep(1 * time.Second)
			fmt.Printf("State of server 1: %v\n", sst1.raftServer.State().String())
			fmt.Printf("State of server 2: %v\n", sst2.raftServer.State().String())
			fmt.Printf("Value of fsm1: %v\n", st1.value)
			fmt.Printf("Value of fsm2: %v\n", st2.value)

			future = sst1.raftServer.Apply(cmd, time.Second)
			if err := future.Error(); err != nil {
				fmt.Printf("Error on apply: %v\n", err)
			}
			time.Sleep(100 * time.Millisecond)

			fmt.Printf("Value of fsm1: %v\n", st1.value)
			fmt.Printf("Value of fsm2: %v\n", st2.value)

			//drop one server and see reaction
			sst1.raftServer.Shutdown()
			time.Sleep(5 * time.Second)
			fmt.Printf("State of server 1: %v\n", sst1.raftServer.State().String())
			fmt.Printf("State of server 2: %v\n", sst2.raftServer.State().String())
			fmt.Printf("Value of fsm1: %v\n", st1.value)
			fmt.Printf("Value of fsm2: %v\n", st2.value)

		})
	})
}
