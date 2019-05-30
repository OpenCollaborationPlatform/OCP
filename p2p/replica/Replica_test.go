package replica

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-crypto"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	logging.SetDebugLogging()
}

func setupReplicas(num uint, path string, name string) ([]*Replica, error) {

	fmt.Printf("\n\n\n Setup Replicas \n\n\n")

	trans := newTestTransport()
	overlord := newTestOverlord()

	replicas := make([]*Replica, num)

	names := make([]string, len(replicas))
	for i := 0; i < int(num); i++ {

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		priv, pub, err := crypto.GenerateRSAKeyPair(512, r)
		if err != nil {
			return nil, err
		}
		rep, err := NewReplica(path, fmt.Sprintf("%s_%v", name, i), trans, overlord, *priv.(*crypto.RsaPrivateKey))
		if err != nil {
			return nil, err
		}

		//register the key with the overlord
		overlord.setApiPubKey(*pub.(*crypto.RsaPublicKey))

		names[i] = rep.name
		replicas[i] = rep

		rep.Start()
	}

	return replicas, nil
}

func closeReplicas(reps []*Replica) {

	for _, rep := range reps {
		rep.Stop()
	}
}

//this function waits till all replicas commit the given idx or the timeout is reached
//on timeout a error is returned
func waitTillCommitIdx(reps []*Replica, commit uint64, timeout time.Duration) chan error {

	ret := make(chan error)

	go func() {
		wait := sync.WaitGroup{}
		start := time.Now()

		for _, rep := range reps {
			wait.Add(1)
			go func(rep *Replica) {
				closer := time.NewTimer(timeout)
			loop:
				for {
					select {
					case <-closer.C:
						break loop
					case idx := <-rep.appliedChan:
						if idx >= commit {
							break loop
						}
					}
				}
				wait.Done()

			}(rep)
		}

		wait.Wait()
		end := time.Now()

		if end.Sub(start) > timeout {
			ret <- fmt.Errorf("Timout occured: Not all replicas received commit")
		}
		ret <- nil
		close(ret)
	}()

	time.Sleep(10 * time.Millisecond)
	return ret
}

func areStatesEqual(st []*testState) bool {

	for i := 0; i < (len(st) - 1); i++ {

		for j := i + 1; j < len(st); j++ {

			if !st[i].Equals(st[j]) {
				return false
			}
		}
	}
	return true
}

func TestReplicaCommit(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "replica")
	defer os.RemoveAll(path)

	num := 3

	Convey("Setting up 3 replicas with basic state", t, func() {

		reps, err := setupReplicas(3, path, "Replica")
		defer closeReplicas(reps)
		So(err, ShouldBeNil)

		states := make([]*testState, len(reps))
		for i, rep := range reps {
			st := newTestState()
			states[i] = st
			So(rep.AddState(st), ShouldEqual, 0)
		}

		Convey("Adding commits to all replicas should work", func() {

			waiter := waitTillCommitIdx(reps, uint64(num-1), 1*time.Second)
			for i := 0; i < num; i++ {
				log := Log{Index: uint64(i), Epoch: 0, Type: 0, Data: intToByte(uint64(i))}

				for j := 0; j < len(reps); j++ {
					reps[j].commitLog(log)
				}
			}
			So(<-waiter, ShouldBeNil)

			for _, state := range states {
				So(len(state.Value), ShouldEqual, num)
			}
			So(areStatesEqual(states), ShouldBeTrue)
		})

		Convey("and letting a replica fetch missing logs works", func() {

			//the las replica does not get any logs
			waiter := waitTillCommitIdx(reps, uint64(num-1), 1*time.Second)

			for i := 0; i < num; i++ {
				log := Log{Index: uint64(i), Epoch: 0, Type: 0, Data: intToByte(uint64(i))}

				for j := 0; j < (len(reps) - 1); j++ {
					reps[j].commitLog(log)
				}
			}

			//we now add only the last log to the last replica
			log := Log{Index: uint64(num - 1), Epoch: 0, Type: 0, Data: intToByte(uint64(num - 1))}
			reps[len(reps)-1].commitLog(log)

			So(<-waiter, ShouldBeNil)

			for _, state := range states {
				So(len(state.Value), ShouldEqual, num)
			}
			So(areStatesEqual(states), ShouldBeTrue)
		})

	})
}

func TestReplicaLeader(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "replica")
	defer os.RemoveAll(path)

	Convey("Setting up 3 replicas with basic state", t, func() {

		reps, err := setupReplicas(3, path, "Replica")
		defer closeReplicas(reps)
		So(err, ShouldBeNil)

		states := make([]*testState, len(reps))
		for i, rep := range reps {
			st := newTestState()
			states[i] = st
			So(rep.AddState(st), ShouldEqual, 0)
		}

		Convey("Adding commits to replica after setup", func() {
			ctx := context.Background()

			waiter := waitTillCommitIdx(reps, 0, 1*time.Second)
			err := reps[0].AddCommand(ctx, 0, intToByte(uint64(0)))
			So(err, ShouldBeNil)
			So(<-waiter, ShouldBeNil)

			Convey("a new epoch should have startet,", func() {
				epoch, err := reps[0].overlord.GetCurrentEpoch()
				So(err, ShouldBeNil)
				So(epoch, ShouldEqual, 0)
			})

			Convey("all replicas know about it,", func() {
				for _, rep := range reps {
					So(rep.leaders.EpochCount(), ShouldBeGreaterThan, 0)
					So(rep.leaders.HasEpoch(0), ShouldBeTrue)
				}
			})

			Convey("and all states should have received the commit", func() {
				for _, state := range states {
					So(len(state.Value), ShouldEqual, 1)
				}
				So(areStatesEqual(states), ShouldBeTrue)
			})
		})

	})
}
