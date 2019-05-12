package replica

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func setupReplicas(num uint, path string) ([]*Replica, error) {

	trans := &testTransport{delay: 0 * time.Millisecond, followerAPIs: make(map[Follower]FollowerAPI)}
	replicas := make([]*Replica, num)

	names := make([]string, len(replicas))
	for i := 0; i < int(num); i++ {

		rep, err := NewReplica(path, strconv.Itoa(i), trans)
		if err != nil {
			return nil, err
		}

		names[i] = rep.name
		replicas[i] = rep
		trans.followerAPIs[rep.name] = rep.CreateFollowerAPI()

		rep.Start()
	}

	//connect them all!
	for _, rep := range replicas {
		rep.followers = append(rep.followers, names...)
	}

	return replicas, nil
}

func closeReplicas(reps []*Replica) {

	for _, rep := range reps {
		rep.Stop()
	}
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

func TestReplicaBasics(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "replica")
	defer os.RemoveAll(path)

	Convey("Setting up 3 replicas with basic state", t, func() {

		reps, err := setupReplicas(3, path)
		defer closeReplicas(reps)
		So(err, ShouldBeNil)

		states := make([]*testState, len(reps))
		for i, rep := range reps {
			st := newTestState()
			states[i] = st
			So(rep.AddState(st), ShouldEqual, 0)
		}

		Convey("Adding commits to all replicas should work", func() {

			for i := 0; i < 10; i++ {
				log := Log{Index: uint64(i), Epoch: 0, Type: 0, Data: intToByte(uint64(i))}

				for j := 0; j < len(reps); j++ {
					reps[j].commitLog(log)
				}
			}

			So(areStatesEqual(states), ShouldBeTrue)
		})

		Convey("and letting a replica fetch missing logs works", func() {

			//the las replica does not get any logs
			for i := 0; i < 10; i++ {
				log := Log{Index: uint64(i), Epoch: 0, Type: 0, Data: intToByte(uint64(i))}

				for j := 0; j < (len(reps) - 1); j++ {
					reps[j].commitLog(log)
				}
			}

			//we now add only the last log to the last replica
			log := Log{Index: uint64(9), Epoch: 0, Type: 0, Data: intToByte(uint64(9))}
			reps[len(reps)-1].commitLog(log)

			So(areStatesEqual(states), ShouldBeTrue)
		})

	})
}
