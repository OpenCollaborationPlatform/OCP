package replica

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
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

	num := 3

	Convey("Setting up 3 replicas with basic state", t, func() {

		reps, err := setupReplicas(3, path, "base")
		defer closeReplicas(reps)
		So(err, ShouldBeNil)

		states := make([]*testState, len(reps))
		for i, rep := range reps {
			st := newTestState()
			states[i] = st
			So(rep.AddState(st), ShouldEqual, 0)
		}

		Convey("Adding commits to all replicas should work", func() {

			for i := 0; i < num; i++ {
				log := Log{Index: uint64(i), Epoch: 0, Type: 0, Data: intToByte(uint64(i))}

				for j := 0; j < len(reps); j++ {
					reps[j].commitLog(log)
				}
			}

			for _, state := range states {
				So(len(state.Value), ShouldEqual, num)
			}
			So(areStatesEqual(states), ShouldBeTrue)
		})

		Convey("and letting a replica fetch missing logs works", func() {

			//the las replica does not get any logs
			for i := 0; i < num; i++ {
				log := Log{Index: uint64(i), Epoch: 0, Type: 0, Data: intToByte(uint64(i))}

				for j := 0; j < (len(reps) - 1); j++ {
					reps[j].commitLog(log)
				}
			}

			//we now add only the last log to the last replica
			log := Log{Index: uint64(num - 1), Epoch: 0, Type: 0, Data: intToByte(uint64(num - 1))}
			reps[len(reps)-1].commitLog(log)
			time.Sleep(50 * time.Millisecond)

			for _, state := range states {
				So(len(state.Value), ShouldEqual, num)
			}
			So(areStatesEqual(states), ShouldBeTrue)
		})

	})
}
