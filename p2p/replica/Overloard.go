package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"

	crypto "github.com/libp2p/go-libp2p-crypto"
)

/*The overloard is the holy untouchable source of truth. It handles the leader
  election and provides information about leaders that is needed in the replicas
  to detect if information is valid or not*/
type Overload interface {

	//Register a OverloardAPI so that the overloard can interact with the replica
	RegisterOverloardAPI(api OverloardAPI) error

	//request a new leader election starts
	RequestNewLeader()

	//gather information for a certain epoch
	GetCurrentEpoch() uint64
	GetLeaderDataForEpoch(uint64) (Address, crypto.RsaPublicKey)

	//Handling of destructive replicas
	//ReportReplica(addr Address, readon string)
}

/*The API the overloard is using to communicate with us*/
type OverloardAPI struct {
	replica *Replica
}

func (self *OverloardAPI) GetHighestLogIndex(ctx context.Context, empty struct{}, result *uint64) error {

	//logstore is threadsafe
	idx, err := self.replica.logs.LastIndex()
	if err != nil {
		return utils.StackError(err, "Unable to access latest log")
	}

	*result = idx
	return nil
}

func (self *OverloardAPI) SetAsLeader(ctx context.Context, epoch uint64) error {

	//	self.replica
	return nil
}

func (self *OverloardAPI) StartNewEpoch(ctx context.Context, epoch uint64, leader Address, key crypto.RsaPublicKey) error {

	return nil
}

type testOverlord struct {
	apis   []OverloardAPI
	epoche uint64
	leader map[uint64]Address
	key    map[uint64]crypto.RsaPublicKey
}

func newTestOverlord() *testOverlord {
	return &testOverlord{
		apis:   make([]OverloardAPI, 0),
		epoche: 0,
		leader: make(map[uint64]Address),
		key:    make(map[uint64]crypto.RsaPublicKey, 0),
	}
}

func (self *testOverlord) RegisterOverloardAPI(api OverloardAPI) error {
	self.apis = append(self.apis, api)
	return nil
}

func (self *testOverlord) RequestNewLeader() {

}

func (self *testOverlord) GetCurrentEpoch() uint64 {
	return self.epoche
}

func (self *testOverlord) GetLeaderDataForEpoch(epoche uint64) (Address, crypto.RsaPublicKey, error) {

	if epoche > self.epoche {
		return Address(""), crypto.RsaPublicKey{}, fmt.Errorf("Epoch unknown")
	}
	return self.leader[epoche], self.key[epoche], nil
}
