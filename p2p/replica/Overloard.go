package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-crypto"
)

var ologger = logging.Logger("Overlord")

/*The overloard is the holy untouchable source of truth. It handles the leader
  election and provides information about leaders that is needed in the replicas
  to detect if information is valid or not*/
type Overlord interface {

	//Register a OverloardAPI so that the overloard can interact with the replica
	RegisterOverloardAPI(api OverloardAPI) error

	//request a new leader election starts
	RequestNewLeader(ctx context.Context)

	//gather information for a certain epoch
	GetCurrentEpoch() uint64
	GetLeaderDataForEpoch(uint64) (Address, crypto.RsaPublicKey, error)

	//Handling of destructive replicas
	//ReportReplica(addr Address, readon string)
}

/*The API the overloard is using to communicate with us*/
type OverloardAPI struct {
	replica *Replica
}

func (self *OverloardAPI) GetHighestLogIndex(ctx context.Context, result *uint64) error {

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
	apis       []OverloardAPI
	epoche     uint64
	leader     leaderStore
	apimutex   sync.RWMutex
	isElecting bool
	apiKeys    []crypto.RsaPublicKey
}

func newTestOverlord() *testOverlord {
	return &testOverlord{
		apis:       make([]OverloardAPI, 0),
		epoche:     0,
		leader:     newLeaderStore(),
		isElecting: false,
		apiKeys:    make([]crypto.RsaPublicKey, 0),
	}
}

//for testing only, not a interface function
func (self *testOverlord) setApiPubKey(key crypto.RsaPublicKey) error {
	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	self.apiKeys = append(self.apiKeys, key)
	return nil
}

func (self *testOverlord) RegisterOverloardAPI(api OverloardAPI) error {
	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	self.apis = append(self.apis, api)
	return nil
}

func (self *testOverlord) RequestNewLeader(ctx context.Context) {

	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	if len(self.apis) <= 0 {
		ologger.Error("Request for new leader received, but no API is registered")
		return
	}

	//we may already have started an election
	if self.isElecting {
		ologger.Info("Request for leader election, but already ongoing")
		return
	}
	self.isElecting = true
	defer func() { self.isElecting = false }()

	ologger.Info("Start leader election")

	//collect the replica with highest log that is reachable
	max := uint64(0)
	addr := -1
	for i, api := range self.apis {
		nctx, _ := context.WithTimeout(ctx, 10*time.Millisecond)
		var res uint64
		err := api.GetHighestLogIndex(nctx, &res)
		if err == nil {
			if res > max {
				max = res
				addr = i
			}
		} else {
			ologger.Errorf("Failed to reach api %v", i)
		}
	}

	if addr < 0 {
		ologger.Error("No API reached, no leader can be sellected")
		return
	}

	//setup the new epoch (make sure we start with 0 instead of 1)
	if self.leader.EpochCount() != 0 {
		self.epoche++
	}
	self.leader.AddEpoch(self.epoche, fmt.Sprintf("%v", addr), self.apiKeys[addr])

	//communicate the result
	nctx, _ := context.WithTimeout(ctx, 10*time.Millisecond)
	self.apis[addr].SetAsLeader(nctx, self.epoche)

	idxs := rand.Perm(len(self.apis))
	for _, idx := range idxs {
		go func(api OverloardAPI) {
			nctx, _ := context.WithTimeout(ctx, 10*time.Millisecond)
			addr, _ := self.leader.GetLeaderAdressForEpoch(self.epoche)
			key, _ := self.leader.GetLeaderKeyForEpoch(self.epoche)
			api.StartNewEpoch(nctx, self.epoche, addr, key)
		}(self.apis[idx])
	}
}

func (self *testOverlord) GetCurrentEpoch() uint64 {

	self.apimutex.RLock()
	defer self.apimutex.RUnlock()

	return self.epoche
}

func (self *testOverlord) GetLeaderDataForEpoch(epoche uint64) (Address, crypto.RsaPublicKey, error) {

	self.apimutex.RLock()
	defer self.apimutex.RUnlock()

	if epoche > self.epoche || !self.leader.HasEpoch(epoche) {
		return Address(""), crypto.RsaPublicKey{}, fmt.Errorf("Epoch unknown")
	}

	addr, _ := self.leader.GetLeaderAdressForEpoch(self.epoche)
	key, _ := self.leader.GetLeaderKeyForEpoch(self.epoche)
	return addr, key, nil
}
