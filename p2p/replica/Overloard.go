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
	RegisterOverloardAPI(api *OverloardAPI) error

	//request a new leader election starts
	RequestNewLeader(ctx context.Context) error

	//gather information for a certain epoch
	GetCurrentEpoch() (uint64, error)
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

		*result = 0
		if IsNoEntryError(err) {
			return err

		} else {
			return utils.StackError(err, "Unable to access latest log")
		}
	}

	*result = idx
	return nil
}

func (self *OverloardAPI) SetAsLeader(ctx context.Context, epoch uint64) error {

	select {
	case self.replica.setLeaderChan <- epoch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("Unable to set as leader: operation canceld")
	}
}

func (self *OverloardAPI) StartNewEpoch(ctx context.Context, epoch uint64, leader Address, key crypto.RsaPublicKey) error {

	self.replica.logger.Infof("Start new epoch %v with leader %v", epoch, leader)
	self.replica.leaders.AddEpoch(epoch, leader, key)
	return nil
}

type testOverlord struct {
	apis       []*OverloardAPI
	epoche     uint64
	leader     leaderStore
	apimutex   sync.RWMutex
	isElecting bool
	apiKeys    []crypto.RsaPublicKey
}

func newTestOverlord() *testOverlord {
	return &testOverlord{
		apis:       make([]*OverloardAPI, 0),
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

func (self *testOverlord) RegisterOverloardAPI(api *OverloardAPI) error {
	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	self.apis = append(self.apis, api)
	return nil
}

func (self *testOverlord) RequestNewLeader(ctx context.Context) error {

	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	if len(self.apis) <= 0 {
		ologger.Error("Request for new leader received, but no API is registered")
		return fmt.Errorf("failed")
	}

	//we may already have started an election
	if self.isElecting {
		ologger.Info("Request for leader election, but already ongoing")
		return fmt.Errorf("failed")
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
		if err == nil || IsNoEntryError(err) {
			if res >= max {
				max = res
				addr = i
			}
		} else {
			ologger.Errorf("Failed to reach api %v: %v", i, err)
		}
	}

	if addr < 0 {
		ologger.Error("No API reached, no leader can be sellected")
		return fmt.Errorf("failed")
	}

	//setup the new epoch (make sure we start with 0 instead of 1)
	if self.leader.EpochCount() != 0 {
		self.epoche++
	}
	self.leader.AddEpoch(self.epoche, fmt.Sprintf("%v", addr), self.apiKeys[addr])

	//communicate the result
	ologger.Infof("Leader elected for epoch %v: %v", self.epoche, addr)
	self.apis[addr].SetAsLeader(ctx, self.epoche)

	idxs := rand.Perm(len(self.apis))
	for _, idx := range idxs {
		go func(api *OverloardAPI) {
			addr, _ := self.leader.GetLeaderAdressForEpoch(self.epoche)
			key, _ := self.leader.GetLeaderKeyForEpoch(self.epoche)
			api.StartNewEpoch(ctx, self.epoche, addr, key)
		}(self.apis[idx])
	}
	time.Sleep(100 * time.Millisecond)

	return nil
}

func (self *testOverlord) GetCurrentEpoch() (uint64, error) {

	self.apimutex.RLock()
	defer self.apimutex.RUnlock()

	return self.epoche, nil
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
