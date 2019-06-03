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

type NoEpochError struct{}

func (self *NoEpochError) Error() string {
	return "No epoch available"
}

func IsNoEpochError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*NoEpochError)
	return ok
}

/*The overloard is the holy untouchable source of truth. It handles the leader
  election and provides information about leaders that is needed in the replicas
  to detect if information is valid or not*/
type Overlord interface {

	//Register a OverloardAPI so that the overloard can interact with the replica
	RegisterOverloardAPI(api *OverloardAPI) error

	//request a new leader election starts
	RequestNewLeader(ctx context.Context, epoch uint64) error

	//gather information for a certain epoch
	GetCurrentEpoch(ctx context.Context) (uint64, error)
	GetCurrentEpochData(ctx context.Context) (uint64, Address, crypto.RsaPublicKey, uint64, error)
	GetDataForEpoch(ctx context.Context, epoch uint64) (Address, crypto.RsaPublicKey, uint64, error)

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

	last, _ := self.replica.logs.LastIndex()
	err := self.replica.leaders.AddEpoch(epoch, self.replica.address, self.replica.pubKey, last+1)
	if err != nil {
		self.replica.logger.Errorf("Set as leader by overlord, but cannot comply: %v", err)
		return err
	}
	err = self.replica.leaders.SetEpoch(epoch)
	if err != nil {
		self.replica.logger.Errorf("Set as leader by overlord, but cannot comply: %v", err)
		return err
	}
	return nil
}

func (self *OverloardAPI) StartNewEpoch(ctx context.Context, epoch uint64, leader Address, key crypto.RsaPublicKey, startIdx uint64) error {

	self.replica.logger.Infof("Start new epoch %v with leader %v", epoch, leader)
	self.replica.leaders.AddEpoch(epoch, leader, key, startIdx)
	return nil
}

type testOverlord struct {
	apis     []*OverloardAPI
	leader   leaderStore
	apimutex sync.RWMutex
	apiKeys  []crypto.RsaPublicKey
}

func newTestOverlord() *testOverlord {
	return &testOverlord{
		apis:    make([]*OverloardAPI, 0),
		leader:  newLeaderStore(),
		apiKeys: make([]crypto.RsaPublicKey, 0),
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

func (self *testOverlord) RequestNewLeader(ctx context.Context, epoch uint64) error {

	self.apimutex.RLock()
	defer self.apimutex.RUnlock()

	if len(self.apis) <= 0 {
		ologger.Error("Request for new leader received, but no API is registered")
		return fmt.Errorf("failed")
	}

	//check if the epoch is already elected
	if self.leader.HasEpoch(epoch) {
		return nil
	}

	//or not the next one
	if (self.leader.EpochCount() != 0) && ((epoch - self.leader.GetEpoch()) != 1) {
		return fmt.Errorf("The requested epoch %v is not the next in line, that would be %v", epoch, self.leader.GetEpoch()+1)
	}

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
	var newEpoch uint64
	if self.leader.EpochCount() != 0 {
		newEpoch = self.leader.GetEpoch() + 1
	} else {
		newEpoch = 0
	}
	self.leader.AddEpoch(newEpoch, fmt.Sprintf("%v", addr), self.apiKeys[addr], max)
	self.leader.SetEpoch(newEpoch)

	//communicate the result
	ologger.Infof("Leader elected for epoch %v: %v", self.leader.GetEpoch(), addr)
	self.apis[addr].SetAsLeader(ctx, self.leader.GetEpoch())

	idxs := rand.Perm(len(self.apis))
	for _, idx := range idxs {
		go func(api *OverloardAPI) {
			addr := self.leader.GetLeaderAddress()
			key := self.leader.GetLeaderKey()
			sidx := self.leader.GetLeaderStartIdx()
			api.StartNewEpoch(ctx, self.leader.GetEpoch(), addr, key, sidx)
		}(self.apis[idx])
	}
	time.Sleep(100 * time.Millisecond)

	return nil
}

func (self *testOverlord) GetCurrentEpoch(ctx context.Context) (uint64, error) {

	return self.leader.GetEpoch(), nil
}

func (self *testOverlord) GetCurrentEpochData(ctx context.Context) (uint64, Address, crypto.RsaPublicKey, uint64, error) {

	if self.leader.EpochCount() == 0 {
		return 0, Address(""), crypto.RsaPublicKey{}, 0, &NoEpochError{}
	}

	addr := self.leader.GetLeaderAddress()
	key := self.leader.GetLeaderKey()
	sidx := self.leader.GetLeaderStartIdx()

	return self.leader.GetEpoch(), addr, key, sidx, nil
}

func (self *testOverlord) GetDataForEpoch(ctx context.Context, epoche uint64) (Address, crypto.RsaPublicKey, uint64, error) {

	if epoche > self.leader.GetEpoch() || !self.leader.HasEpoch(epoche) {
		return Address(""), crypto.RsaPublicKey{}, 0, fmt.Errorf("Epoch unknown")
	}

	addr := self.leader.GetLeaderAddress()
	key := self.leader.GetLeaderKey()
	sidx := self.leader.GetLeaderStartIdx()

	return addr, key, sidx, nil
}
