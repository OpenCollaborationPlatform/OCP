package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"math/rand"
	"sync"

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

	//request a new leader election for a certain epoch
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
	idx, err := self.replica.store.LastIndex()
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

func (self *OverloardAPI) SetAsLeader(ctx context.Context, epoch uint64, idx uint64) error {

	//do we have this already?
	if self.replica.leaders.HasEpoch(epoch) {
		return nil
	}

	//check if we can simply add!
	if (epoch == 0 && self.replica.leaders.EpochCount() == 0) ||
		(epoch != 0) && self.replica.leaders.HasEpoch(epoch-1) {

		log, err := self.replica.store.GetLatestLog()
		if ((err == nil) && (log.Epoch == epoch-1)) ||
			IsNoEntryError(err) {

			err := self.replica.leaders.AddEpoch(epoch, self.replica.address, self.replica.pubKey, idx)
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
	}

	//it seems something is wrong... start recovery!
	//--> not blocking, as it can sync with AddCommand if it requests a new leader which becomes the node itself
	select {
	case self.replica.recoverChan <- recoverStruct{epoch, ctx}:
	default:
	}

	return nil
}

//Note: This can block, so use it in events only!
func (self *OverloardAPI) StartNewEpoch(epoch uint64, leader Address, key crypto.RsaPublicKey, startIdx uint64) error {

	self.replica.logger.Infof("Start new epoch %v with leader %v (begining with index %v)", epoch, leader, startIdx)

	//nothing to be done if we have it already
	if self.replica.leaders.HasEpoch(epoch) {
		return nil
	}

	//check if we can add directly or if we need recover!
	add := false
	if (epoch == 0 && self.replica.leaders.EpochCount() == 0) ||
		(epoch != 0) && self.replica.leaders.HasEpoch(epoch-1) {

		log, err := self.replica.store.GetLatestLog()
		if err != nil {
			add = false

		} else if IsNoEntryError(err) {
			add = true

		} else if (log.Epoch == epoch-1) && (log.Index == startIdx-1) {
			add = true
		}
	}

	if add {
		err := self.replica.leaders.AddEpoch(epoch, leader, key, startIdx)
		if err != nil {
			self.replica.logger.Errorf("Unable to add new epoch: %v", err)
			return err
		}
		err = self.replica.leaders.SetEpoch(epoch)
		if err != nil {
			self.replica.logger.Errorf("Unable to set new epoch: %v", err)
			return err
		}
		return nil

	} else {

		//it seems something is wrong... start recovery!
		//--> not blocking, as it can sync with AddCommand if it requests a new leader which becomes the node itself
		self.replica.recoverChan <- recoverStruct{epoch, nil}
	}
	return nil
}

type TestOverlord struct {
	apis        []*OverloardAPI
	leader      leaderStore
	apimutex    sync.RWMutex
	apiKeys     []crypto.RsaPublicKey
	apiAddr     []Address
	unreachable []int
	urmutex     sync.RWMutex //for uneachable
}

func NewTestOverlord() *TestOverlord {
	return &TestOverlord{
		apis:    make([]*OverloardAPI, 0),
		leader:  newLeaderStore(),
		apiKeys: make([]crypto.RsaPublicKey, 0),
		apiAddr: make([]Address, 0),
	}
}

//for testing only, not a interface function
func (self *TestOverlord) SetApiData(addr Address, key crypto.RsaPublicKey) error {
	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	self.apiKeys = append(self.apiKeys, key)
	self.apiAddr = append(self.apiAddr, addr)
	return nil
}

func (self *TestOverlord) Clear() {
	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	self.leader = newLeaderStore()
	self.apis = make([]*OverloardAPI, 0)
	self.apiKeys = make([]crypto.RsaPublicKey, 0)
	self.apiAddr = make([]Address, 0)
}

func (self *TestOverlord) isReachable(idx int) bool {

	self.urmutex.RLock()
	defer self.urmutex.RUnlock()

	for _, val := range self.unreachable {
		if val == idx {
			return false
		}
	}
	return true
}

func (self *TestOverlord) RegisterOverloardAPI(api *OverloardAPI) error {
	self.apimutex.Lock()
	defer self.apimutex.Unlock()

	self.apis = append(self.apis, api)
	return nil
}

//does not return an error if the epoch has already been elected
func (self *TestOverlord) RequestNewLeader(ctx context.Context, epoch uint64) error {

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
	addr := Address("")
	idx := -1
	start := uint64(0)
	for i, api := range self.apis {

		if !self.isReachable(i) {
			continue
		}

		var res uint64
		err := api.GetHighestLogIndex(ctx, &res)
		if IsNoEntryError(err) {
			if max == 0 {
				addr = self.apiAddr[i]
				idx = i
			}
		} else if err == nil {
			if res >= max {
				max = res
				addr = self.apiAddr[i]
				idx = i
				start = res + 1
			}
		} else {
			ologger.Errorf("Failed to reach api %v: %v", i, err)
		}
	}

	if addr == Address("") {
		ologger.Error("No API reached, no leader can be selected")
		return fmt.Errorf("failed")
	}

	//setup the new epoch (make sure we start with 0 instead of 1)
	var newEpoch uint64
	if self.leader.EpochCount() != 0 {
		newEpoch = self.leader.GetEpoch() + 1
	} else {
		newEpoch = 0
	}
	self.leader.AddEpoch(newEpoch, addr, self.apiKeys[idx], start)
	self.leader.SetEpoch(newEpoch)

	//communicate the result
	ologger.Infof("Leader elected for epoch %v: %v (start index is %v)", self.leader.GetEpoch(), self.leader.GetLeaderAddress(), self.leader.GetLeaderStartIdx())
	self.apis[idx].SetAsLeader(ctx, self.leader.GetEpoch(), self.leader.GetLeaderStartIdx())

	idxs := rand.Perm(len(self.apis))
	for _, idx := range idxs {
		if !self.isReachable(idx) {
			continue
		}

		go func(api *OverloardAPI) {
			addr := self.leader.GetLeaderAddress()
			key := self.leader.GetLeaderKey()
			sidx := self.leader.GetLeaderStartIdx()
			api.StartNewEpoch(self.leader.GetEpoch(), addr, key, sidx)
		}(self.apis[idx])
	}

	return nil
}

func (self *TestOverlord) GetCurrentEpoch(ctx context.Context) (uint64, error) {

	return self.leader.GetEpoch(), nil
}

func (self *TestOverlord) GetCurrentEpochData(ctx context.Context) (uint64, Address, crypto.RsaPublicKey, uint64, error) {

	if self.leader.EpochCount() == 0 {
		return 0, Address(""), crypto.RsaPublicKey{}, 0, &NoEpochError{}
	}

	addr := self.leader.GetLeaderAddress()
	key := self.leader.GetLeaderKey()
	sidx := self.leader.GetLeaderStartIdx()

	return self.leader.GetEpoch(), addr, key, sidx, nil
}

func (self *TestOverlord) GetDataForEpoch(ctx context.Context, epoche uint64) (Address, crypto.RsaPublicKey, uint64, error) {

	if epoche > self.leader.GetEpoch() || !self.leader.HasEpoch(epoche) {
		return Address(""), crypto.RsaPublicKey{}, 0, fmt.Errorf("Epoch unknown")
	}

	addr := self.leader.GetLeaderAddress()
	key := self.leader.GetLeaderKey()
	sidx := self.leader.GetLeaderStartIdx()

	return addr, key, sidx, nil
}
