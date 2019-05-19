package replica

import (
	"context"

	crypto "github.com/libp2p/go-libp2p-crypto"
)

/*The overloard is the holy untouchable source of truth. It handles the leader
  election and provides information about leaders that is needed in the replicas
  to detect if information is valid or not*/
type Overload interface {

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
