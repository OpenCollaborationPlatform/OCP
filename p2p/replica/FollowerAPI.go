package replica

import (
	"CollaborationNode/utils"
	"context"
)

type FollowerAPI struct {
	replica *Replica
}

//func (t *T) MethodName(ctx context.Context, argType T1, replyType *T2) error

//returns what we think have as log for the given idx
func (self *FollowerAPI) GetLog(ctx context.Context, idx uint64, result *Log) error {

	//check if the replica has the required log (logstore is threadsafe)
	log, err := self.replica.logs.GetLog(idx)

	if err != nil {
		return utils.StackError(err, "Log not available")
	}

	*result = log
	return nil
}

/*
//returns the range of logs we have available. It includes the start and end idx
func (self *FollowerAPI) GetLogRange(start uint64, end uint64) ([]Log, error) {

	//check if we have the required logs (logstore is threadsafe)
	res := make([]Log, end-start+1)
	for i := start; i <= end; i++ {
		l, err := self.logs.GetLog(i)
		if err != nil {
			return nil, utils.StackError("Cannot collect log range")
		}
		res[i-start] = l
	}
	return self.logs.GetLog(idx)
}
*/
