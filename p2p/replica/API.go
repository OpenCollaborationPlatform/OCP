package replica

import (
	"CollaborationNode/utils"
	"context"
)

/*API types: How replicas interact with each other. Those
  APIs must be callable with the replica transport
*/

//p2p rpc api for read only auth
type ReadAPI struct {
	replica *Replica
}

//p2p rpc api for read/write auth
type WriteAPI struct {
	replica *Replica
}

//returns what we think have as log for the given idx
func (self *ReadAPI) GetLog(ctx context.Context, idx uint64, result *Log) error {

	//check if the replica has the required log (logstore is threadsafe)
	log, err := self.replica.logs.GetLog(idx)

	if err != nil {
		return utils.StackError(err, "Log not available")
	}

	*result = log
	return nil
}
