package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
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

	self.replica.logger.Debugf("Asked for log %v", idx)

	//check if the replica has the required log (logstore is threadsafe)
	log, err := self.replica.logs.GetLog(idx)

	if err != nil {
		return utils.StackError(err, "Log not available")
	}

	*result = log
	return nil
}

//receives a new log created by the leader
func (self *ReadAPI) NewLog(log Log) {

	self.replica.commitChan <- log
}

func (self *WriteAPI) RequestCommand(ctx context.Context,
	args struct {
		state uint8
		cmd   []byte
	}, result *uint64) error {

	ret := make(chan error)

	cmd := cmdStruct{
		cmd:     args.cmd,
		state:   args.state,
		retChan: ret,
		local:   false,
		ctx:     ctx,
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("Unable to commit command: aborted")

	case self.replica.cmdChan <- cmd:
		break
	}

	err := <-ret
	close(ret)

	return err
}
