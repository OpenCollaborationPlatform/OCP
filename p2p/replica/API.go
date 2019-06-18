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

//returns what we think have as log for the given idx, or a snapshot that superseeds this log index
func (self *ReadAPI) GetLog(ctx context.Context, idx uint64, result *Log) error {

	self.replica.logger.Debugf("Asked for log %v", idx)

	//should we go for the snapshot?
	first, err := self.replica.logs.FirstIndex()
	if err != nil {
		return err
	}
	if first != 0 && first >= idx {
		snap, err := self.replica.logs.GetLog(first)
		if err != nil {
			return err
		}
		*result = snap
		return nil
	}

	//check if the replica has the required log
	log, err := self.replica.logs.GetLog(idx)

	if err != nil {
		return utils.StackError(err, "Log not available")
	}

	*result = log
	return nil
}

//returns what we think have as log for the given idx
func (self *ReadAPI) GetNewestLog(ctx context.Context, empty struct{}, result *Log) error {

	self.replica.logger.Debugf("Asked for newest log")

	//check if the replica has the required log (logstore is threadsafe)
	last, err := self.replica.logs.LastIndex()
	if err != nil {
		return utils.StackError(err, "Logs cannot be accessed")
	}

	log, err := self.replica.logs.GetLog(last)
	if err != nil {
		return utils.StackError(err, "Log not available")
	}

	*result = log
	return nil
}

//receives a new log created by the leader
func (self *ReadAPI) NewLog(log Log) {

	self.replica.commitChan <- commitStruct{log, nil}
}

//receives a beacon from the leader
func (self *ReadAPI) NewBeacon(beacon beaconStruct) {

	self.replica.beaconChan <- beacon
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

	if err != nil {
		self.replica.logger.Debugf("Unable to handle command request: %v", err)
	}

	return err
}
