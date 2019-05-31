package replica

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-crypto"
)

const (
	requestState_Fetching = iota
	requestState_Available

	logType_Leader       = math.MaxUint8
	logType_MemberAdd    = math.MaxUint8 - 1
	logType_MemberRemove = math.MaxUint8 - 2
	logType_Snapshot     = math.MaxUint8 - 3
)

func init() {
	rand.Seed(time.Now().Unix())
}

type NotLeaderError struct{}

func (self *NotLeaderError) Error() string {
	return "Not leader, cannot execute command"
}

func IsNotLeaderError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*NotLeaderError)
	return ok
}

type Replica struct {
	//init values
	transport Transport
	overlord  Overlord
	name      string
	privKey   crypto.RsaPrivateKey
	pubKey    crypto.RsaPublicKey
	logger    logging.EventLogger
	address   Address

	//replica state handling
	requests map[uint64]requestState
	leaders  leaderStore

	//user state handling
	states stateStore
	logs   logStore

	//syncronizing via channels
	ctx         context.Context
	cncl        context.CancelFunc
	commitChan  chan Log
	cmdChan     chan cmdStruct
	appliedChan chan uint64
}

func NewReplica(path string, name string, addr Address, trans Transport, ol Overlord,
	priv crypto.RsaPrivateKey, pub crypto.RsaPublicKey) (*Replica, error) {

	//create new logStore for this state
	store := newMemoryLogStore()
	/*if err != nil {
		return nil, utils.StackError(err, "Cannot create replica")
	}*/

	//setup the main context
	ctx, cncl := context.WithCancel(context.Background())

	replica := &Replica{
		transport:   trans,
		overlord:    ol,
		requests:    make(map[uint64]requestState),
		leaders:     newLeaderStore(),
		ctx:         ctx,
		cncl:        cncl,
		commitChan:  make(chan Log),
		cmdChan:     make(chan cmdStruct),
		appliedChan: make(chan uint64, 100),
		states:      newStateStore(),
		logs:        store,
		name:        name,
		privKey:     priv,
		pubKey:      pub,
		address:     addr,
		logger:      logging.Logger(name),
	}

	//setup transport correctly
	if err := trans.RegisterReadAPI(&ReadAPI{replica}); err != nil {
		replica.logger.Errorf("Unable to register ReadAPI: %v", err)
	}
	if err := trans.RegisterWriteAPI(&WriteAPI{replica}); err != nil {
		replica.logger.Errorf("Unable to register ReadAPI: %v", err)
	}
	if err := ol.RegisterOverloardAPI(&OverloardAPI{replica}); err != nil {
		replica.logger.Errorf("Unable to register ReadAPI: %v", err)
	}

	return replica, nil
}

func (self *Replica) Start() {
	go self.run()
}

func (self *Replica) Stop() {
	self.cncl()

	first, _ := self.logs.FirstIndex()
	last, _ := self.logs.LastIndex()
	self.logs.DeleteRange(first, last)
	self.logs.Close()
}

func (self *Replica) AddState(s State) uint8 {
	return self.states.Add(s)
}

func (self *Replica) AddCommand(ctx context.Context, state uint8, cmd []byte) error {

	ret := make(chan error)

	//push the command into the processing loop
	select {
	case <-ctx.Done():
		return fmt.Errorf("Command could not be processed")
	case self.cmdChan <- cmdStruct{cmd, state, ret, true, ctx}:
		break
	}

	//wait till the processing is successfull
	select {
	case <-ctx.Done():
		return fmt.Errorf("Command in process, but not commited yet")
	case err := <-ret:
		if err != nil {
			fmt.Errorf("Unable to commit command: %v", err)
		}
		return err
	}

	return nil
}

/******************************************************************************
					helper structs
******************************************************************************/
type requestState struct {
	ctx  context.Context
	cncl context.CancelFunc

	state      int8
	fetchedLog Log
}

type cmdStruct struct {
	cmd     []byte
	state   uint8
	retChan chan error
	local   bool
	ctx     context.Context
}

type leaderData struct {
	address Address
	key     crypto.RsaPublicKey
}

/******************************************************************************
					internal functions
******************************************************************************/

func (self *Replica) run() {

loop:
	for {
		select {

		case <-self.ctx.Done():
			break loop

		case log := <-self.commitChan:
			self.commitLog(log)

		case val := <-self.cmdChan:
			if val.local {
				val.retChan <- self.newCommand(val.ctx, val.cmd, val.state)

			} else {
				val.retChan <- self.requestCommand(val.cmd, val.state)
			}
		}
	}
	self.logger.Debug("Shutdown run loop")
}

//internal functions
func (self *Replica) commitLog(log Log) {

	self.logger.Debugf("Received log entry %v in epoch %v (which is of type %v)", log.Index, log.Epoch, log.Type)

	//check the required next index
	var requiredIdx uint64
	prev, err := self.logs.LastIndex()
	if err == nil {
		requiredIdx = prev + 1

	} else if IsNoEntryError(err) {
		requiredIdx = 0

	} else {
		self.logger.Errorf("Unable to commit entry: %v", err.Error())
	}

	//test if this commit is in the request loop and handle the request if so
	req, ok := self.requests[log.Index]
	if ok {
		//stop the request, we received it
		if req.cncl != nil {
			req.cncl()
		}

		//if the request is the next in line the log will be commited and the
		//request can be removed. Otherwise we set the request up for later processing
		if log.Index == requiredIdx {
			delete(self.requests, log.Index)

		} else {
			req.state = requestState_Available
			req.fetchedLog = log
			self.requests[log.Index] = req
		}
	}

	//check if we can commit, and do so or request what is missing bevore commit
	if requiredIdx == log.Index {

		//commit
		self.logs.StoreLog(log)
		self.applyLog(log)

		//see if we can finalize more requests (already received in the request log)
		maxIdx := (requiredIdx + uint64(len(self.requests)))
		for i := requiredIdx + 1; i <= maxIdx; i++ {

			req, ok := self.requests[i]
			if !ok || req.state != requestState_Available {
				break
			}

			//we have a finished request! commit
			delete(self.requests, i)
			self.logs.StoreLog(req.fetchedLog)
			self.applyLog(req.fetchedLog)
		}

	} else if requiredIdx < log.Index {

		//and add the received log to the request log, to ensure it is added
		//when possible
		rstate := requestState{nil, nil, requestState_Available, log}
		self.requests[log.Index] = rstate

		//we need to request all missing logs!
		for i := prev; i < log.Index; i++ {
			self.requestLog(i)
		}

	} else {
		self.logger.Warning("Received log commit, but is already applyed")
	}
}

//a request loop tries to fetch the missing log. It does add a new request state
//and starts fetching.
func (self *Replica) requestLog(idx uint64) {

	//if we are already requesting nothing needs to be done
	if _, has := self.requests[idx]; has == true {
		return
	}

	self.logger.Debugf("Request log %v started", idx)

	//build the request state
	ctx, cncl := context.WithCancel(self.ctx)
	rstate := requestState{ctx, cncl, requestState_Fetching, Log{}}
	self.requests[idx] = rstate

	//run the fetch. We ask one follower after the other to give us the missing
	//log, till it is received or the context is canceled
	go func(ctx context.Context, reqIdx uint64) {

	loop:
		for {
			select {

			case <-ctx.Done():
				break

			default:
				ctx, _ := context.WithTimeout(ctx, 500*time.Millisecond)
				reply := Log{}
				err := self.transport.CallAny(ctx, `ReadAPI`, `GetLog`, reqIdx, &reply)

				//handle the commit if we received it
				if err == nil {

					if reply.Index != reqIdx {
						self.logger.Errorf("Request for log %v returned log %v", reqIdx, reply.Index)
						continue loop
					}
					self.logger.Debugf("Request for log %v successfull", reply.Index)

					//we simply push it into the replic, the commit handler will
					//take care of cleaning the request state (we cant do it as
					//we are a asynchrous thread)
					self.commitChan <- reply
					break loop

				} else {
					self.logger.Debugf("Call of function GetLog in ReadAPI failed: %v", err.Error())
					time.Sleep(100 * time.Microsecond)
				}
			}
		}

	}(ctx, idx)
}

//applie the log to the state, or to internal config if required
func (self *Replica) applyLog(log Log) {

	switch log.Type {

	case logType_Leader:

	case logType_MemberAdd:

	case logType_MemberRemove:

	case logType_Snapshot:

	default:
		state := self.states.Get(log.Type)
		if state != nil {
			state.Apply(log.Data)

			//inform (but do not block if noone listends)
			select {
			case self.appliedChan <- log.Index:
				break
			default:
				break
			}

		} else {
			self.logger.Error("Cannot apply command: Unknown state")
		}
	}
}

//handles local commands: either process if we are leader or forward to leader.
//do not use for remote requests, as this can go into deadlook if some replica adresses the wrong leader
func (self *Replica) newCommand(ctx context.Context, cmd []byte, state uint8) error {

	self.logger.Debug("New local command added")

	if err := self.ensureLeader(ctx); err != nil {
		self.logger.Debugf("Anort new command: %v", err)
		return err
	}

	//check if we are leader. If so, it is easy to commit!
	leader := self.leaders.GetLeaderAddress()
	if self.address == leader {
		return self.requestCommand(cmd, state)
	}

	//we are not, so lets call the leader for the command request
	args := struct {
		state uint8
		cmd   []byte
	}{state, cmd}
	var ret uint64

	for {
		self.logger.Debugf("Request command from leader %v", leader)

		err := self.transport.Call(ctx, leader, `WriteAPI`, `RequestCommand`, args, &ret)

		//if it failed because of wrong leader data we need to check what is the correct one
		if IsNotLeaderError(err) {

			err := self.ensureLeader(ctx)
			if err != nil {
				self.logger.Debugf("Unable to get correct leader: %v", err)
			}

			if leader == self.leaders.GetLeaderAddress() {
				self.logger.Error("Replica says it is not leader, but overlord says so")
				return fmt.Errorf("Replica says it is not leader, but overlord says so")
			}

		} else if err != nil {
			self.logger.Error("Unable to call leader: %v", err)
			return err

		} else {
			return nil
		}
	}

	return nil
}

//ensures that there is a leader in the current leaderStore. If none is known it triggers an election.
//However, it does not reassure the known leader with the overlord, it assumes the internal state is up to date
func (self *Replica) ensureLeader(ctx context.Context) error {

	if self.leaders.EpochCount() == 0 {

		//maybe there is a new epoch we don't know about?
		epoch, addr, key, err := self.overlord.GetCurrentEpochData(ctx)

		if IsNoEpochError(err) {
			//there is no epoch yet, lets start one!
			err := self.overlord.RequestNewLeader(ctx, 0)
			if err != nil {
				return fmt.Errorf("Unable to reach overlord, abort: %v", err)
			}
			epoch, addr, key, err = self.overlord.GetCurrentEpochData(ctx)

		} else if err != nil {
			return fmt.Errorf("Unable to reach overlord for leader epoch inquery: %v", err)
		}

		self.leaders.AddEpoch(epoch, addr, key)
		self.leaders.SetEpoch(epoch)
	}
	return nil
}

//ensures that the known leader is the one provided by the overlord. It updates the internal state if not
func (self *Replica) reassureLeader(ctx context.Context) error {

	return nil
}

//handles remote command requests.
// - Not thread safe, only call from within "run"
// - Does not need a context as it is fast and does not call any context aware funcitons
func (self *Replica) requestCommand(cmd []byte, state uint8) error {

	self.logger.Debugf("Received command request")

	//we only process if we are the leader!
	if (self.leaders.EpochCount() == 0) || (self.address != self.leaders.GetLeaderAddress()) {
		self.logger.Error("Abort command request, I'm not leader")
		return &NotLeaderError{}
	}

	//fetch some infos
	var idx uint64
	last, err := self.logs.LastIndex()
	if err == nil {
		idx = last + 1

	} else if IsNoEntryError(err) {
		idx = 0

	} else {
		self.logger.Errorf("Can't access logstore during log creation: %v", err)
		return err
	}

	//build the log
	log := Log{
		Index: idx,
		Epoch: self.leaders.GetEpoch(),
		Type:  state,
		Data:  cmd,
	}
	log.Sign(self.privKey)

	//store and inform
	self.logs.StoreLog(log)
	self.applyLog(log)

	self.logger.Debugf("Send out new log")
	err = self.transport.Send(`ReadAPI`, `NewLog`, log)
	if err != nil {
		self.logger.Errorf("Unable to send out commited log: %v", err)
		return err
	}

	return nil
}
