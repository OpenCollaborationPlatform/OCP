package replica

import (
	"CollaborationNode/utils"
	"context"
	"math"
	"math/rand"
	"time"

	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-crypto"
)

var logger = logging.Logger("replica")

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

type Replica struct {
	transport Transport
	requests  map[uint64]requestState
	leaders   map[uint64]leaderData
	name      string
	key       crypto.RsaPrivateKey

	//syncronizing via channels
	ctx          context.Context
	cncl         context.CancelFunc
	commitChan   chan Log
	newStateChan chan newStateStruct
	newCmdChan   chan newCmdStruct

	//state handling
	states []State
	logs   LogStore
}

func NewReplica(path string, name string, trans Transport, key crypto.RsaPrivateKey) (*Replica, error) {

	//create new logstore for this state
	store, err := NewLogStore(path, name)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create replica")
	}

	//setup the main context
	ctx, cncl := context.WithCancel(context.Background())

	replica := &Replica{
		transport:    trans,
		requests:     make(map[uint64]requestState),
		leaders:      make(map[uint64]leaderData, 0),
		ctx:          ctx,
		cncl:         cncl,
		commitChan:   make(chan Log, 10),
		newStateChan: make(chan newStateStruct),
		newCmdChan:   make(chan newCmdStruct),
		states:       make([]State, 0),
		logs:         store,
		name:         name,
		key:          key,
	}

	//setup transport correctly
	trans.RegisterReadAPI(&ReadAPI{replica})
	trans.RegisterWriteAPI(&WriteAPI{replica})

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

	retChan := make(chan uint8)
	self.newStateChan <- newStateStruct{s, retChan}

	return <-retChan
}

func (self *Replica) AddCommand(state uint8, cmd []byte) {

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

type newStateStruct struct {
	state   State
	retChan chan uint8
}

type newCmdStruct struct {
	cmd     []byte
	retChan chan struct{}
}

type leaderData struct {
	address Address
	key     crypto.RsaPublicKey
}

/******************************************************************************
					internal functions
******************************************************************************/

func (self *Replica) run() {

	for {
		select {

		case <-self.ctx.Done():
			break

		case log := <-self.commitChan:
			self.commitLog(log)

		case val := <-self.newStateChan:
			self.states = append(self.states, val.state)
			val.retChan <- uint8(len(self.states) - 1)
			close(val.retChan)

		case val := <-self.newCmdChan:
			self.newLog(val.cmd)
		}
	}
}

//internal functions
func (self *Replica) commitLog(log Log) {

	logger.Debugf("Received log entry %v in epoch %v (which is of type %v)", log.Index, log.Epoch, log.Type)

	//check the required next index
	var requiredIdx uint64
	prev, err := self.logs.LastIndex()
	if err == nil {
		requiredIdx = prev + 1

	} else if IsNoEntryError(err) {
		requiredIdx = 0

	} else {
		logger.Error("Unable to commit entry: %v", err.Error())
	}

	//test if this commit is in the request loop and handle the request if so
	req, ok := self.requests[log.Index]
	if ok {
		//stop the request, we received it
		req.cncl()

		//if the request is the next in line the log will be commited and the
		//request can be removed. Otherwise we set the request up for later processing
		if log.Index == requiredIdx {
			delete(self.requests, log.Index)

		} else {
			req.state = requestState_Available
			req.fetchedLog = log
		}
	}

	//check if we can commit, and do so or request what is missing bevore commit
	if requiredIdx == log.Index {

		//commit
		logger.Debugf("Commit log %v", log.Index)
		self.logs.StoreLog(log)
		self.applyLog(log)

		//see if we can finalize more requests (already received in the request log)
		for i := requiredIdx + 1; i <= (requiredIdx + uint64(len(self.requests))); i++ {

			req, ok := self.requests[i]
			if !ok || req.state != requestState_Available {
				break
			}

			//we have a finished request! commit
			logger.Debugf("Commit log %v from request store", log.Index)
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
		logger.Warning("Received log commit, but is already applyed")
	}
}

//a request loop tries to fetch the missing log. It does add a new request state
//and starts fetching.
func (self *Replica) requestLog(idx uint64) {

	logger.Debugf("Request log %v", idx)

	//if we are already requesting nothing needs to be done
	if _, has := self.requests[idx]; has == true {
		return
	}

	//build the request state
	ctx, cncl := context.WithCancel(context.Background())
	rstate := requestState{ctx, cncl, requestState_Fetching, Log{}}
	self.requests[idx] = rstate

	//run the fetch. We ask one follower after the other to give us the missing
	//log, till it is received or the context is canceled
	go func(ctx context.Context) {

	loop:
		for {
			select {

			case <-ctx.Done():
				break

			default:
				ctx, _ := context.WithTimeout(ctx, 1*time.Second)
				reply := Log{}
				err := self.transport.CallAny(ctx, `ReadAPI`, `GetLog`, idx, &reply)

				//handle the commit if we received it
				if err == nil {

					logger.Debugf("Request for log %v successfull", reply.Index)

					//we simply push it into the replic, the commit handler will
					//take care of cleaning the request state (we cant do it as
					//we are a asynchrous thread)
					self.commitChan <- reply
					break loop

				} else {
					logger.Debugf("Call of function GetLog in ReadAPI failed: %v", err.Error())
				}
			}
		}

	}(ctx)
}

//applie the log to the state, or to internal config if required
func (self *Replica) applyLog(log Log) {

	switch log.Type {

	case logType_Leader:

	case logType_MemberAdd:

	case logType_MemberRemove:

	case logType_Snapshot:

	default:
		if uint8(len(self.states)) <= log.Type {
			return
		}

		state := self.states[log.Type]
		if state != nil {
			state.Apply(log.Data)

		} else {
			logger.Error("Cannot apply command: Unknown state")
		}
	}
}

func (self *Replica) newLog(data []byte) {

}
