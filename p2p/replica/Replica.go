package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
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

type Replica struct {
	transport Transport
	requests  map[uint64]requestState
	followers []Address
	name      string

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

func NewReplica(path string, name string, trans Transport) (*Replica, error) {

	//create new logstore for this state
	store, err := NewLogStore(path, name)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create replica")
	}

	//setup the main context
	ctx, cncl := context.WithCancel(context.Background())

	return &Replica{
		transport:    trans,
		requests:     make(map[uint64]requestState),
		followers:    make([]Address, 0),
		ctx:          ctx,
		cncl:         cncl,
		commitChan:   make(chan Log, 10),
		newStateChan: make(chan newStateStruct),
		newCmdChan:   make(chan newCmdStruct),
		states:       make([]State, 0),
		logs:         store,
		name:         name,
	}, nil
}

func (self *Replica) Start() {
	go self.run()
}

func (self *Replica) Stop() {
	self.cncl()
	self.logs.Close()
}

func (self *Replica) CreateAPIs() (ReadAPI, WriteAPI, ElectionAPI) {
	return ReadAPI{self}, WriteAPI{self}, ElectionAPI{self}
}

func (self *Replica) AddState(s State) uint8 {

	retChan := make(chan uint8)
	self.newStateChan <- newStateStruct{s, retChan}

	return <-retChan
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

/******************************************************************************
					internal functions
******************************************************************************/

func (self *Replica) run() error {

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
func (self *Replica) commitLog(log Log) error {

	prev, err := self.logs.LastIndex()
	if err != nil {
		return utils.StackError(err, "Unable to commit log")
	}

	//test if this commit is in the request loop and handle the request of so
	req, ok := self.requests[log.Index]
	if ok {
		//stop the request, we received it
		req.cncl()

		//if the request is the next in line the log will be commited and the
		//request can be romved. Otherwise we set the request up for later processing
		if log.Index == prev+1 {
			delete(self.requests, log.Index)

		} else {
			req.state = requestState_Available
			req.fetchedLog = log
		}
	}

	//check if we can commit, and do so or request what is missing bevore commit
	if prev == log.Index-1 {

		//commit
		self.logs.StoreLog(log)
		self.applyLog(log)

		//see if we can finalize more requests

	} else if prev < log.Index {

		//we need to request all missing logs!
		for i := prev; i <= log.Index; i++ {
			self.requestLog(i)
		}
		//and add the received one to the request log too, to ensure it is added
		//when possible
		rstate := requestState{nil, nil, requestState_Available, log}
		self.requests[log.Index] = rstate

	} else {

		return fmt.Errorf("Log already commited")
	}

	return nil
}

//a request loop tries to fetch the missing log. It does add a new request state
//and starts fetching.
func (self *Replica) requestLog(idx uint64) {

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

		for {
			select {

			case <-ctx.Done():
				break

			default:
				ctx, _ := context.WithTimeout(ctx, 1*time.Second)
				reply := Log{}
				addr := self.followers[rand.Intn(len(self.followers))]
				err := self.transport.Call(ctx, addr, `GetLog`, idx, &reply)

				//handle the commit if we received it
				if err != nil {
					//we simply push it into the replic, the commit handler will
					//take care of cleaning the request state (we cant do it as
					//we are a asynchrous thread)
					self.commitChan <- reply
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
		self.states[log.Type].Apply(log.Data)
	}
}

func (self *Replica) newLog(data []byte) {

}
