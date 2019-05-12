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

type requestState struct {
	ctx  context.Context
	cncl context.CancelFunc

	state      int8
	fetchedLog Log
}

type Replica struct {
	transport Transport
	requests  map[uint64]requestState
	followers []Follower

	//syncronizing via channels
	ctx        context.Context
	cncl       context.CancelFunc
	commitChan chan Log

	//state handling
	states []State
	logs   LogStore
}

func NewReplica(path string, name string, state State) (*Replica, error) {

	//create new logstore for this state
	/*store, err := NewLogStore(path, name)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create replica")
	}

	return &Replica{nil, state, store}, nil*/
	return nil, nil
}

func (self *Replica) Start() {
	go self.run()
}

func (self *Replica) Stop() {
	self.cncl()
}

func (self *Replica) run() error {

	for {
		select {

		case <-self.ctx.Done():
			break

		case log := <-self.commitChan:
			self.commitLog(log)
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
				err := self.transport.FollowerCall(ctx, addr, `GetLog`, idx, &reply)

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
