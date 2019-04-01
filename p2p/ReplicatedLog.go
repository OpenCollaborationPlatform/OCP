package p2p

import (
	"CollaborationNode/p2p/replica"
	"CollaborationNode/utils"
	"context"
	"fmt"
	"log"
)

/* A log that can be replicated to multiple instances and that supports compaction*/
type replicatedLog struct {
	commited replica.LogStore
	swarm    *Swarm
	name     string

	//controling the execution
	shutdown bool
	baseCtx  context.Context
	baseCncl context.CancelFunc

	//channels to sync all actions on the log
	chanReceivedVote chan *Event
	chanMakeVote     chan []byte
	chanShutdown     chan bool

	//handling the process
	logstore       replica.LogStore
	voteManager    *replica.VoteManager
	desicionEngine *replica.DesicionEngine
}

func newSwarmReplicatedLog(name string, swarm *Swarm) (*replicatedLog, error) {

	//conntext for shutdown
	ctx, cncl := context.WithCancel(context.Background())

	//build the store
	store, err := replica.NewLogStore(swarm.GetPath(), name)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create log store")
	}

	//and the managers
	voteManager := replica.NewVoteManager(store)

	//build the log
	log := &replicatedLog{
		commited:         store,
		swarm:            swarm,
		name:             name,
		shutdown:         false,
		baseCtx:          ctx,
		baseCncl:         cncl,
		chanReceivedVote: make(chan *Event, 100),
		chanMakeVote:     make(chan []byte),
		chanShutdown:     make(chan bool),
		logstore:         store,
		voteManager:      voteManager,
		desicionEngine:   replica.NewDesicionEngine(store, voteManager),
	}

	//start the main thread
	go log.runLoop()

	//setup the event listening
	topic := "replica." + name + ".vote"
	sub, err := swarm.Event.Subscribe(topic, AUTH_READWRITE)
	if err != nil {
		return nil, utils.StackError(err, "Unable to subscribe to log events")
	}
	go func(ctx context.Context, channel chan *Event) {
		newctx, _ := context.WithCancel(ctx)
		for {
			event, err := sub.Next(newctx)
			if err != nil {
				break
			}
			channel <- event
		}
	}(log.baseCtx, log.chanReceivedVote)

	//done!
	return log, nil
}

/* 					API: can be called from any thread
*******************************************************************************/

func (self *replicatedLog) Stop() {

	//make sure it is never blocking, even if called from multiple threads
	//(the shutdown only needs to be done once to stop everything)
	select {
	case self.chanShutdown <- true:

	default:
	}
}

func (self *replicatedLog) AppendEntry(entry []byte) {

	//TODO: Make blocking until commited
	self.chanMakeVote <- entry
}

//loop that handles all channels. Channels are used to syncronize access to the
//log state, hence everything flows through them. This functionis basicaly the
//main thread for the log
func (self *replicatedLog) runLoop() {

	//loop until shutdown
	for !self.shutdown {

		select {

		case event := <-self.chanReceivedVote:
			fmt.Printf("\n%v received Vote\n", self.swarm.host.ID().pid().String())
			receivedVote := replica.VoteFromData(string(event.Source), event.Data)
			self.handleVote(receivedVote)

		case data := <-self.chanMakeVote:
			fmt.Printf("\n%v create new Vote\n", self.swarm.host.ID().pid().String())
			vote := self.voteManager.GenerateVote(string(self.swarm.host.ID()), data)
			self.swarm.Event.Publish("replica."+self.name+".vote", vote.ToByte())

		case <-self.chanShutdown:
			self.shutdown()
		}
	}
}

/* 			internal functions: only to be called from runLoop()
*******************************************************************************/

func (self *replicatedLog) shutdown() {
	self.baseCncl()
	self.shutdown = true
}

func (self *replicatedLog) handleVote(vote replica.Vote) {

	//check first if we already voted for this instance
	isNew, err := self.voteManager.AppendVote(vote)
	if err != nil {
		log.Printf("Invalid vote received: %v", err)
		return
	}
	if isNew {
		//we also vote for this
		ownVote := replica.NewVote(string(self.swarm.host.ID()), vote.Index(), vote.Data())
		self.swarm.Event.Publish("replica."+self.name+".vote", ownVote.ToByte())
	}

	//with a new vote we need to recheck if it can be commited
	commited, clog := self.desicionEngine.DecideOnFirst()
	if commited {
		err := self.logstore.StoreLog(clog)
		if err != nil {
			log.Printf("Error commiting log: %v", err)
		}
	}
}
