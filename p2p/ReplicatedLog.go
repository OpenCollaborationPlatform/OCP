package p2p

import (
	"CollaborationNode/p2p/replica"
	"CollaborationNode/utils"
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

/* A log that can be replicated to multiple instances and that supports compaction*/
type replicatedLog struct {
	commited *bolt.DB
	swarm    *Swarm
	name     string

	//controling the execution
	shutdown bool
	baseCtx  context.Context
	baseCncl context.CancelFunc

	//channels to sync all actions on the log
	chanReceivedVote chan Event
	chanMakeVote     chan []byte
	chanShutdown     chan bool

	//handling the process
	propManager replica.VoteManager
}

func newSwarmReplicatedLog(name string, swarm *Swarm) (*replicatedLog, error) {

	//open the database
	err := os.MkdirAll(swarm.GetPath(), os.ModePerm)
	if err != nil {
		return nil, utils.StackError(err, "Cannot open path %s for log store", swarm.GetPath())
	}
	dbpath := filepath.Join(swarm.GetPath(), name, "-log.db")
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, utils.StackError(err, "Unable to open bolt db: %s", dbpath)
	}

	//make sure the basic structure exists
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("logs"))
		tx.CreateBucketIfNotExists([]byte("conf"))
		return nil
	})

	//conntext for shutdown
	ctx, cncl := context.WithCancel(context.Background())

	//build the log
	log := &replicatedLog{
		commited:         db,
		swarm:            swarm,
		name:             name,
		shutdown:         false,
		baseCtx:          ctx,
		baseCncl:         cncl,
		chanReceivedVote: make(chan Event, 100),
		chanMakeVote:     make(chan []byte),
		chanShutdown:     make(chan bool),
		propManager:      replica.NewVoteManager(),
	}

	//start the main thread
	go log.runLoop()

	//done!
	return log, nil
}

/* 		API: can be called from any thread 	*/

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
			receivedVote := replica.VoteFromData(string(event.Source), event.Data)
			isNew := self.propManager.AppendVote(receivedVote)
			if isNew {
				//we also vote for this
				ownVote := replica.NewVote(string(self.swarm.host.ID()), receivedVote.Index(), receivedVote.Data())
				self.swarm.Event.Publish("replica."+self.name+".vote", ownVote.ToByte())
			}

		case data := <-self.chanMakeVote:
			vote := self.propManager.GenerateVote(string(self.swarm.host.ID()), data)
			self.swarm.Event.Publish("replica."+self.name+".vote", vote.ToByte())

		case <-self.chanShutdown:
			self.baseCncl()
			self.shutdown = true

		}
	}
}

/* 		internal functions: only to be called from runLoop() 	*/
