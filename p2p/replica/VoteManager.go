package replica

import (
	"CollaborationNode/utils"
	"encoding/json"
	"fmt"
	"math"
)

type Vote struct {
	owner string
	voted Log
}

func (self *Vote) Index() uint64 { return self.voted.Index }
func (self *Vote) Owner() string { return self.owner }
func (self *Vote) Data() []byte  { return self.voted.Data }

func NewVote(id string, idx uint64, data []byte) Vote {
	voted := Log{idx, data}
	return Vote{id, voted}
}

//build a Vote from an event. Note that the owner is takes from the event, and not
//part of the Vote data. This is done to uitilize the signed event system to ensure
//owner is set correctl always
func VoteFromData(owner string, data []byte) Vote {
	voted := Log{}
	json.Unmarshal(data, &voted)
	return Vote{owner, voted}
}

func (self *Vote) ToByte() []byte {
	b, _ := json.Marshal(self.voted)
	return b
}

type VoteManager struct {
	store LogStore

	//each intex can have multiple Votes
	Votes map[uint64][]Vote
}

func NewVoteManager(store LogStore) *VoteManager {

	return &VoteManager{
		store,
		make(map[uint64][]Vote),
	}
}

func (self *VoteManager) FirstIdx() uint64 {

	if len(self.Votes) == 0 {
		return 0
	}

	//iterate and search the smalles number
	var first uint64 = math.MaxUint64
	for idx, _ := range self.Votes {
		if idx < first {
			first = idx
		}
	}

	return first
}

func (self *VoteManager) LastIdx() uint64 {

	//iterate and search the smallest number
	var first uint64 = 0
	for idx, _ := range self.Votes {
		if idx > first {
			first = idx
		}
	}

	return first
}

func (self *VoteManager) GenerateVote(owner string, data []byte) Vote {

	idx := self.LastIdx() + 1
	v := Vote{
		owner,
		Log{idx, data},
	}

	self.Votes[idx] = []Vote{v}

	return v
}

//appends Vote, returns true if it is the first Vote for a index, false otherwise
func (self *VoteManager) AppendVote(v Vote) (isFirst bool, err error) {

	//can this still be voted on?
	idx, e := self.store.LastIndex()
	if e != nil {
		return false, utils.StackError(err, "Unable to access logstore")
	}
	if idx >= v.Index() {
		return false, fmt.Errorf("Votes for already commited log")
	}
	err = nil

	//check if we have already a vote for it
	list, ok := self.Votes[v.Index()]
	if !ok {
		isFirst = true
		self.Votes[v.Index()] = []Vote{v}

	} else {
		isFirst = false

		//see if this Vote already exists. If so we need to override
		override := false
		for idx, lVote := range list {

			if lVote.Owner() == v.Owner() {
				//index and owner are the same, hence we override the Vote!
				list[idx].voted = v.voted
				override = true
				break
			}
		}
		//if overridden just replace the list
		if override {
			self.Votes[v.Index()] = list

			//otherwise append the new Vote
		} else {
			self.Votes[v.Index()] = append(self.Votes[v.Index()], v)
		}
	}

	return
}

func (self *VoteManager) GetVotes(idx uint64) []Vote {

	list, ok := self.Votes[idx]
	if !ok {
		return []Vote{}
	}
	return list
}
