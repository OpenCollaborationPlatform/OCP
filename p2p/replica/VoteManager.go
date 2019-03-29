package replica

import (
	"encoding/json"
	"math"
)

type voteData struct {
	Index uint64
	Data  []byte
}

type Vote struct {
	owner string
	vdata voteData
}

func (self *Vote) Index() uint64 { return self.vdata.Index }
func (self *Vote) Owner() string { return self.owner }
func (self *Vote) Data() []byte  { return self.vdata.Data }

func NewVote(id string, idx uint64, data []byte) Vote {
	vdata := voteData{idx, data}
	return Vote{id, vdata}
}

//build a Vote from an event. Note that the owner is takes from the event, and not
//part of the Vote data. This is done to uitilize the signed event system to ensure
//owner is set correctl always
func VoteFromData(owner string, data []byte) Vote {
	vdata := voteData{}
	json.Unmarshal(data, &vdata)
	return Vote{owner, vdata}
}

func (self *Vote) ToByte() []byte {
	b, _ := json.Marshal(self.vdata)
	return b
}

type VoteManager struct {

	//each intex can have multiple Votes
	Votes map[uint64][]Vote
}

func NewVoteManager() VoteManager {

	return VoteManager{
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
		voteData{idx, data},
	}

	self.Votes[idx] = []Vote{v}

	return v
}

//appends Vote, returns true if it is the first Vote for a index, false otherwise
func (self *VoteManager) AppendVote(v Vote) (isFirst bool) {

	//check if we have something already
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
				list[idx].vdata = v.vdata
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
