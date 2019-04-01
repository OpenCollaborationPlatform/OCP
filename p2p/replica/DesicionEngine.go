package replica

import (
	"bytes"
	"fmt"
)

type DesicionEngine struct {
	commited chan Log
	requried chan RequestRange
	logs     LogStore
	votes    *VoteManager
	voter    []string
}

type RequestRange struct {
	First uint64
	Last  uint64
}

func NewDesicionEngine(store LogStore, votes *VoteManager) *DesicionEngine {

	return &DesicionEngine{
		commited: make(chan Log),
		requried: make(chan RequestRange),
		logs:     store,
		votes:    votes,
	}
}

//decides if the first idx which was voted on can be commited. Returns true if so,
//false otherwise, as well as the data which was decided on
func (self *DesicionEngine) DecideOnFirst() (bool, Log) {

	//first check if we need some additional log entries
	idx := self.votes.FirstIdx()
	logidx, _ := self.logs.LastIndex()
	if idx != logidx+1 {
		//make the range of required
		fmt.Printf("Error: Not full range available")
	}

	//we go through the next votes and see if we can commit it!
	counts := self.countVotesFor(idx)

	//see if we have a quorum
	var commit Log
	commited := false
	for voteIdx, cnt := range counts {

		if cnt >= self.getQuorum() {
			votes := self.votes.GetVotes(idx)
			commited = true
			commit = votes[voteIdx].voted
			break
		}
	}

	return commited, commit
}

func (self *DesicionEngine) countVotesFor(idx uint64) map[int]int {

	//we go through the next votes and see if we can commit it!
	votes := self.votes.GetVotes(idx)
	sorted_votes := make(map[int]int)
	for i, v := range votes {

		//iterate over all keys and add +1 to the key that is equal
		used := false
		for key, _ := range sorted_votes {

			if bytes.Equal(votes[key].Data(), v.Data()) {
				sorted_votes[key] = sorted_votes[key] + 1
				used = true
			}
		}

		//if not counted to any existing entry we create a new one
		if !used {
			sorted_votes[i] = 1
		}
	}
	return sorted_votes
}

func (self *DesicionEngine) getQuorum() int {
	return len(self.voter)/2 + 1
}
