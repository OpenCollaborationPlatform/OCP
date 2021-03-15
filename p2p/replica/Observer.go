package replica

import (
	"github.com/hashicorp/raft"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type Event_Type int

const (
	EVENT_PEER_ADDED         Event_Type = iota // new peer added to config (for leader only)
	EVENT_PEER_REMOVED                         // peer removed from config (for leader only)
	EVENT_LEADER_CHANGED                       // leader changed
	EVENT_MAJORITY_AVAILABLE                   // Majority of nodes online and reachable
	EVENT_MAJORITY_LOST                        // Cannot reach majority of notes
)

type Event struct {
	Peer  peer.ID
	Event Event_Type
}

type Observer struct {
	raftObs *raft.Observer
	obsCh   chan raft.Observation
	evtCh   chan Event
}

func (self *Replica) NewObserver(blocking bool) Observer {

	obsC := make(chan raft.Observation, 10)
	evtC := make(chan Event, 10)
	robs := raft.NewObserver(obsC, blocking, nil)

	obs := Observer{robs, obsC, evtC}
	go obs.observationLoop()

	//register after obs loop to get all events!
	self.rep.RegisterObserver(robs)

	return obs
}

func (self *Replica) CloseObserver(obs Observer) {

	self.rep.DeregisterObserver(obs.raftObs)
	close(obs.obsCh)
	//evtCh closes when obsCh is empty and observation loops exits
}

func (self Observer) EventChannel() chan Event {
	return self.evtCh
}

func (self Observer) observationLoop() {

	//stops when obsCh is closed
	for obs := range self.obsCh {

		switch obs := obs.Data.(type) {

		case raft.LeaderObservation:
			pid, err := peer.IDB58Decode(string(obs.Leader))
			if err != nil {
				continue
			}

			self.evtCh <- Event{pid, EVENT_LEADER_CHANGED}

		case raft.PeerObservation:
			pid, err := peer.IDB58Decode(string(obs.Peer.Address))
			if err != nil {
				continue
			}
			evtType := EVENT_PEER_ADDED
			if obs.Removed {
				evtType = EVENT_PEER_REMOVED
			}
			self.evtCh <- Event{pid, evtType}

		case raft.RaftState:
			if obs == raft.Leader || obs == raft.Follower {
				self.evtCh <- Event{peer.ID(""), EVENT_MAJORITY_AVAILABLE}

			} else {
				//candidate
				self.evtCh <- Event{peer.ID(""), EVENT_MAJORITY_LOST}
			}
		}
	}
	close(self.evtCh)
}
