// Swarm.go
package p2p

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-net"
)

const swarmURI = "/swarm/1.0.0/"

func newSwarmProtocol(host *Host) *swarmProtocol {

	sp := &swarmProtocol{host}
	host.host.SetStreamHandler(swarmURI, sp.RequestHandler)
	return sp
}

//Protocol that handles swarm connection requests. It
type swarmProtocol struct {
	host *Host
}

func (sp *swarmProtocol) RequestHandler(s net.Stream) {

	log.Println("Swarm stream requested")

	//	pid := PeerID{s.Conn().RemotePeer()}
	messenger := newStreamMessenger(s)
	msg, err := messenger.ReadMsg()
	if err != nil {
		log.Printf("Error reading stream in protocol /swarm/1.0.0/:  %s", err)
	}

	if msg.MessageType() != PARTICIPATE {
		messenger.WriteMsg(Error{"Anticipated PARTICIPATE message"})
		messenger.Close()
		return
	}

	//check if stream exist
	id := msg.(*Participate).Swarm

	swarm, err := sp.host.GetSwarm(id)
	if err != nil {
		messenger.WriteMsg(Error{"Swarm does not exist"})
		messenger.Close()
		return
	}

	//the swarm is from here on responsible
	pid := PeerID{s.Conn().RemotePeer()}
	swarm.participate(pid, *msg.(*Participate), &messenger)
}

//Holds all needed data and streams for a full blown connection to a swarm peer
type peerConnection struct {
	WriteAccess bool
	Event       *streamMessenger
}

func (pc *peerConnection) Close() {
	pc.Event.Close()
	pc.Event = nil
}

func (pc *peerConnection) IsConnected() bool {

	return pc.Event != nil
}

//Type that represents a collection if peers which are connected together and form
//a swarm. It allows to share data between all peers as well as have common events.
//The following properties hold:
// - Each peer is connected to all other peers in the swarm
// -- On Adding, a write stream is opened to the peer
// -- As the other peers get us added, they open a stream to us too
// -- All accepted streams are used as read streams
// - Data cannot be send to a peer, only be requested
// - Data is split and transfer is split to all peers if possible
// - Event handling mimics the wamp interface
type Swarm struct {
	//general stuff
	peerLock sync.RWMutex
	host     *Host
	ID       SwarmID
	privKey  crypto.PrivKey
	pubKey   crypto.PubKey
	peers    map[PeerID]peerConnection
	public   bool

	//events
	eventLock      sync.RWMutex
	events         chan Message //internal collection of all events
	eventCallbacks map[string][]func(Dict)
	eventChannels  map[string][]chan Dict
}

type SwarmID string

func (id SwarmID) Pretty() string {
	return string(id)
}

//not accassible outside the package: should be used via Host only
func newSwarm(host *Host, id SwarmID, public bool, privKey crypto.PrivKey, pubKey crypto.PubKey) *Swarm {

	swarm := &Swarm{
		host:           host,
		peerLock:       sync.RWMutex{},
		ID:             id,
		privKey:        privKey,
		pubKey:         pubKey,
		public:         public,
		peers:          make(map[PeerID]peerConnection, 0),
		events:         make(chan Message, 10),
		eventCallbacks: make(map[string][]func(Dict)),
		eventChannels:  make(map[string][]chan Dict)}

	//start processing
	swarm.startEventHandling()

	return swarm
}

/* Peer handling
 *************  */

func (s *Swarm) AddPeer(pid PeerID, readOnly bool) error {

	s.peerLock.Lock()
	//check if peer exist already
	_, ok := s.peers[pid]
	if ok {
		s.peerLock.Unlock()
		return nil
	}
	//and now add it
	s.peers[pid] = peerConnection{WriteAccess: !readOnly}
	s.peerLock.Unlock()

	//try to connect. We want to make sure the peer is always connected if it is part
	//of this swarm, even after disconnectes etc. Hence we see if it is still connected
	//and reconnect from time to time
	go func() {
		for {
			s.peerLock.RLock()
			pc, ok := s.peers[pid]
			s.peerLock.RUnlock()
			if !ok {
				//if not part of the swarm anymore we stop the connection attempts
				break
			}

			if !pc.IsConnected() {
				s.connectPeer(pid)
			}
			//next attemp: in a second
			time.Sleep(time.Second)
		}
	}()

	return nil
}

func (s *Swarm) connectPeer(pid PeerID) error {

	//lock over whole time to make sure the connection process is finished before
	//any other stream does harm
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	pc, ok := s.peers[pid]

	if !ok {
		return fmt.Errorf("Peer is not allowed to be connected: %s", pid.Pretty())
	}
	if pc.IsConnected() {
		return nil
	}

	//we start with the signals needed in both cases, ReadWrite and ReadOnly
	//that are: Event publish, Data send and RPC callable

	//start of with publishing Events
	stream, err := s.host.host.NewStream(context.Background(), pid.ID, swarmURI)
	if err != nil {
		return err
	}
	evtMessenger := newStreamMessenger(stream)
	msg := Participate{Swarm: s.ID, Role: "EventPublish"}
	err = evtMessenger.WriteMsg(msg)
	if err != nil {
		evtMessenger.Close()
		return err
	}

	ret, err := evtMessenger.ReadMsg()
	if err != nil {
		evtMessenger.Close()
		return err
	}

	if ret.MessageType() != SUCCESS {
		evtMessenger.Close()
		return fmt.Errorf("Event connection failed")
	}

	//see if we also need the event listening
	if pc.WriteAccess {
		stream, err := s.host.host.NewStream(context.Background(), pid.ID, swarmURI)
		if err != nil {
			return err
		}
		messanger := newStreamMessenger(stream)
		msg := Participate{Swarm: s.ID, Role: "EventListen"}
		err = messanger.WriteMsg(msg)
		if err != nil {
			messanger.Close()
			return err
		}

		ret, err := messanger.ReadMsg()
		if err != nil {
			messanger.Close()
			return err
		}

		if ret.MessageType() != SUCCESS {
			messanger.Close()
			return fmt.Errorf("Event connection failed")
		}

		//successfull! we forward all all events we receive to the swarm event
		messanger.reader.ForwardMsg(s.events)
	}

	//everything was successfull
	pc.Event = &evtMessenger
	s.peers[pid] = pc

	return nil
}

func (s *Swarm) participate(pid PeerID, msg Participate, messenger *streamMessenger) {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	//lets see if it is allowed to participate
	pc, ok := s.peers[pid]
	if !ok {
		messenger.WriteMsg(Error{"Peer not allowed to join swarm"})
		messenger.Close()
		return
	}

	//check event type and handle accordingly
	switch msg.Role {

	case "EventListen":

		//the peer wants to listen for events, that is always allowed if he is part
		//of the stream (as long as we are not yet doing it...)
		if pc.Event != nil {
			messenger.WriteMsg(Error{"Peer does already listen for events"})
			messenger.Close()
			return
		}
		err := messenger.WriteMsg(Success{})
		if err != nil {
			messenger.Close()
			return
		}
		pc.Event = messenger

	case "EventPublish":

		//this is a event publish stream, hence we shall listen for events. This
		//we only do if the peer has write access to this stream
		if !pc.WriteAccess {
			messenger.WriteMsg(Error{"Peer does not have write access"})
			messenger.Close()
			return
		}
		//let's listen for those nice events!
		err := messenger.WriteMsg(Success{})
		if err != nil {
			messenger.Close()
			return
		}
		messenger.reader.ForwardMsg(s.events)
	}

	//store the updates
	s.peers[pid] = pc

}

func (s *Swarm) RemovePeer(peer PeerID) {

}

func (s *Swarm) HasPeer(peer PeerID) bool {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	_, ok := s.peers[peer]
	return ok
}

/* Event handling
 *************  */

func (s *Swarm) PostEvent(uri string, kwargs Dict) {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	//lets call all our peers!
	for _, data := range s.peers {
		if data.IsConnected() {
			go data.Event.WriteMsg(Event{uri, kwargs, List{}})
		}
	}
}

func (s *Swarm) RegisterEventChannel(uri string, channel chan Dict) {

	s.eventLock.Lock()
	defer s.eventLock.Unlock()

	slice, ok := s.eventChannels[uri]
	if !ok {
		s.eventChannels[uri] = make([]chan Dict, 0)
		slice, _ = s.eventChannels[uri]
	}

	s.eventChannels[uri] = append(slice, channel)
}

func (s *Swarm) RegisterEventCallback(uri string, function func(Dict)) {

	s.eventLock.Lock()
	defer s.eventLock.Unlock()

	slice, ok := s.eventCallbacks[uri]
	if !ok {
		s.eventCallbacks[uri] = make([]func(Dict), 0)
		slice, _ = s.eventCallbacks[uri]
	}

	s.eventCallbacks[uri] = append(slice, function)
}

//converts the message channel to calbacks
func (s *Swarm) startEventHandling() {

	go func() {
		for {
			msg, more := <-s.events
			if !more {
				return
			}

			//we only handle events
			if msg.MessageType() != EVENT {
				log.Printf("Received msg that is not event: %s", msg.MessageType().String())
				continue
			}
			event := msg.(*Event)
			log.Printf("Received event for URI %s", event.Uri)

			//TODO: check if we have seen this event already
			//TODO: forward event to all our peers

			//now distribute the event to all registered callbacks and channels
			callbacks, ok := s.eventCallbacks[event.Uri]
			if ok {
				for _, call := range callbacks {
					go call(event.KwArgs)
				}
			}
			channels, ok := s.eventChannels[event.Uri]
			if ok {
				for _, channel := range channels {
					go func() { channel <- event.KwArgs }()
				}
			}
		}
	}()
}

/* Data handling
 *************  */

func (s *Swarm) DistributeData(data []byte) string {

	return ""
}

func (s *Swarm) DistributeFile(path string) string {
	return ""
}

func (s *Swarm) DropDataOrFile(path string) string {
	return ""
}

/* General functions
 ******************  */

func (s *Swarm) IsPublic() bool {
	return s.public
}

/* Internal functions
 ******************  */

func (s *Swarm) signatureMessage(conn net.Conn) []byte {
	hashMsg := conn.RemotePeer().Pretty() + "_" +
		conn.LocalPeer().Pretty() + "_" + string(s.ID)
	fmt.Println(hashMsg)
	return []byte(hashMsg)
}
