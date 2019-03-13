// Swarm: main functions
package p2p

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	crypto "github.com/libp2p/go-libp2p-crypto"
	net "github.com/libp2p/go-libp2p-net"
	"github.com/spf13/viper"
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

	messenger := newStreamMessenger(s, 2<<(10*2)) //2mb per message max size
	msg, err := messenger.ReadMsg(false)
	if err != nil {
		log.Printf("Error reading stream in protocol /swarm/1.0.0/:  %s", err)
	}

	if msg.MessageType() != PARTICIPATE {
		messenger.WriteMsg(Error{"Anticipated PARTICIPATE message"}, false)
		messenger.Close()
		return
	}

	//check if swarm exist
	id := msg.(*Participate).Swarm
	swarm, err := sp.host.GetSwarm(id)
	if err != nil {
		messenger.WriteMsg(Error{"Swarm does not exist"}, false)
		messenger.Close()
		return
	}

	//the swarm is responsible from here on
	pid := PeerID{s.Conn().RemotePeer()}
	swarm.participate(pid, *msg.(*Participate), messenger)
}

//Holds all needed data and streams for a full blown connection to a swarm peer
type peerConnection struct {
	WriteAccess bool
	Event       participationMessenger
	Data        participationMessenger
}

func (pc *peerConnection) Close() {
	pc.Event.Close()
	pc.Data.Close()
}

func (pc *peerConnection) Connected() bool {
	return pc.Event.Connected() && pc.Data.Connected()
}

//Type that represents a collection if peers which are connected together and form
//a swarm. It allows to share data between all peers, as well as have common events
//and provide rpc calls
//The following properties hold:
// - Each peer is connected to all other peers in the swarm (for now at least)
// -- No forwarding of events
// -- No douplication of event
// - If a peer is allowed to send and receive data depends on the swarms peer list
// -- Authorisation must happen outside of the swarm: AddPeer has holy information
// -- Messages need to be signed with swarm key, but swarmkey is not exclusive right
//	  guarantee (a peer could have been removed from allowed peers afterwards)
// - Data cannot be send to a peer, only be requested by it
// - Data is split and transfer is split to all peers if possible
// - Event and rpc handling mimics the wamp interface
type Swarm struct {
	//general stuff
	peerLock sync.RWMutex
	host     *Host
	ID       SwarmID
	privKey  crypto.PrivKey
	pubKey   crypto.PubKey
	peers    map[PeerID]peerConnection
	public   bool
	ctx      context.Context
	cancel   context.CancelFunc

	//events
	eventLock      sync.RWMutex
	eventCallbacks map[string][]func(Dict)
	eventChannels  map[string][]chan Dict

	//data
	fileStore  *bolt.DB          //store which blocks are available where
	newFiles   chan Dict         //internal distribution of new files
	fetchBlock chan RequestBlock // channel for single blocks which are to be fetched
}

type SwarmID string

func (id SwarmID) Pretty() string {
	return string(id)
}

//not accassible outside the package: should be used via Host only
func newSwarm(host *Host, id SwarmID, public bool, privKey crypto.PrivKey, pubKey crypto.PubKey) *Swarm {

	dir := viper.GetString("directory")
	dir = filepath.Join(dir, id.Pretty())
	os.MkdirAll(dir, os.ModePerm)
	path := filepath.Join(dir, "filestore.db")

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil
	}

	//the context to use for all goroutines
	ctx, cancel := context.WithCancel(context.Background())

	swarm := &Swarm{
		host:           host,
		peerLock:       sync.RWMutex{},
		ID:             id,
		privKey:        privKey,
		pubKey:         pubKey,
		public:         public,
		peers:          make(map[PeerID]peerConnection, 0),
		eventCallbacks: make(map[string][]func(Dict)),
		eventChannels:  make(map[string][]chan Dict),
		newFiles:       make(chan Dict),
		fetchBlock:     make(chan RequestBlock),
		fileStore:      db,
		ctx:            ctx,
		cancel:         cancel}

	swarm.setupDataHandling()

	return swarm
}

func (self *Swarm) Path() string {
	dir := viper.GetString("directory")
	return filepath.Join(dir, self.ID.Pretty())
}

/* Peer handling
 *************  */

//Peer is added, either as readonly, or with write allowance.
// - Connection only succeeds if the other peer has same swarm
// - Connection only succedds if the other peer has us added to swarm
// - Connection is retried periodically whenever it fails
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

	s.connectPeer(pid)

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
	if pc.Event.Connected() {
		return nil
	}

	//open the streams for all needed functionality
	pc.Event = newParticipationMessenger(s.host, s.ID, pid, "Event")
	pc.Data = newParticipationMessenger(s.host, s.ID, pid, "Data")

	//everything was successfull
	s.peers[pid] = pc

	return nil
}

func (s *Swarm) participate(pid PeerID, msg Participate, messenger streamMessenger) {

	//check event type and handle accordingly
	switch msg.Role {

	case "Event":
		messenger.WriteMsg(Success{}, false)
		s.handleEventStream(pid, messenger)

	case "Data":
		messenger.WriteMsg(Success{}, false)
		s.handleDataStream(pid, messenger)

	default:
		messenger.WriteMsg(Error{fmt.Sprintf("Unknown role: %s", msg.Role)}, false)
		messenger.Close()
		return
	}
}

func (s *Swarm) RemovePeer(peer PeerID) {

}

func (s *Swarm) HasPeer(peer PeerID) bool {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	_, ok := s.peers[peer]
	return ok
}

/* General functions
 ******************  */

func (s *Swarm) IsPublic() bool {
	return s.public
}

func (s *Swarm) Close() {
	s.cancel()
	close(s.newFiles)
	close(s.fetchBlock)
}
