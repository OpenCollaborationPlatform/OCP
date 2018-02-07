// Swarm.go
package p2p

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"sync"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-net"
	"github.com/libp2p/go-libp2p-peerstore"
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
	log.Println("Message received")

	//handle msg
	//	var swarm *Swarm
	switch msg.MessageType() {

	case PARTICIPATE:
		log.Println("Participate message received")

		/*//check if stream exist and if the peer is allowed to participate
			id := msg.(Participate).Swarm
			sig := msg.(Participate).Signature

			var err error
			swarm, err = sp.host.GetSwarm(id)
			if err != nil {
				messenger.WriteMsg(Error{"Swarm does not exist"})
				messenger.Close()
				return
			}

			//verify if the signature is availbale and correct
			if sig != "" {

				hash:= signatureHash(s.Conn())
				sig, err := rsa.VerifyPKCS1v15(&swarm.pubKey, crypto.SHA256, hash, base64.Decode(sig))
				if err != nil {
					messenger.WriteMsg(Error{fmt.Sprintf("Signature is not valid %s", err)})
					messenger.Close()
				}

				log.Println("Attention: this one should be added as real peer")
			}

			if !swarm.HasPeer(pid) {
				messenger.WriteMsg(Error{fmt.Sprintf("Swarm does not accept peer %s", pid.Pretty())})
				messenger.Close()
				return
			}

		default:
			log.Printf("Swarm received request with unexpected message type \"%s\", aborting", msg.MessageType().String())
			return
		*/
	}

	//let the caller know it was successfull
	err = messenger.WriteMsg(Success{})
	if err != nil {
		log.Printf("Error writing stream in protocol /swarm/1.0.0/: %s", err)
	}
	log.Println("Message returned")

	//swarm.setReadMessengerForPeer(pid, messenger)
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
	peerLock sync.RWMutex
	host     *Host
	ID       SwarmID
	privKey  crypto.PrivKey
	pubKey   crypto.PubKey
	peers    peerstore.Peerstore
	public   bool
	//streams  map[peer.ID]net.Stream
}

type SwarmID string

func (id SwarmID) Pretty() string {
	return string(id)
}

//not accassible outside the package: should be used via Host only
func newSwarm(host *Host, id SwarmID, public bool, privKey crypto.PrivKey, pubKey crypto.PubKey) *Swarm {

	swarm := &Swarm{
		host:     host,
		peerLock: sync.RWMutex{},
		ID:       id,
		privKey:  privKey,
		pubKey:   pubKey,
		public:   public}

	return swarm
}

/* Peer handling
 *************  */

func (s *Swarm) AddPeer(peer PeerID) error {

	//TODO: check if peer exist already

	//call the peer swarm protocol
	stream, err := s.host.host.NewStream(context.Background(), peer.ID, swarmURI)
	if err != nil {
		return err
	}
	log.Println("Swarm stream opened")

	//generate the signature
	sig, err := s.privKey.Sign(s.signatureMessage(stream.Conn()))
	if err != nil {
		stream.Close()
		return fmt.Errorf("Encrytion error: %s", err)
	}

	//write the participate message
	msg := Participate{}
	msg.Swarm = s.ID
	msg.Signature = base64.StdEncoding.EncodeToString(sig)

	log.Println("Participate message bevore write")
	messenger := newStreamMessenger(stream)
	err = messenger.WriteMsg(msg)
	log.Println("Message writen")
	if err != nil {
		messenger.Close()
		return err
	}
	ret, err := messenger.ReadMsg()
	log.Println("Return message received")
	if err != nil {
		messenger.Close()
		return err
	}

	switch ret.MessageType() {

	case ERROR:
		messenger.Close()
		return fmt.Errorf(ret.(Error).Reason)
	case SUCCESS:
		log.Println("Successfully added peer")
	}

	return nil

}

func (s *Swarm) RemovePeer(peer PeerID) {

}

func (s *Swarm) HasPeer(peer PeerID) bool {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	for _, p := range s.peers.Peers() {
		if p == peer.ID {
			return true
		}
	}
	return false
}

/* Event handling
 *************  */

func (s *Swarm) PostEvent() {

}

func (s *Swarm) RegisterEvent() {

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
	return []byte(hashMsg)
}
