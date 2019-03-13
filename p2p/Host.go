// LocalServer
package p2p

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"

	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-crypto"
	p2phost "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/spf13/viper"
)

type Host struct {
	host       p2phost.Host
	swarmMutex sync.RWMutex
	swarms     []*Swarm

	swarmProto *swarmProtocol
}

//Host creates p2p host which manages all peer connections
func NewHost() *Host {

	return &Host{swarms: make([]*Swarm, 0)}
}

// Starts the listening for connections and the bootstrap prozess
func (h *Host) Start() error {

	//load the keys
	content, err := ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "public"))
	if err != nil {
		log.Fatalf("Public key could not be read: %s\n", err)
	}
	content, err = ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "private"))
	if err != nil {
		log.Fatalf("Private key could not be read: %s\n", err)
	}
	priv, err := crypto.UnmarshalPrivateKey(content)
	if err != nil {
		log.Fatalf("Private key is invalid: %s\n", err)
	}

	// Create the multiaddress we listen on
	addr := fmt.Sprintf("/ip4/%s/tcp/%d", viper.GetString("p2p.uri"), viper.GetInt("p2p.port"))

	//setup default p2p host
	ctx := context.Background()
	h.host, err = libp2p.New(ctx,
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
		libp2p.DefaultTransports,
		libp2p.DefaultMuxers,
		libp2p.DefaultSecurity,
		libp2p.NATPortMap(),
	)

	if err != nil {
		return err
	}

	//bootstrap
	nodes := viper.GetStringSlice("p2p.bootstrap")
	for _, value := range nodes {

		ipfsaddr, err := ma.NewMultiaddr(value)
		if err != nil {
			log.Printf("Not a valid bootstrap node: %s", err)
		}
		if err := h.Connect(ipfsaddr); err != nil {
			log.Printf("Bootstrap error: %s", err)
		}
	}

	//add the protocols
	h.swarmProto = newSwarmProtocol(h)

	log.Printf("Host successful stated at %s", addr)

	return nil
}

func (h *Host) Stop() error {

	return h.host.Close()
}

func (h *Host) Connect(ipfsaddr ma.Multiaddr) error {

	pid, err := ipfsaddr.ValueForProtocol(ma.P_IPFS)
	if err != nil {
		return err
	}

	peerid, err := peer.IDB58Decode(pid)
	if err != nil {
		return err
	}

	// Decapsulate the /ipfs/<peerID> part from the target
	// /ip4/<a.b.c.d>/ipfs/<peer> becomes /ip4/<a.b.c.d>
	targetPeerAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", peer.IDB58Encode(peerid)))
	targetAddr := ipfsaddr.Decapsulate(targetPeerAddr)

	h.host.Peerstore().AddAddr(peerid, targetAddr, pstore.PermanentAddrTTL)
	h.host.Connect(context.Background(), h.host.Peerstore().PeerInfo(peerid))

	return nil
}

func (h *Host) CloseConnection(peer PeerID) error {
	return h.host.Network().ClosePeer(peer.ID)
}

func (h *Host) IsConnected(peer PeerID) bool {

	for _, p := range h.host.Network().Peers() {
		if peer.ID == p {
			if len(h.host.Network().ConnsToPeer(peer.ID)) > 0 {
				return true
			}
			return false
		}
	}
	return false
}

func (h *Host) Peers() []PeerID {

	result := make([]PeerID, len(h.host.Network().Peers()))
	for i, peer := range h.host.Network().Peers() {
		result[i] = PeerID{peer}
	}
	return result
}

func (h *Host) OwnAddresses() []ma.Multiaddr {

	proto := ma.ProtocolWithCode(ma.P_IPFS).Name
	p2paddr, _ := ma.NewMultiaddr("/" + proto + "/" + h.host.ID().Pretty())

	var addrs []ma.Multiaddr
	for _, addr := range h.host.Addrs() {
		addrs = append(addrs, addr.Encapsulate(p2paddr))
	}
	return addrs
}

func (h *Host) Addresses(peer PeerID) ([]ma.Multiaddr, error) {

	proto := ma.ProtocolWithCode(ma.P_IPFS).Name
	p2paddr, err := ma.NewMultiaddr("/" + proto + "/" + peer.Pretty())
	if err != nil {
		return nil, err
	}

	pi := h.host.Peerstore().PeerInfo(peer.ID)
	var addrs []ma.Multiaddr
	for _, addr := range pi.Addrs {
		addrs = append(addrs, addr.Encapsulate(p2paddr))
	}

	return addrs, nil
}

func (h *Host) ID() PeerID {
	return PeerID{h.host.ID()}
}

/*		Swarm Handling
****************************** */

func (h *Host) Swarms() []*Swarm {
	h.swarmMutex.RLock()
	defer h.swarmMutex.RUnlock()
	return h.swarms
}

func (h *Host) CreateSwarm(id SwarmID, privKey crypto.PrivKey, public bool) *Swarm {

	h.swarmMutex.Lock()
	defer h.swarmMutex.Unlock()
	swarm := newSwarm(h, id, public, privKey, privKey.GetPublic())
	if swarm != nil {
		h.swarms = append(h.swarms, swarm)
	}
	return swarm
}

func (h *Host) GetSwarm(id SwarmID) (*Swarm, error) {

	h.swarmMutex.RLock()
	defer h.swarmMutex.RUnlock()
	for _, swarm := range h.swarms {
		if swarm.ID == id {
			return swarm, nil
		}
	}
	return nil, fmt.Errorf("No such swarm exists")
}
