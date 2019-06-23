// LocalServer
package p2p

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-crypto"
	p2phost "github.com/libp2p/go-libp2p-host"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/spf13/viper"
)

type Host struct {
	host       p2phost.Host
	swarmMutex sync.RWMutex
	swarms     []*Swarm

	privKey  crypto.PrivKey
	pubKey   crypto.PubKey
	overlord Overlord

	//serivces the host provides
	Rpc   *hostRpcService
	Data  DataService
	Event *hostEventService

	//some internal data
	path string
}

//Host creates p2p host which manages all peer connections
func NewHost(overlord Overlord) *Host {

	return &Host{swarms: make([]*Swarm, 0), overlord: overlord}
}

// Starts the listening for connections and the bootstrap prozess
func (h *Host) Start() error {

	//store the path
	h.path = viper.GetString("directory")

	//load the keys
	content, err := ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "public"))
	if err != nil {
		log.Fatalf("Public key could not be read: %s\n", err)
	}
	pub, err := crypto.UnmarshalPublicKey(content)
	if err != nil {
		log.Fatalf("Private key is invalid: %s\n", err)
	}
	h.pubKey = pub

	content, err = ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "private"))
	if err != nil {
		log.Fatalf("Private key could not be read: %s\n", err)
	}
	priv, err := crypto.UnmarshalPrivateKey(content)
	if err != nil {
		log.Fatalf("Private key is invalid: %s\n", err)
	}
	h.privKey = priv

	// Create the multiaddress we listen on
	addr := fmt.Sprintf("/ip4/%s/tcp/%d", viper.GetString("p2p.uri"), viper.GetInt("p2p.port"))

	//setup default p2p host
	ctx := context.Background()
	h.host, err = libp2p.New(ctx,
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
		libp2p.NATPortMap(),
	)

	if err != nil {
		return err
	}

	//bootstrap
	nodes := viper.GetStringSlice("p2p.bootstrap")
	for _, value := range nodes {

		addr, err := ma.NewMultiaddr(value)
		if err != nil {
			log.Printf("Not a valid bootstrap node: %s", err)
		}
		info, err := peerstore.InfoFromP2pAddr(addr)
		if err != nil {
			log.Printf("Invalid multiadress: %v", value)
			continue
		}
		//connect
		tctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err = h.host.Connect(tctx, *info)
		if err != nil {
			log.Printf("Cannot connect to  %v", value)
			continue
		}
	}

	//add the services
	h.Rpc = newRpcService(h)
	h.Data, err = NewDataService(h)
	if err != nil {
		return utils.StackError(err, "Unable to startup data service")
	}
	h.Event, err = newHostEventService(h)
	if err != nil {
		return utils.StackError(err, "Unable to startup event service")
	}

	log.Printf("Host successful stated at %s", addr)

	return nil
}

func (h *Host) Stop() error {

	//stop swarms
	h.swarmMutex.RLock()
	defer h.swarmMutex.RUnlock()
	for _, swarm := range h.swarms {
		swarm.Close()
	}

	//stop services
	if h.Event != nil {
		h.Event.Stop()
	}
	if h.Data != nil {
		h.Data.Close()
	}

	return h.host.Close()
}

func (h *Host) SetAdress(peer PeerID, addr ma.Multiaddr) error {

	h.host.Peerstore().AddAddr(peer.pid(), addr, peerstore.PermanentAddrTTL)
	return nil
}

func (self *Host) SetMultipleAdress(peer PeerID, addrs []ma.Multiaddr) error {

	self.host.Peerstore().AddAddrs(peer.pid(), addrs, peerstore.PermanentAddrTTL)
	return nil
}

func (h *Host) Connect(ctx context.Context, peer PeerID) error {

	info := h.host.Peerstore().PeerInfo(peer.pid())
	return h.host.Connect(ctx, info)
}

func (h *Host) CloseConnection(peer PeerID) error {
	return h.host.Network().ClosePeer(peer.pid())
}

func (h *Host) IsConnected(peer PeerID) bool {

	for _, p := range h.host.Network().Peers() {
		if peer.pid() == p {
			if len(h.host.Network().ConnsToPeer(peer.pid())) > 0 {
				return true
			}
			return false
		}
	}
	return false
}

//returns all known peers
func (h *Host) Peers(connectedOnly bool) []PeerID {

	if connectedOnly {
		result := make([]PeerID, len(h.host.Network().Peers()))
		for i, peer := range h.host.Network().Peers() {
			result[i] = PeerID(peer)
		}
		return result
	}

	//this gives all known peers
	peers := h.host.Peerstore().Peers()
	result := make([]PeerID, len(peers))
	for i, p := range peers {
		result[i] = PeerID(p)
	}

	return result
}

func (h *Host) OwnAddresses() []ma.Multiaddr {

	return h.host.Addrs()
}

func (h *Host) Addresses(peer PeerID) ([]ma.Multiaddr, error) {

	proto := ma.ProtocolWithCode(ma.P_IPFS).Name
	p2paddr, err := ma.NewMultiaddr("/" + proto + "/" + peer.pid().Pretty())
	if err != nil {
		return nil, err
	}

	pi := h.host.Peerstore().PeerInfo(peer.pid())
	var addrs []ma.Multiaddr
	for _, addr := range pi.Addrs {
		addrs = append(addrs, addr.Encapsulate(p2paddr))
	}

	return addrs, nil
}

func (h *Host) ID() PeerID {
	return PeerID(h.host.ID())
}

func (h *Host) Keys() (crypto.PrivKey, crypto.PubKey) {
	return h.privKey, h.pubKey
}

/*		Swarm Handling
****************************** */

func (h *Host) Swarms() []*Swarm {
	h.swarmMutex.RLock()
	defer h.swarmMutex.RUnlock()
	return h.swarms
}

func (h *Host) CreateSwarm(id SwarmID) *Swarm {

	h.swarmMutex.Lock()
	defer h.swarmMutex.Unlock()
	swarm := newSwarm(h, id, h.overlord)
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
