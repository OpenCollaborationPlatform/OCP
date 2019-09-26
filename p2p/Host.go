// LocalServer
package p2p

import (
	"os"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	libp2p "github.com/libp2p/go-libp2p"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	crypto "github.com/libp2p/go-libp2p-crypto"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
)

//RPC Api of the host
type HostRPCApi struct {
	host *Host
}

func (self HostRPCApi) HasSwarm(ctx context.Context, id SwarmID, has *bool) error {
	_, err := self.host.GetSwarm(id)
	*has = (err == nil)
	return nil
}

type Host struct {
	host       p2phost.Host
	swarmMutex sync.RWMutex
	swarms     []*Swarm

	privKey crypto.PrivKey
	pubKey  crypto.PubKey

	//find service
	dht *kaddht.IpfsDHT

	//serivces the host provides
	Rpc   *hostRpcService
	Data  DataService
	Event *hostEventService

	//some internal data
	path string
}

//Host creates p2p host which manages all peer connections
func NewHost() *Host {

	return &Host{swarms: make([]*Swarm, 0)}
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
		info, err := peer.AddrInfoFromP2pAddr(addr)
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

	//setup the dht
	kadctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	h.dht, err = kaddht.New(kadctx, h.host)
	if err != nil {
		return utils.StackError(err, "Unable to setup distributed hash table")
	}
	conf := kaddht.BootstrapConfig{
		Queries: 3,                // how many queries to run per period
		Period:  5 * time.Minute,  // how often to run periodic bootstrap.
		Timeout: 10 * time.Second} // how long to wait for a bootstrap query to run
	h.dht.BootstrapWithConfig(kadctx, conf)

	//add the services
	h.Rpc = newRpcService(h)
	err = h.Rpc.Register(&HostRPCApi{h})
	if err != nil {
		return utils.StackError(err, "Unable to register Host API")
	}
	h.Data, err = NewDataService(h)
	if err != nil {
		return utils.StackError(err, "Unable to startup data service")
	}
	h.Event, err = newHostEventService(h)
	if err != nil {
		return utils.StackError(err, "Unable to startup event service")
	}

	return nil
}

func (h *Host) Stop(ctx context.Context) error {

	//stop data replication

	//stop swarms
	for _, swarm := range h.Swarms() {
		swarm.Close(ctx)
	}

	//stop services
	if h.Event != nil {
		h.Event.Stop()
	}
	if h.Data != nil {
		h.Data.Close()
	}
	if h.Rpc != nil {
		h.Rpc.Close()
	}

	//stop dht
	h.dht.Close()

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
	if len(info.Addrs) == 0 {
		//go find it!
		var err error
		info, err = h.dht.FindPeer(ctx, peer.pid())
		if err != nil {
			return utils.StackError(err, "Unable to find adress of peer, cannot connect")
		}
	}
	return h.host.Connect(ctx, info)
}

func (h *Host) CloseConnection(peer PeerID) error {
	return h.host.Network().ClosePeer(peer.pid())
}

func (h *Host) IsConnected(peer PeerID) bool {

	return len(h.host.Network().ConnsToPeer(peer.pid())) > 0
}

func (h *Host) EnsureConnection(ctx context.Context, peer PeerID) error {

	if peer == h.ID() {
		return nil
	}

	if h.IsConnected(peer) {
		return nil
	}
	return h.Connect(ctx, peer)
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

	//return new list to make sure the returned value can be manipulated
	//without chaning the host slice
	newList := make([]*Swarm, len(h.swarms))
	copy(newList, h.swarms)

	return newList
}

func (h *Host) CreateSwarm(ctx context.Context, states []State) (*Swarm, error) {

	id := SwarmID(uuid.NewV4().String())
	return h.CreateSwarmWithID(ctx, id, states)
}

func (h *Host) CreateSwarmWithID(ctx context.Context, id SwarmID, states []State) (*Swarm, error) {
	
	h.swarmMutex.Lock()
	defer h.swarmMutex.Unlock()
	
	//We create the swarm, that means net by definition... lets clear all folders
	//if the id was there already once!
	swpath := filepath.Join(h.path, string(id))
	if _, err := os.Stat(swpath); !os.IsNotExist(err) {
		err := os.RemoveAll(swpath)
		if err != nil {
			return nil, utils.StackError(err, "Unable to clear swarm folder: existed once already")
		}
	}
	
	swarm, err := newSwarm(ctx, h, id, states, true, NoPeers())
	if err != nil {
		return nil, utils.StackError(err, "Unable to create swarm")
	}
	if swarm != nil {
		h.swarms = append(h.swarms, swarm)
	}
	return swarm, nil
}

func (h *Host) JoinSwarm(ctx context.Context, id SwarmID, states []State, knownPeers []PeerID) (*Swarm, error) {

	h.swarmMutex.Lock()
	defer h.swarmMutex.Unlock()

	swarm, err := newSwarm(ctx, h, id, states, false, knownPeers)
	if err != nil {
		return swarm, err
	}
	if swarm != nil {
		h.swarms = append(h.swarms, swarm)
	}
	return swarm, nil
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

//remove swarm from list: only called from Swarm itself in Close()
func (h *Host) removeSwarm(id SwarmID) {

	h.swarmMutex.Lock()
	defer h.swarmMutex.Unlock()
	for i, swarm := range h.swarms {
		if swarm.ID == id {
			h.swarms = append(h.swarms[:i], h.swarms[i+1:]...)
			return
		}
	}
}
