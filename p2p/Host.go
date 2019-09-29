// LocalServer
package p2p

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	libp2p "github.com/libp2p/go-libp2p"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	crypto "github.com/libp2p/go-libp2p-crypto"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
	uuid "github.com/satori/go.uuid"
	"github.com/libp2p/go-libp2p-core/peer"
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

	privKey      crypto.PrivKey
	pubKey       crypto.PubKey
	bootstrapper io.Closer

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
func (h *Host) Start(shouldBootstrap bool) error {

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

	//setup the dht
	kadctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	h.dht, err = kaddht.New(kadctx, h.host)
	if err != nil {
		return utils.StackError(err, "Unable to setup distributed hash table")
	}

	//bootstrap if required (means connect to online nodes)
	conf := DefaultBootstrapConfig
	if !shouldBootstrap {		
		conf.BootstrapPeers = func()[]peer.AddrInfo {return make([]peer.AddrInfo, 0)}
	}
	h.bootstrapper, err = bootstrap(h.ID(), h.host, h.dht, conf)
	if err != nil {
		return utils.StackError(err, "Unable to bootstrap p2p node")
	}

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

	//stop bootstrapping
	if h.bootstrapper != nil {
		h.bootstrapper.Close()
	}

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

func (h *Host) GetPath() string {
	return h.path
}

func (h *Host) SetAdress(peer PeerID, addr ma.Multiaddr) error {

	h.host.Peerstore().AddAddr(peer, addr, peerstore.PermanentAddrTTL)
	return nil
}

func (self *Host) SetMultipleAdress(peer PeerID, addrs []ma.Multiaddr) error {

	self.host.Peerstore().AddAddrs(peer, addrs, peerstore.PermanentAddrTTL)
	return nil
}

func (h *Host) Connect(ctx context.Context, peer PeerID) error {

	info := h.host.Peerstore().PeerInfo(peer)
	if len(info.Addrs) == 0 {
		//go find it!
		var err error
		info, err = h.dht.FindPeer(ctx, peer)
		if err != nil {
			return utils.StackError(err, "Unable to find adress of peer, cannot connect")
		}
	}
	return h.host.Connect(ctx, info)
}

func (h *Host) CloseConnection(peer PeerID) error {
	return h.host.Network().ClosePeer(peer)
}

func (h *Host) IsConnected(peer PeerID) bool {

	return len(h.host.Network().ConnsToPeer(peer)) > 0
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
	p2paddr, err := ma.NewMultiaddr("/" + proto + "/" + peer.Pretty())
	if err != nil {
		return nil, err
	}

	pi := h.host.Peerstore().PeerInfo(peer)
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

/*		Search and Find Handling
******************************** */

//provides for 24h, afterwards gets deletet if not provided again
func (h *Host) Provide(ctx context.Context, cid Cid) error {
	return h.dht.Provide(ctx, cid, true)
}

//find peers that provide the given cid. The returned slice can have less than num
//entries, depending on the find results
func (h *Host) FindProviders(ctx context.Context, cid Cid, num int) ([]PeerID, error) {

	input := h.dht.FindProvidersAsync(ctx, cid, num)
	result := make([]PeerID, 0)
	for {
		select {

		case info, more := <-input:
			if !more {
				return result, nil
			}
			h.SetMultipleAdress(PeerID(info.ID), info.Addrs)
			h.EnsureConnection(ctx, PeerID(info.ID))
			result = append(result, PeerID(info.ID))

		case <-ctx.Done():
			return result, nil
		}
	}
	return result, nil
}

//find peers that provide the given cid
func (h *Host) FindProvidersAsync(ctx context.Context, cid Cid, num int) (chan PeerID, error) {

	ret := make(chan PeerID, num)

	go func() {

		found := 0
		dhtCtx, cncl := context.WithCancel(ctx)
		input := h.dht.FindProvidersAsync(dhtCtx, cid, num*2)
		for {
			select {

			case info, more := <-input:
				if !more {
					close(ret)
					cncl()
					return
				}
				if info.ID.Validate() == nil && len(info.Addrs) != 0 && info.ID != h.ID() {

					h.SetMultipleAdress(PeerID(info.ID), info.Addrs)
					h.EnsureConnection(ctx, PeerID(info.ID))

					//found a peer! return it
					ret <- PeerID(info.ID)

					//check if we have enough!
					found = found + 1
					if found >= num {
						close(ret)
						cncl()
						return
					}
				}

			case <-ctx.Done():
				close(ret)
				cncl()
				return
			}
		}
	}()

	return ret, nil
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

//remove swarm from list: only called from Swarm itself in Close()
func (h *Host) FindSwarmMember(ctx context.Context, id SwarmID) (PeerID, error) {

	peerChan := h.findSwarmPeersAsync(ctx, id, 1)

	select {
	case peer, more := <-peerChan:
		if !more {
			//we are not able to find any peers... that is bad!
			return PeerID(""), fmt.Errorf("Unable to find any peer in swarm")
		}
		return peer, nil

	case <-ctx.Done():
		//we did not find any swarm member... return with error
		return PeerID(""), fmt.Errorf("Did not find any swarm members before timeout")
	}
}

//finds and connects other peers in the swarm
func (self *Host) findSwarmPeersAsync(ctx context.Context, id SwarmID, num int) <-chan PeerID {

	//we look for hosts that provide the swarm CID. However, cids are always provided
	//for min. 24h. That means we afterwards need to check if the host still has the
	//swarm active by querying the host API
	ret := make(chan PeerID, num)

	go func() {

		dhtCtx, cncl := context.WithCancel(ctx)
		input := self.dht.FindProvidersAsync(dhtCtx, id.Cid(), num*5)
		found := 0
		for {
			select {

			case info, more := <-input:
				if !more {
					close(ret)
					cncl()
					return
				}
				if info.ID.Validate() == nil && len(info.Addrs) != 0 && info.ID != self.ID() {

					self.SetMultipleAdress(PeerID(info.ID), info.Addrs)
					self.EnsureConnection(ctx, PeerID(info.ID))

					//check host api!
					var has bool
					err := self.Rpc.CallContext(ctx, info.ID, "HostRPCApi", "HasSwarm", id, &has)
					if err != nil {
						break //wait for next peer if contacting failed for whatever reason
					}
					if !has {
						break //wait for next peer if this one does not have the swarm anymore
					}

					//found a peer! return it
					ret <- PeerID(info.ID)

					//check if we have enough!
					found = found + 1
					if found >= num {
						close(ret)
						cncl()
						return
					}
				}

			case <-ctx.Done():
				close(ret)
				cncl()
				return
			}
		}
	}()

	return ret
}
