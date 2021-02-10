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

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/wamp"
	logging "github.com/ipfs/go-log/v2"
	libp2p "github.com/libp2p/go-libp2p"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	crypto "github.com/libp2p/go-libp2p-crypto"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery"
	ma "github.com/multiformats/go-multiaddr"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
)

var log = logging.Logger("P2P")

//RPC Api of the host
type HostRPCApi struct {
	host *Host
}

func (self HostRPCApi) HasSwarm(ctx context.Context, id SwarmID, has *bool) error {
	_, err := self.host.GetSwarm(id)
	*has = (err == nil)
	return nil
}

//little helper for mdns discovery
type discoveryHandler struct {
	ctx  context.Context
	host p2phost.Host
}

func (dh *discoveryHandler) HandlePeerFound(p peer.AddrInfo) {
	log.Info("connecting to discovered peer: ", p)
	ctx, cancel := context.WithTimeout(dh.ctx, 30*time.Second)
	defer cancel()
	if err := dh.host.Connect(ctx, p); err != nil {
		log.Warningf("failed to connect to peer %s found by discovery: %s", p.ID, err)
	}
}

type Host struct {
	host       p2phost.Host
	swarmMutex sync.RWMutex
	swarms     []*Swarm

	privKey      crypto.PrivKey
	pubKey       crypto.PubKey
	bootstrapper io.Closer

	//services
	serviceCtx  context.Context
	serviceCncl context.CancelFunc
	dht         *kaddht.IpfsDHT
	mdns        mdns.Service

	//serivces the host provides
	Rpc   *hostRpcService
	Data  DataService
	Event *hostEventService

	//some internal data
	path string
	wamp *nxclient.Client
}

//Host creates p2p host which manages all peer connections
func NewHost(router *connection.Router) *Host {

	var client *nxclient.Client = nil
	if router != nil {
		client, _ = router.GetLocalClient("p2p")
	}

	return &Host{swarms: make([]*Swarm, 0), wamp: client}
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
	ctx, cncl := context.WithCancel(context.Background())
	h.serviceCtx = ctx
	h.serviceCncl = cncl
	h.host, err = libp2p.New(ctx,
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
		libp2p.NATPortMap(),
	)

	if err != nil {
		return err
	}

	//setup mdns discovery (careful: the context does control lifetime of some internal mdns things)
	//--> mdns works fine, but for whatever reason this makes the p2p test fail,
	/*tag := "_ocp-discovery._udp.local"
	h.mdns, err = mdns.NewMdnsService(ctx, h.host, 30*time.Second, tag)
	if err != nil {
		h.mdns = nil
	} else {
		h.mdns.RegisterNotifee(&discoveryHandler{h.serviceCtx, h.host})
	}*/

	//setup the dht (careful: the context does control lifetime of some internal dht things)
	dhtOpts := []kaddht.Option{kaddht.Mode(kaddht.ModeServer), kaddht.ProtocolPrefix("/ocp")}
	h.dht, err = kaddht.New(ctx, h.host, dhtOpts...)
	if err != nil {
		return utils.StackError(err, "Unable to setup distributed hash table")
	}

	//bootstrap if required (means connect to online nodes)
	conf := GetDefaultBootstrapConfig()
	if !shouldBootstrap {
		conf.BootstrapPeers = func() []peer.AddrInfo { return make([]peer.AddrInfo, 0) }
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

	//add the wamp functions
	if h.wamp != nil {
		h.wamp.Register("ocp.p2p.id", h._id, wamp.Dict{})
		h.wamp.Register("ocp.p2p.addresses", h._addresses, wamp.Dict{})
		h.wamp.Register("ocp.p2p.peers", h._peers, wamp.Dict{})
	}

	log.Info("P2P host started")
	return nil
}

func (h *Host) Stop(ctx context.Context) error {

	//stop bootstrapping
	if h.bootstrapper != nil {
		h.bootstrapper.Close()
	}

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

	//stop dht and mdns
	if h.serviceCncl != nil {
		h.serviceCncl()
	}
	if h.mdns != nil {
		h.mdns.Close()
	}
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

func (h *Host) Routing() *kaddht.IpfsDHT {
	return h.dht
}

/*		Search and Find Handling
******************************** */

//provides for 24h, afterwards gets deletet if not provided again
func (h *Host) Provide(ctx context.Context, cid utils.Cid) error {

	if len(h.host.Network().Conns()) == 0 {
		return fmt.Errorf("Cannot provide, no connected peers")
	}

	return h.dht.Provide(ctx, cid.P2P(), true)
}

//find peers that provide the given cid. The returned slice can have less than num
//entries, depending on the find results
func (h *Host) FindProviders(ctx context.Context, cid utils.Cid, num int) ([]PeerID, error) {

	input := h.dht.FindProvidersAsync(ctx, cid.P2P(), num)
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
func (h *Host) FindProvidersAsync(ctx context.Context, cid utils.Cid, num int) (chan PeerID, error) {

	ret := make(chan PeerID, num)

	go func() {

		found := 0
		dhtCtx, cncl := context.WithCancel(ctx)
		input := h.dht.FindProvidersAsync(dhtCtx, cid.P2P(), num*2)
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

/*		Wamp API: wamp functions for normal Host ones
*********************************************************** */

func (self *Host) _id(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		return nxclient.InvokeResult{Args: wamp.List{"No arguments allowed for this function"}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{Args: wamp.List{self.ID().Pretty()}}
}

func (self *Host) _addresses(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Argument required: shortened adresses true/false"}, Err: wamp.URI("ocp.error")}
	}

	short, ok := inv.Arguments[0].(bool)
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be boolean"}, Err: wamp.URI("ocp.error")}
	}

	addrs := make(wamp.List, 0)
	for _, addr := range self.OwnAddresses() {
		result := addr.String()
		if !short {
			result += "/ipfs/" + self.ID().Pretty()
		}
		addrs = append(addrs, result)
	}

	return nxclient.InvokeResult{Args: addrs}
}

func (self *Host) _peers(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		return nxclient.InvokeResult{Args: wamp.List{"No arguments allowed for this function"}, Err: wamp.URI("ocp.error")}
	}

	peers := make(wamp.List, len(self.Peers(true)))
	for i, peer := range self.Peers(true) {

		peers[i] = peer.Pretty()
	}

	return nxclient.InvokeResult{Args: peers}
}
