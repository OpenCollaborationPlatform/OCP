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
	hclog "github.com/hashicorp/go-hclog"
	libp2p "github.com/libp2p/go-libp2p"
	p2pevent "github.com/libp2p/go-libp2p-core/event"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	p2pnet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	p2prouting "github.com/libp2p/go-libp2p-core/routing"
	crypto "github.com/libp2p/go-libp2p-crypto"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery"
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

/*
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
*/

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
	subs        p2pevent.Subscription

	//serivces the host provides
	Rpc   *hostRpcService
	Data  DataService
	Event *hostEventService

	//some internal data
	path         string
	logger       hclog.Logger
	wamp         *nxclient.Client
	reachability p2pnet.Reachability
}

//Host creates p2p host which manages all peer connections
func NewHost(router *connection.Router, logger hclog.Logger) *Host {

	var client *nxclient.Client = nil
	if router != nil {
		client, _ = router.GetLocalClient("p2p")
	}

	return &Host{swarms: make([]*Swarm, 0),
		wamp:         client,
		reachability: p2pnet.ReachabilityUnknown,
		logger:       logger,
	}
}

// Starts the listening for connections and the bootstrap prozess
func (h *Host) Start(shouldBootstrap bool) error {

	//store the path
	h.path = viper.GetString("directory")

	//load the keys
	content, err := ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "public"))
	if err != nil {
		err := utils.StackError(err, "Public key could not be read")
		return err
	}
	pub, err := crypto.UnmarshalPublicKey(content)
	if err != nil {
		return utils.StackError(err, "Public key is invalid")
	}
	h.pubKey = pub

	content, err = ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "private"))
	if err != nil {
		return utils.StackError(err, "Priveta key could not be read")
	}
	priv, err := crypto.UnmarshalPrivateKey(content)
	if err != nil {
		return utils.StackError(err, "Private key is invalid")
	}
	h.privKey = priv

	// Create the multiaddress we listen on
	addr := fmt.Sprintf("/ip4/%s/tcp/%d", viper.GetString("p2p.uri"), viper.GetInt("p2p.port"))

	//setup default p2p host
	ctx, cncl := context.WithCancel(context.Background())
	h.serviceCtx = ctx
	h.serviceCncl = cncl

	//dht creation function to allow passing relay as option
	var dht *kaddht.IpfsDHT
	newDHT := func(h p2phost.Host) (p2prouting.PeerRouting, error) {
		var err error
		dhtOpts := []kaddht.Option{kaddht.ProtocolPrefix("/ocp"), kaddht.Mode(kaddht.ModeAutoServer)}
		dht, err = kaddht.New(ctx, h, dhtOpts...)
		return dht, err
	}

	hostOpts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
		libp2p.NATPortMap(),
		libp2p.Routing(newDHT),
		libp2p.EnableAutoRelay(),
	}
	if viper.GetBool("p2p.natservice.enable") {
		h.logger.Debug("Start up NatService")
		hostOpts = append(hostOpts, libp2p.EnableNATService())
		limit := viper.GetInt("p2p.natservice.limit")
		peerlimit := viper.GetInt("p2p.natservice.peerlimit")
		hostOpts = append(hostOpts, libp2p.AutoNATServiceRateLimit(limit, peerlimit, 60*time.Second))
	}
	h.host, err = libp2p.New(ctx, hostOpts...)
	h.dht = dht
	if err != nil {
		return utils.StackError(err, "Unable to setup P2P host")
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
		h.wamp.Register("ocp.p2p.reachability", h._reach, wamp.Dict{})
	}

	//and the wamp events
	if h.wamp != nil {
		sub, err := h.host.EventBus().Subscribe([]interface{}{new(p2pevent.EvtLocalReachabilityChanged),
			new(p2pevent.EvtLocalAddressesUpdated)})
		if err != nil {
			return utils.StackError(err, "Unable to setup p2p events")
		}
		h.subs = sub

		go func() {
			h.logger.Debug("Startup event loop")
			for e := range h.subs.Out() {
				switch e := e.(type) {

				case p2pevent.EvtLocalReachabilityChanged:
					h.reachability = e.Reachability
					h.logger.Info("Reachability changed", "value", e.Reachability)
					h.wamp.Publish("ocp.p2p.reachabilityChanged", wamp.Dict{}, wamp.List{h.reachability.String()}, wamp.Dict{})

				//case p2pevent.EvtPeerConnectednessChanged:
				//this is never emited, we keep it here, maybe later libp2p implements it
				//h.logger.Debug("Peer conectedness event received", "event", e)
				//h.wamp.Publish("ocp.p2p.peerChanged", wamp.Dict{}, wamp.List{e.Peer.Pretty(), e.Connectedness.String()}, wamp.Dict{})

				case p2pevent.EvtLocalAddressesUpdated:
					h.logger.Info("Local addresses changed", "current", e.Current, "removed", e.Removed)
					h.wamp.Publish("ocp.p2p.addressesChanged", wamp.Dict{}, wamp.List{e.Current}, wamp.Dict{})

				default:
					h.logger.Warn("Received unhandled event", "event", e, "type", fmt.Sprintf("%T", e))
				}
			}
			h.logger.Debug("Shutdown event loop")
		}()

		//for coneection add/change event we need to use the network notifee, as the event is not emited
		confnc := func(n p2pnet.Network, c p2pnet.Conn) {
			peer := c.RemotePeer().Pretty()
			h.logger.Debug("Peer connected event", "peer", peer)
			if peer[:2] == "Qm" {
				h.wamp.Publish("ocp.p2p.peerConnected", wamp.Dict{}, wamp.List{peer}, wamp.Dict{})
			}
		}
		disconfnc := func(n p2pnet.Network, c p2pnet.Conn) {
			peer := c.RemotePeer().Pretty()
			h.logger.Debug("Peer disconnected event", "peer", peer)
			if peer[:2] == "Qm" {
				h.wamp.Publish("ocp.p2p.peerDisconnected", wamp.Dict{}, wamp.List{peer}, wamp.Dict{})
			}
		}
		h.host.Network().Notify(&p2pnet.NotifyBundle{ConnectedF: confnc, DisconnectedF: disconfnc})
	}

	//bootstrap if required (means connect to online nodes)
	conf := GetDefaultBootstrapConfig(h.logger.Named("Bootstrap"))
	if !shouldBootstrap {
		conf.BootstrapPeers = func() []peer.AddrInfo { return make([]peer.AddrInfo, 0) }
	}
	h.bootstrapper, err = bootstrap(h.ID(), h.host, h.dht, conf)
	if err != nil {
		return utils.StackError(err, "Unable to bootstrap p2p node")
	}

	h.logger.Info("Host started")
	return nil
}

func (h *Host) Stop(ctx context.Context) error {

	//stop events
	//h.subs.Close() crahes in tests

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

func (h *Host) Reachability() string {
	return h.reachability.String()
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

//Finds a peer active in current swarm
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

	addrs := make([]string, 0)
	for _, addr := range self.OwnAddresses() {
		result := addr.String()
		if !short {
			result += "/ipfs/" + self.ID().Pretty()
		}
		addrs = append(addrs, result)
	}

	return nxclient.InvokeResult{Args: wamp.List{addrs}}
}

func (self *Host) _peers(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		return nxclient.InvokeResult{Args: wamp.List{"No arguments allowed for this function"}, Err: wamp.URI("ocp.error")}
	}

	peers := make([]string, 0)
	for _, peer := range self.Peers(true) {

		str := peer.Pretty()
		if str[:2] == "Qm" {
			peers = append(peers, str)
		}
	}

	return nxclient.InvokeResult{Args: wamp.List{peers}}
}

func (self *Host) _reach(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		return nxclient.InvokeResult{Args: wamp.List{"No arguments allowed for this function"}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{Args: wamp.List{self.reachability.String()}}
}
