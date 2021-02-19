package p2p

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	p2phost "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/spf13/viper"

	"github.com/jbenet/goprocess"
	"github.com/jbenet/goprocess/context"
	"github.com/jbenet/goprocess/periodic"

	"github.com/libp2p/go-libp2p-core/routing"
)

// BootstrapConfig specifies parameters used in an IpfsNode's network
// bootstrapping process.
type BootstrapConfig struct {
	// MinPeerThreshold governs whether to bootstrap more connections. If the
	// node has less open connections than this number, it will open connections
	// to the bootstrap nodes. From there, the routing system should be able
	// to use the connections to the bootstrap nodes to connect to even more
	// peers. Routing systems like the IpfsDHT do so in their own Bootstrap
	// process, which issues random queries to find more peers.
	MinPeerThreshold int

	// Period governs the periodic interval at which the node will
	// attempt to bootstrap. The bootstrap process is not very expensive, so
	// this threshold can afford to be small (<=30s).
	Period time.Duration

	// ConnectionTimeout determines how long to wait for a bootstrap
	// connection attempt before cancelling it.
	ConnectionTimeout time.Duration

	// BootstrapPeers is a function that returns a set of bootstrap peers
	// for the bootstrap process to use. This makes it possible for clients
	// to control the peers the process uses at any moment.
	BootstrapPeers func() []peer.AddrInfo

	// The logger used to log all bootstrap messages
	Logger hclog.Logger
}

// DefaultBootstrapConfig specifies default sane parameters for bootstrapping.

func GetDefaultBootstrapConfig(log hclog.Logger) BootstrapConfig {

	var DefaultBootstrapConfig = BootstrapConfig{
		MinPeerThreshold:  4,
		Period:            30 * time.Second,
		ConnectionTimeout: (30 * time.Second) / 3, // Perod / 3
		Logger:            log,
	}

	DefaultBootstrapConfig.BootstrapPeers = func() []peer.AddrInfo {

		addrs := make([]peer.AddrInfo, 0)

		nodes := viper.GetStringSlice("p2p.bootstrap")
		for _, value := range nodes {
			addr, err := ma.NewMultiaddr(value)
			if err != nil {
				log.Error("Invalid bootstrap address provided", "address", value)
				continue
			}
			info, err := peer.AddrInfoFromP2pAddr(addr)
			if err != nil {
				log.Error("Invalid bootstrap address provided", "address", value)
				continue
			}
			addrs = append(addrs, *info)
		}

		return addrs
	}

	return DefaultBootstrapConfig
}

// Bootstrap kicks off IpfsNode bootstrapping. This function will periodically
// check the number of open connections and -- if there are too few -- initiate
// connections to well-known bootstrap peers. It also kicks off subsystem
// bootstrapping (i.e. routing).
func bootstrap(id peer.ID, host p2phost.Host, rt routing.Routing, cfg BootstrapConfig) (io.Closer, error) {

	// make a signal to wait for one bootstrap round to complete.
	doneWithRound := make(chan struct{})

	if len(cfg.BootstrapPeers()) == 0 {
		// We *need* to bootstrap but we have no bootstrap peers
		// configured *at all*, inform the user.
		cfg.Logger.Warn("no bootstrap nodes configured: go-ipfs may have difficulty connecting to the network")
	}

	// the periodic bootstrap function -- the connection supervisor
	periodic := func(worker goprocess.Process) {
		ctx := goprocessctx.OnClosingContext(worker)

		bootstrapRound(ctx, host, cfg)
		<-doneWithRound
	}

	// kick off the node's periodic bootstrapping
	proc := periodicproc.Tick(cfg.Period, periodic)
	proc.Go(periodic) // run one right now.

	// kick off Routing.Bootstrap
	if rt != nil {
		ctx := goprocessctx.OnClosingContext(proc)
		if err := rt.Bootstrap(ctx); err != nil {
			proc.Close()
			return nil, err
		}
	}

	doneWithRound <- struct{}{}
	close(doneWithRound) // it no longer blocks periodic
	return proc, nil
}

func bootstrapRound(ctx context.Context, host p2phost.Host, cfg BootstrapConfig) error {

	ctx, cancel := context.WithTimeout(ctx, cfg.ConnectionTimeout)
	defer cancel()

	// get bootstrap peers from config. retrieving them here makes
	// sure we remain observant of changes to client configuration.
	peers := cfg.BootstrapPeers()
	// determine how many bootstrap connections to open
	connected := host.Network().Peers()
	if len(connected) >= cfg.MinPeerThreshold {
		cfg.Logger.Debug("Connection round skipped", "connected", len(connected), "required", cfg.MinPeerThreshold)
		return nil
	}
	numToDial := cfg.MinPeerThreshold - len(connected)

	// filter out bootstrap nodes we are already connected to
	var notConnected []peer.AddrInfo
	for _, p := range peers {
		if host.Network().Connectedness(p.ID) != network.Connected {
			notConnected = append(notConnected, p)
		}
	}

	// if connected to all bootstrap peer candidates, exit
	if len(notConnected) < 1 {
		cfg.Logger.Debug("No more peers to create additional connections", "required", numToDial)
		return fmt.Errorf("Not enough bootstrap peers")
	}

	// connect to a random susbset of bootstrap candidates
	randSubset := randomSubsetOfPeers(notConnected, numToDial)

	cfg.Logger.Debug("Tries to connect to nodes", "nodes", randSubset)
	return bootstrapConnect(ctx, host, randSubset, cfg.Logger)
}

func bootstrapConnect(ctx context.Context, ph p2phost.Host, peers []peer.AddrInfo, logger hclog.Logger) error {
	if len(peers) < 1 {
		return fmt.Errorf("Not enough bootstrap peers")
	}

	errs := make(chan error, len(peers))
	var wg sync.WaitGroup
	for _, p := range peers {

		// performed asynchronously because when performed synchronously, if
		// one `Connect` call hangs, subsequent calls are more likely to
		// fail/abort due to an expiring context.
		// Also, performed asynchronously for dial speed.

		wg.Add(1)
		go func(p peer.AddrInfo) {
			defer wg.Done()

			ph.Peerstore().AddAddrs(p.ID, p.Addrs, peerstore.PermanentAddrTTL)
			if err := ph.Connect(ctx, p); err != nil {
				logger.Debug("Failed to connect with", "peer", p.ID, "eeror", err)
				errs <- err
				return
			}
			logger.Info("Connected with node", "peer", p.ID)
		}(p)
	}
	wg.Wait()

	// our failure condition is when no connection attempt succeeded.
	// So drain the errs channel, counting the results.
	close(errs)
	count := 0
	var err error
	for err = range errs {
		if err != nil {
			count++
		}
	}
	if count == len(peers) {
		return fmt.Errorf("failed to bootstrap. %s", err)
	}
	return nil
}

func randomSubsetOfPeers(in []peer.AddrInfo, max int) []peer.AddrInfo {
	if max > len(in) {
		max = len(in)
	}

	out := make([]peer.AddrInfo, max)
	for i, val := range rand.Perm(len(in))[:max] {
		out[i] = in[val]
	}
	return out
}
