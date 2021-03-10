package replica

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/hashicorp/raft"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	gostream "github.com/libp2p/go-libp2p-gostream"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
)

const raftBaseProtocol string = "/ocp/raft/1.0.0/"

// This provides a custom logger for the network transport
// which intercepts messages and rewrites them to our own logger
type logForwarder struct{}

var logLogger = log.New(&logForwarder{}, "", 0)

// Write forwards to our go-log logger.
// According to https://golang.org/pkg/log/#Logger.Output
// it is called per line.
func (fw *logForwarder) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// streamLayer an implementation of raft.StreamLayer for use
// with raft.NetworkTransportConfig.
type streamLayer struct {
	host host.Host
	dht  *kaddht.IpfsDHT
	l    net.Listener
	id   protocol.ID
	name string
}

func newStreamLayer(h host.Host, dht *kaddht.IpfsDHT, name string) (*streamLayer, error) {

	protocol := protocol.ID(raftBaseProtocol + name)
	listener, err := gostream.Listen(h, protocol)
	if err != nil {
		return nil, wrapConnectionError(err, Error_Setup)
	}

	return &streamLayer{
		host: h,
		dht:  dht,
		l:    listener,
		id:   protocol,
		name: name,
	}, nil
}

func (sl *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if sl.host == nil {
		return nil, newInternalError(Error_Setup, "StreamLayer not initialized")
	}

	pid, err := peer.IDB58Decode(string(address))
	if err != nil {
		return nil, wrapInternalError(err, Error_Invalid_Data)
	}

	//check if we know the peer, or find it otherwise
	addrs := sl.host.Peerstore().Addrs(pid)
	if len(addrs) == 0 && pid != sl.host.ID() {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		info, err := sl.dht.FindPeer(ctx, pid)
		if err != nil {
			return nil, wrapConnectionError(err, Error_Unavailable)
		}
		if info.ID == "" || len(info.Addrs) == 0 {
			return nil, newConnectionError(Error_Unavailable, "Peer not found, no connection possible")
		}
		sl.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := gostream.Dial(ctx, sl.host, pid, sl.id)
	sl.host.ConnManager().Protect(pid, sl.name)
	return c, wrapConnectionError(err, Error_Process)
}

func (sl *streamLayer) Accept() (net.Conn, error) {
	c, err := sl.l.Accept()
	return c, wrapConnectionError(err, Error_Process)
}

func (sl *streamLayer) Addr() net.Addr {
	return sl.l.Addr()
}

func (sl *streamLayer) Close() error {
	return sl.l.Close()
}

type addrProvider struct {
	h host.Host
}

// ServerAddr takes a raft.ServerID and checks that it is a valid PeerID
func (ap *addrProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	_, err := peer.IDB58Decode(string(id))
	if err != nil {
		return "", newInternalError(Error_Operation_Invalid, "Bad peer ID", "id", id)
	}

	return raft.ServerAddress(id), nil
}

func NewLibp2pTransport(h host.Host, dht *kaddht.IpfsDHT, timeout time.Duration, name string) (*raft.NetworkTransport, error) {
	provider := &addrProvider{h}
	stream, err := newStreamLayer(h, dht, name)
	if err != nil {
		return nil, wrapConnectionError(err, Error_Setup)
	}

	// This is a configuration for raft.NetworkTransport
	// initialized with our own StreamLayer and Logger.
	// We set MaxPool to 0 so the NetworkTransport does not
	// pool connections. This allows re-using already stablished
	// TCP connections, for example, which are expensive to create.
	// We are, however, multiplexing streams over an already created
	// Libp2p connection, which is cheap. We don't need to re-use
	// streams.
	cfg := &raft.NetworkTransportConfig{
		ServerAddressProvider: provider,
		Logger:                nil,
		Stream:                stream,
		MaxPool:               0,
		Timeout:               timeout,
	}

	return raft.NewNetworkTransportWithConfig(cfg), nil
}
