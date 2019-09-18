package replica

import (
	"context"
	"errors"
	"fmt"
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

const raftBaseProtocol string = "/ocpraft/1.0.0/"

// This provides a custom logger for the network transport
// which intercepts messages and rewrites them to our own logger
type logForwarder struct{}

var logLogger = log.New(&logForwarder{}, "", 0)

// Write forwards to our go-log logger.
// According to https://golang.org/pkg/log/#Logger.Output
// it is called per line.
func (fw *logForwarder) Write(p []byte) (n int, err error) {
	/*	 t := strings.TrimSuffix(string(p), "\n")
		 switch {
		 case strings.Contains(t, "[DEBUG]"):
		 	log.Println(strings.TrimPrefix(t, "[DEBUG] raft-net: "))
		 case strings.Contains(t, "[WARN]"):
		 	log.Println(strings.TrimPrefix(t, "[WARN]  raft-net: "))
		 case strings.Contains(t, "[ERR]"):
		 	log.Println(strings.TrimPrefix(t, "[ERR] raft-net: "))
		 case strings.Contains(t, "[INFO]"):
		 	log.Println(strings.TrimPrefix(t, "[INFO] raft-net: "))
		 default:
		 	log.Println(t)
		 }*/
	return len(p), nil
}

// streamLayer an implementation of raft.StreamLayer for use
// with raft.NetworkTransportConfig.
type streamLayer struct {
	host host.Host
	dht  *kaddht.IpfsDHT
	l    net.Listener
	id   protocol.ID
}

func newStreamLayer(h host.Host, dht *kaddht.IpfsDHT, name string) (*streamLayer, error) {

	protocol := protocol.ID(raftBaseProtocol + name)
	listener, err := gostream.Listen(h, protocol)
	if err != nil {
		return nil, err
	}

	return &streamLayer{
		host: h,
		dht:  dht,
		l:    listener,
		id:   protocol,
	}, nil
}

func (sl *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if sl.host == nil {
		return nil, errors.New("streamLayer not initialized")
	}

	pid, err := peer.IDB58Decode(string(address))
	if err != nil {
		return nil, err
	}

	//check if we know the peer, or find it otherwise
	addrs := sl.host.Peerstore().Addrs(pid)
	if len(addrs) == 0 && pid != sl.host.ID() {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		info, err := sl.dht.FindPeer(ctx, pid)
		if err != nil {
			return nil, err
		}
		if info.ID == "" || len(info.Addrs) == 0 {
			return nil, fmt.Errorf("Peer not found, no connection possible")
		}
		sl.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	}

	return gostream.Dial(sl.host, pid, sl.id)
}

func (sl *streamLayer) Accept() (net.Conn, error) {
	return sl.l.Accept()
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
		return "", fmt.Errorf("bad peer ID: %s", id)
	}

	return raft.ServerAddress(id), nil
}

func NewLibp2pTransport(h host.Host, dht *kaddht.IpfsDHT, timeout time.Duration, name string) (*raft.NetworkTransport, error) {
	provider := &addrProvider{h}
	stream, err := newStreamLayer(h, dht, name)
	if err != nil {
		return nil, err
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
		Logger:                logLogger,
		Stream:                stream,
		MaxPool:               0,
		Timeout:               timeout,
	}

	return raft.NewNetworkTransportWithConfig(cfg), nil
}
