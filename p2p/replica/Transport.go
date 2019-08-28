package replica

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/hashicorp/raft"
	gostream "github.com/libp2p/go-libp2p-gostream"
)

const raftBaseProtocol string = "/raft/1.0.0/"

// This provides a custom logger for the network transport
// which intercepts messages and rewrites them to our own logger
type logForwarder struct{}

var logLogger = log.New(&logForwarder{}, "", 0)

// Write forwards to our go-log logger.
// According to https://golang.org/pkg/log/#Logger.Output
// it is called per line.
func (fw *logForwarder) Write(p []byte) (n int, err error) {
	// t := strings.TrimSuffix(string(p), "\n")
	// switch {
	// case strings.Contains(t, "[DEBUG]"):
	// 	logger.Debug(strings.TrimPrefix(t, "[DEBUG] raft-net: "))
	// case strings.Contains(t, "[WARN]"):
	// 	logger.Warning(strings.TrimPrefix(t, "[WARN]  raft-net: "))
	// case strings.Contains(t, "[ERR]"):
	// 	logger.Error(strings.TrimPrefix(t, "[ERR] raft-net: "))
	// case strings.Contains(t, "[INFO]"):
	// 	logger.Info(strings.TrimPrefix(t, "[INFO] raft-net: "))
	// default:
	// 	logger.Debug(t)
	// }
	return len(p), nil
}

// streamLayer an implementation of raft.StreamLayer for use
// with raft.NetworkTransportConfig.
type streamLayer struct {
	host host.Host
	l    net.Listener
	id   protocol.ID
}

func newStreamLayer(h host.Host, name string) (*streamLayer, error) {

	protocol := protocol.ID(raftBaseProtocol + name)
	listener, err := gostream.Listen(h, protocol)
	if err != nil {
		return nil, err
	}

	return &streamLayer{
		host: h,
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

// ServerAddr takes a raft.ServerID and checks that it is a valid PeerID and
// that our libp2p host has at least one address to contact it. It then returns
// the ServerID as ServerAddress. On all other cases it will throw an error.
func (ap *addrProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	pid, err := peer.IDB58Decode(string(id))
	if err != nil {
		return "", fmt.Errorf("bad peer ID: %s", id)
	}
	addrs := ap.h.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		return "", fmt.Errorf("libp2p host does not know peer %s", id)
	}
	return raft.ServerAddress(id), nil

}

func NewLibp2pTransport(h host.Host, timeout time.Duration, name string) (*raft.NetworkTransport, error) {
	provider := &addrProvider{h}
	stream, err := newStreamLayer(h, name)
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
