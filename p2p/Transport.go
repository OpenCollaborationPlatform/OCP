/* This is a transport layer for hashicorps raft implementation that uses the
   libp2p connection. It is a blant copy from libp2praft, however, we need multiple
   share states and hence multiple distuinged transports. The only way doing is is
   to use a different protocol for each.
*/

package p2p

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"time"

	"github.com/hashicorp/raft"
	gostream "github.com/hsanjuan/go-libp2p-gostream"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

const RaftProtocolBase protocol.ID = "/raft/1.0.0/"

var logLogger = log.New(ioutil.Discard, "", 0)

// streamLayer an implementation of raft.StreamLayer for use
// with raft.NetworkTransportConfig.
type streamLayer struct {
	host  host.Host
	l     net.Listener
	proto protocol.ID
}

func newStreamLayer(h host.Host, name string) (*streamLayer, error) {

	proto := protocol.ID(string(RaftProtocolBase) + name)
	listener, err := gostream.Listen(h, proto)
	if err != nil {
		return nil, err
	}

	return &streamLayer{
		host:  h,
		l:     listener,
		proto: proto,
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

	return gostream.Dial(sl.host, pid, sl.proto)
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

func NewLibp2pTransport(h host.Host, name string, timeout time.Duration) (*raft.NetworkTransport, error) {
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
