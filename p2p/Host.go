// LocalServer
package p2p

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

	crypto "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	swarm "github.com/libp2p/go-libp2p-swarm"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/spf13/viper"
	msmux "github.com/whyrusleeping/go-smux-multistream"
	spdy "github.com/whyrusleeping/go-smux-spdystream"
	yamux "github.com/whyrusleeping/go-smux-yamux"
)

type Host struct {
	host *bhost.BasicHost
}

//Host creates p2p host which manages all peer connections
func NewHost() *Host {

	return &Host{}
}

// Starts the listening for connections and the bootstrap prozess
func (h *Host) Start() error {

	//load the keys
	content, err := ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "public"))
	if err != nil {
		log.Fatalf("Public key could not be read: %s\n", err)
	}
	pub, err := crypto.UnmarshalPublicKey(content)
	if err != nil {
		log.Fatalf("Public key is invalid: %s\n", err)
	}
	content, err = ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "private"))
	if err != nil {
		log.Fatalf("Private key could not be read: %s\n", err)
	}
	priv, err := crypto.UnmarshalPrivateKey(content)
	if err != nil {
		log.Fatalf("Private key is invalid: %s\n", err)
	}

	//our ID
	pid, _ := peer.IDFromPublicKey(pub)

	// Create the multiaddress we listen on
	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", viper.GetString("p2p.uri"), viper.GetInt("p2p.port")))

	if err != nil {
		return err
	}

	// Create a peerstore
	ps := pstore.NewPeerstore()

	// we use secure connections
	ps.AddPrivKey(pid, priv)
	ps.AddPubKey(pid, pub)

	// Set up stream multiplexer
	tpt := msmux.NewBlankTransport()
	tpt.AddTransport("/yamux/1.0.0", yamux.DefaultTransport)
	tpt.AddTransport("/spdy/3.1.0", spdy.Transport)

	// Create swarm (implements libP2P Network)
	swrm, err := swarm.NewSwarmWithProtector(
		context.Background(),
		[]ma.Multiaddr{addr},
		pid,
		ps,
		nil,
		tpt,
		nil,
	)
	if err != nil {
		return err
	}

	netw := (*swarm.Network)(swrm)

	ctx := context.Background()
	opts := bhost.HostOpts{EnableRelay: false}
	h.host, err = bhost.NewHost(ctx, netw, &opts)
	if err != nil {
		return err
	}

	//bootstrap
	nodes := viper.GetStringSlice("p2p.bootstrap")
	for _, value := range nodes {

		ipfsaddr, err := ma.NewMultiaddr(value)
		if err != nil {
			log.Fatalln(err)
		}

		pid, err := ipfsaddr.ValueForProtocol(ma.P_IPFS)
		if err != nil {
			log.Fatalln(err)
		}

		peerid, err := peer.IDB58Decode(pid)
		if err != nil {
			log.Fatalln(err)
		}

		// Decapsulate the /ipfs/<peerID> part from the target
		// /ip4/<a.b.c.d>/ipfs/<peer> becomes /ip4/<a.b.c.d>
		targetPeerAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", peer.IDB58Encode(peerid)))
		targetAddr := ipfsaddr.Decapsulate(targetPeerAddr)

		h.host.Peerstore().AddAddr(peerid, targetAddr, pstore.PermanentAddrTTL)
		h.host.Connect(ctx, h.host.Peerstore().PeerInfo(peerid))
	}

	log.Printf("Host successful stated at %s", addr.String())

	return nil
}

func (h *Host) Stop() error {

	return h.host.Close()
}

func (h *Host) Peers() []peer.ID {

	return h.host.Network().Peers()
}
