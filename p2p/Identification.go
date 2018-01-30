// Identification.go
package p2p

import (
	"io/ioutil"
	"log"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-peer"
)

type PeerID struct {
	peer.ID
}

func LoadPeerIDFromPublicKeyFile(file string) PeerID {

	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Public key could not be read: %s\n", err)
	}
	key, err := crypto.UnmarshalPublicKey(content)
	if err != nil {
		log.Fatalf("Public key is invalid: %s\n", err)
	}
	id, _ := peer.IDFromPublicKey(key)

	return PeerID{id}
}
