// Identification.go
package p2p

import (
	"io/ioutil"
	"log"

	crypto "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
)

type PeerID struct {
	peer.ID
}

func PeerIDFromString(id string) (PeerID, error) {

	peerid, err := peer.IDB58Decode(id)
	if err != nil {
		return PeerID{}, err
	}
	return PeerID{peerid}, nil
}

func PeerIDFromPublicKey(pk crypto.PubKey) (PeerID, error) {
	id, err := peer.IDFromPublicKey(pk)
	return PeerID{id}, err
}

func PeerIDFromPublicKeyFile(file string) PeerID {

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
