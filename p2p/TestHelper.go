package p2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	blocks "github.com/ipfs/go-block-format"

	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-crypto"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
)

var testport int = 9015

func init() {
	//disable logging for this tests
	//log.SetOutput(ioutil.Discard)
	//ipfslog.Configure(ipfslog.Output(ioutil.Discard))// ipfslog "github.com/ipfs/go-log/writer"
	//ipfslog.SetDebugLogging() // ipfslog "github.com/ipfs/go-log"
}

//creates a random host. The used directory will be a sibling of the provided one.
//note that the returned host is already started!
func temporaryHost(dir string) (*Host, error) {

	//build and set a new withing a new directory
	nodeDir := filepath.Join(dir, uuid.NewV4().String())

	//ensure viper is setup so that the host reads it correctly
	viper.Set("directory", nodeDir)

	//setup the folder
	_, err := os.Stat(nodeDir)
	if !os.IsNotExist(err) {
		if err := os.RemoveAll(nodeDir); err != nil {
			return nil, fmt.Errorf("Error cleaning the node folder \"%s\": %s", nodeDir, err)
		}
	}
	if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("Cannot create the node folder \"%s\": %s\n", nodeDir, err)
	}

	//Generate our node keys (and hence identity)
	priv, pub, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("Could not create key pair: %s\n", err)
	}

	bytes, _ := crypto.MarshalPublicKey(pub)
	err = ioutil.WriteFile(filepath.Join(nodeDir, "public"), bytes, 0644)
	if err != nil {
		return nil, fmt.Errorf("Could not create public key file: %s\n", err)
	}

	bytes, _ = crypto.MarshalPrivateKey(priv)
	err = ioutil.WriteFile(filepath.Join(nodeDir, "private"), bytes, 0644)
	if err != nil {
		return nil, fmt.Errorf("Could not create private key file: %s\n", err)
	}

	//setup the correct url and port for the node
	viper.Set("p2p.uri", "127.0.0.1")
	viper.Set("p2p.port", testport)
	testport = testport + 1

	//start the host
	h := NewHost()
	return h, h.Start()
}

func randomHostWithoutDataSerivce() (*Host, error) {

	addr := fmt.Sprintf("/ip4/%s/tcp/%d", "127.0.0.1", testport)
	testport = testport + 1

	ctx := context.Background()
	p2phost, err := libp2p.New(ctx, libp2p.ListenAddrStrings(addr))
	if err != nil {
		return nil, err
	}

	host := &Host{host: p2phost, swarms: make([]*Swarm, 0)}
	host.Rpc = newRpcService(host)

	return host, nil
}

func randomBlock(size int) blocks.Block {

	data := make([]byte, size)
	rand.Read(data)
	return blocks.NewBlock(data)
}

func repeatableBlock(size int) blocks.Block {

	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}

	return blocks.NewBlock(data)
}
