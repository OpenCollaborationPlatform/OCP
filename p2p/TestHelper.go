package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	blocks "github.com/ipfs/go-block-format"
	//ipfslog "github.com/ipfs/go-log"
	//	ipfswriter "github.com/ipfs/go-log/writer"
	hclog "github.com/hashicorp/go-hclog"
	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-crypto"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	uuid "github.com/satori/go.uuid"

	"github.com/spf13/viper"
)

var testport int = 9015

func init() {
	//disable logging for this tests
	//log.SetOutput(ioutil.Discard)

	//	ipfswriter.Configure(ipfswriter.Output(ioutil.Discard)) // ipfslog "github.com/ipfs/go-log/writer"#
	//	ipfswriter.LevelInfo()
	//	ipfslog.GetSubsystems() //just to not need to remove import
	//ipfslog.SetLogLevel("pubsub", "Debug")

}

//for external modules that need a host for tessting
func MakeTemporaryTestingHost(path string) (*Host, error) {
	return temporaryHost(path)
}

func MakeTemporaryTwoHostNetwork(path string) (*Host, *Host, error) {
	host1, err := MakeTemporaryTestingHost(path)
	if err != nil {
		return nil, nil, err
	}
	host2, err := MakeTemporaryTestingHost(path)
	if err != nil {
		return nil, nil, err
	}

	host1.SetMultipleAdress(host2.ID(), host2.OwnAddresses())
	host2.SetMultipleAdress(host1.ID(), host1.OwnAddresses())
	host1.Connect(context.Background(), host2.ID())

	return host1, host2, nil
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

	//a logger to discard all logs (to not disturb print output)
	testLogger := hclog.New(&hclog.LoggerOptions{
		Output: ioutil.Discard,
	})

	//setup the correct url and port for the node
	viper.Set("p2p.uri", "127.0.0.1")
	viper.Set("p2p.port", testport)
	testport = testport + 1

	//start the host
	h := NewHost(nil, testLogger)
	err = h.Start(false)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func randomHostWithoutDataSerivce() (*Host, error) {

	addr := fmt.Sprintf("/ip4/%s/tcp/%d", "127.0.0.1", testport)
	testport = testport + 1

	ctx := context.Background()
	p2phost, err := libp2p.New(ctx, libp2p.ListenAddrStrings(addr))
	if err != nil {
		return nil, err
	}

	//a logger to discard all logs (to not disturb print output)
	testLogger := hclog.New(&hclog.LoggerOptions{
		Output: ioutil.Discard,
	})

	host := &Host{host: p2phost, swarms: make([]*Swarm, 0), logger: testLogger}
	host.Rpc = newRpcService(host)

	kadctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	host.dht, err = kaddht.New(kadctx, host.host)
	if err != nil {
		return nil, utils.StackError(err, "Unable to setup distributed hash table")
	}
	//host.dht.Bootstrap(kadctx)

	return host, nil
}

func randomBlock(size int) blocks.Block {

	data := make([]byte, size)
	rand.Read(data)

	return blocks.NewBlock(data)
}

func repeatableBlock(size int) blocks.Block {

	data := RepeatableData(size)
	return blocks.NewBlock(data)
}

func RepeatableData(size int) []byte {

	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}

	return data
}

//compares two directories if they are equal
func compareDirectories(path1, path2 string) error {

	files1, err := ioutil.ReadDir(path1)
	if err != nil {
		return utils.StackError(err, "Unable to read dir %v", path1)
	}

	files2, err := ioutil.ReadDir(path2)
	if err != nil {
		return utils.StackError(err, "Unable to read dir %v", path2)
	}

	if len(files1) != len(files2) {
		return fmt.Errorf("\nNumber of files not equal")
	}

	for i, file := range files1 {

		other := files2[i]

		if file.Name() != other.Name() {
			return fmt.Errorf("\nNames not equal")
		}
		if file.Size() != other.Size() {
			return fmt.Errorf("\nSize not equal")
		}
		if file.IsDir() != other.IsDir() {
			return fmt.Errorf("\nType not equal")
		}

		//compare the file content
		subpath1 := filepath.Join(path1, file.Name())
		subpath2 := filepath.Join(path2, other.Name())
		if !file.IsDir() {
			f1, err := os.Open(subpath1)
			if err != nil {
				return fmt.Errorf("\nCannot open file 1")
			}

			f2, err := os.Open(subpath2)
			if err != nil {
				return fmt.Errorf("\nCannot open file 2")
			}

			data1 := make([]byte, file.Size())
			f1.Read(data1)

			data2 := make([]byte, other.Size())
			f2.Read(data2)

			if !bytes.Equal(data1, data2) {
				return fmt.Errorf("\nData for file %v not equal", i)
			}
		} else {
			res := compareDirectories(subpath1, subpath2)
			if res != nil {
				return fmt.Errorf("\nSubdir not equal: %v", res)
			}
		}
	}

	return nil
}
