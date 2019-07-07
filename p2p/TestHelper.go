package p2p

import (
	"CollaborationNode/p2p/replica"
	"CollaborationNode/utils"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	blocks "github.com/ipfs/go-block-format"

	ipfslog "github.com/ipfs/go-log"
	ipfswriter "github.com/ipfs/go-log/writer"
	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-crypto"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
)

var testport int = 9015
var overlord *replica.TestOverlord

func init() {
	//disable logging for this tests
	//log.SetOutput(ioutil.Discard)

	//use a test overlord
	overlord = replica.NewTestOverlord()

	ipfswriter.Configure(ipfswriter.Output(ioutil.Discard)) // ipfslog "github.com/ipfs/go-log/writer"
	ipfswriter.LevelInfo()
	ipfslog.GetSubsystems() //just to not need to remove import
	//ipfslog.SetDebugLogging()

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
	h := NewHost(overlord)
	err = h.Start()
	if err != nil {
		return nil, err
	}

	//inform the overlord about new data
	rsaPubKey := pub.(*crypto.RsaPublicKey)
	overlord.SetApiData(h.ID().String(), *rsaPubKey)

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

	host := &Host{host: p2phost, swarms: make([]*Swarm, 0)}
	host.Rpc = newRpcService(host)

	return host, nil
}

func randomBlock(size int) blocks.Block {

	data := make([]byte, size)
	rand.Read(data)

	block := P2PFileBlock{"RandomBlock", data}
	return block.ToBlock()
}

func repeatableBlock(size int) blocks.Block {

	data := repeatableData(size)
	block := P2PFileBlock{"RepeatableBlock", data}
	return block.ToBlock()
}

func repeatableData(size int) []byte {

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
