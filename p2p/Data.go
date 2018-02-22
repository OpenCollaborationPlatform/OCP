// Datahandling for swarm operations
package p2p

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/mr-tron/base58/base58"
	"github.com/spf13/viper"
)

const (
	blocksize int64 = 1 << (10 * 2)
)

type block struct {
	Offset int64
	Size   int64
	Hash   [32]byte
}

type file struct {
	Blocks []block
	Hash   [32]byte
}

func (f *file) calculateHash() [32]byte {

	data, err := json.Marshal(f.Blocks)
	if err != nil {
		var data [32]byte
		return data
	}
	return sha256.Sum256(data)
}

/* Data handling
 *************  */

func (s *Swarm) DistributeData(data []byte) string {

	return ""
}

func (s *Swarm) DistributeFile(path string) (string, error) {

	return s.addLocalFile(path)
}

func (s *Swarm) DropDataOrFile(path string) string {
	return ""
}

//swarm functions for handling blocks
//***********************************
/*
storing a file is:
bucket[file.Hash] {
	path string
	bucket["received"] {
		hash: block
	}
	bucket["fetching"] {
		hash: block
	}
}*/

func (s *Swarm) setupFile(f file) error {

	if s.hasFile(f.Hash) {
		return nil
	}

	dir := viper.GetString("directory")
	name := base58.Encode(f.Hash[:])
	path := filepath.Join(dir, "files", name)
	_, err := os.Create(path)
	if err != nil {
		return err
	}

	//store the path
	err = s.fileStore.Update(func(tx *bolt.Tx) error {

		bucket, _ := tx.CreateBucketIfNotExists(f.Hash[:])
		bucket.Put([]byte("path"), []byte(name))
		bucket.CreateBucketIfNotExists([]byte("received"))
		bucket, _ = bucket.CreateBucketIfNotExists([]byte("fetching"))

		//store all blocks
		for _, blk := range f.Blocks {
			data, _ := json.Marshal(blk)
			err := bucket.Put(blk.Hash[:], data)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}

func (s *Swarm) hasFile(fileHash [32]byte) bool {

	err := s.fileStore.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(fileHash[:])
		if bucket == nil {
			return fmt.Errorf("No such file")
		}

		return nil
	})

	return err == nil
}

func (s *Swarm) hasBlock(fileHash [32]byte, b block) bool {

	exist := false
	err := s.fileStore.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(fileHash[:])
		if bucket == nil {
			return nil
		}
		bucket = bucket.Bucket([]byte("received"))
		if bucket == nil {
			return nil
		}

		data := bucket.Get(b.Hash[:])
		if data == nil {
			return nil
		}

		fmt.Println("Exist = true")
		exist = true
		return nil
	})
	if err != nil {
		fmt.Printf("Has block: %s\n", err)
	}

	return exist
}

//read the data associated with given block
func (s *Swarm) getBlock(fileHash [32]byte, b block) ([]byte, error) {

	//get the block entry
	var path string
	var storedBlock block

	err := s.fileStore.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(fileHash[:])
		if bucket == nil {
			return fmt.Errorf("No such file available")
		}

		//get the path (relative one!)
		p := bucket.Get([]byte("path"))
		if p == nil {
			return fmt.Errorf("No path saved for file")
		}
		path = string(p)

		bucket = bucket.Bucket([]byte("received"))
		if bucket == nil {
			return fmt.Errorf("This block was not fetched yet")
		}

		data := bucket.Get(b.Hash[:])
		if data == nil {
			return fmt.Errorf("No such block available")
		}

		//see if we really have it...
		err := json.Unmarshal(data, &storedBlock)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	//we use our own block and only copy the hash, to make sure that the data is correct
	dir := viper.GetString("directory")
	path = filepath.Join(dir, "files", path)
	if b.Offset != storedBlock.Offset || b.Size != storedBlock.Size {
		return nil, fmt.Errorf("Block does not match stored one")
	}
	return getBlock(path, storedBlock)
}

//Writes the block to the file, but the file must be created already!
func (s *Swarm) writeBlock(fileHash [32]byte, b block, data []byte) error {

	fmt.Println("Write block")
	if s.hasBlock(fileHash, b) {
		return nil
	}

	fmt.Println("Start writing block")
	//get the filepath first
	var path string
	var storedBlock block
	err := s.fileStore.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(fileHash[:])
		if bucket == nil {
			return fmt.Errorf("No such file available")
		}

		//get the path (relative one!)
		p := bucket.Get([]byte("path"))
		if p == nil {
			return fmt.Errorf("No path saved for file")
		}
		path = string(p)

		bucket = bucket.Bucket([]byte("fetching"))
		if bucket == nil {
			return fmt.Errorf("Cannot access fetched blocks")
		}

		data := bucket.Get(b.Hash[:])
		if data == nil {
			return fmt.Errorf("No such block available")
		}

		//see if we really have it...
		err := json.Unmarshal(data, &storedBlock)
		if err != nil {
			return err
		}
		return nil
	})

	fmt.Printf("Path: %v\n", path)
	fmt.Printf("Block: %v\n", storedBlock)

	if err != nil {
		fmt.Println("Return error")
		return err
	}

	//consistency check
	if b.Size != storedBlock.Size || b.Offset != storedBlock.Offset {
		fmt.Println("return on consistency check")
		return fmt.Errorf("Block not consistent with stored information")
	}

	//store block
	fmt.Println("store block")
	dir := viper.GetString("directory")
	path = filepath.Join(dir, "files", path)
	err = putBlock(path, b, data)
	if err != nil {
		fmt.Printf("return on store block: %s\n", err)
		return err
	}

	//write the info about receiving into the storage
	fmt.Println("update database")
	err = s.fileStore.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(fileHash[:])
		if bucket == nil {
			return fmt.Errorf("No such file available: create it first!")
		}

		fetching := bucket.Bucket([]byte("fetching"))
		if fetching == nil {
			return fmt.Errorf("Error accessing fetching blocks")
		}
		err := fetching.Delete(storedBlock.Hash[:])
		if err != nil {
			return err
		}

		received := bucket.Bucket([]byte("received"))
		if received == nil {
			return fmt.Errorf("Error accessing received blocks")
		}
		dat, _ := json.Marshal(storedBlock)
		err = received.Put(storedBlock.Hash[:], dat)
		return err
	})

	return err
}

//adds a local file to the network
func (s *Swarm) addLocalFile(path string) (string, error) {

	//build the blocks!
	file, err := blockifyFile(path)
	if err != nil {
		return "", err
	}

	//check if it exists already...
	if s.hasFile(file.Hash) {
		return base58.Encode(file.Hash[:]), nil
	}

	//create the data in our store
	err = s.setupFile(file)
	if err != nil {
		return "", err
	}

	//we copy everything over
	fi, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fi.Close()

	stat, _ := os.Stat(path)
	fmt.Printf("Filesize: %d\n", stat.Size())

	outbuf := make([]byte, blocksize)
	for _, block := range file.Blocks {

		fmt.Printf("Handle block: Offset=%d, Size=%d\n", block.Offset, block.Size)
		_, err = fi.Seek(block.Offset, io.SeekStart)
		if err != nil {
			return "", err
		}

		buf := outbuf[:block.Size]
		n, err := io.ReadFull(fi, buf)
		if err != nil {
			return "", err
		}
		fmt.Printf("Read size: %d\n", n)

		err = s.writeBlock(file.Hash, block, buf)
		if err != nil {
			return "", err
		}
	}

	return base58.Encode(file.Hash[:]), nil
}

//adds a network file to the local storage
func (s *Swarm) addFile(f file) {

	//check if it exists already...
	if s.hasFile(f.Hash) {
		return
	}

	//create the data in our store
	s.setupFile(f)

	//TODO: start searching online for the file
}

func (s *Swarm) Files() []string {

	//we iterate over the database and build all file objects
	var files []string
	s.fileStore.View(func(tx *bolt.Tx) error {

		fcursor := tx.Cursor()
		for fhash, _ := fcursor.First(); fhash != nil; fhash, _ = fcursor.Next() {

			//iterate over the blocks
			/*bucket := tx.Bucket(fhash)
			bcursor := bucket.Cursor()
			for bhash, data := bcursor.First(); bhash != nil; bhash, data = bcursor.Next() {

				var blk map[string]interface{}
				err := json.Unmarshal(data, &blk)
				if err != nil {
					return err
				}

				currFile.Blocks = append(currFile.Blocks, blk["block"].(block))
			}*/
			name := base58.Encode(fhash)
			files = append(files, name)
		}

		return nil
	})

	return files
}

//this function handles events. Note that this stream not only has Event messages,
//but is also used for some other internal messages, e.g. for data distribution
func (s *Swarm) handleDataStream(pid PeerID, messenger streamMessenger) {

	go func() {
		for {
			msg, err := messenger.ReadMsg()

			if err != nil {
				log.Printf("Error reading message: %s", err.Error())
				return
			}

			//verify the requester of the data and see if he is allowed to receive data
			//as data is only sent to one peer, not by flooding, signature is not required

			switch msg.MessageType() {

			case REQUESTBLOCK:
				rqst := msg.(*RequestBlock)

				//check if we have the block
				if !s.hasBlock(rqst.File.Hash, rqst.Block) {
					messenger.WriteMsg(Error{"Block not available"})
					continue
				}

				//return the block
				data, err := s.getBlock(rqst.File.Hash, rqst.Block)
				if err != nil {
					messenger.WriteMsg(Error{"Block not available"})
					continue
				}
				err = messenger.WriteMsg(BlockData{File: rqst.File, Block: rqst.Block, Data: data})
				if err != nil {
					messenger.Close()
					return
				}

			default:
				messenger.WriteMsg(Error{"MEssage type not supportet"})
				messenger.Close()
				return
			}
		}
	}()
}

//Helper functions
//****************

//generates a p2p file descriptor from a real file.
// - This does split the files into blocks
// - same file returns exactly the same desciptor, undependend of path
func blockifyFile(path string) (file, error) {

	fi, err := os.Open(path)
	if err != nil {
		return file{}, err
	}
	defer fi.Close()

	info, err := fi.Stat()
	if err != nil {
		return file{}, err
	}
	size := info.Size()

	//we want 1Mb slices, lets see how much we need
	blocknum := int(math.Ceil(float64(size) / float64(blocksize)))
	blocks := make([]block, blocknum)
	data := make([]byte, blocksize)
	i := 0 //need that later
	for ; i < blocknum; i++ {

		n, err := fi.Read(data)
		if err != nil && err != io.EOF {
			return file{}, err
		}

		//the last block can be smaller than blocksize, hence always use n
		fmt.Printf("Create Block: Offset=%d, Size=%d\n", int64(i)*blocksize, int64(n))
		blocks[i] = block{int64(i) * blocksize, int64(n), sha256.Sum256(data[:n])}
	}

	//hash the blocks:
	f := file{Blocks: blocks}
	f.Hash = f.calculateHash()
	return f, nil
}

func putBlock(file string, info block, data []byte) error {

	//consistency checks...
	hash := sha256.Sum256(data)
	if hash != info.Hash || int64(len(data)) != info.Size {
		return fmt.Errorf("Data does not match block")
	}

	fi, err := os.OpenFile(file, os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println("Error on open")
		return err
	}
	defer fi.Close()

	n, err := fi.WriteAt(data, info.Offset)
	if err != nil {
		fmt.Println("Error on write")
		return err
	}

	if int64(n) != info.Size {
		return fmt.Errorf("Could not write all data")
	}

	return nil
}

func getBlock(file string, info block) ([]byte, error) {

	fi, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	_, err = fi.Seek(info.Offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	outbuf := make([]byte, info.Size)
	_, err = io.ReadFull(fi, outbuf)
	if err != nil && err != io.EOF {
		return nil, err
	}

	hash := sha256.Sum256(outbuf)
	if hash != info.Hash {
		return nil, fmt.Errorf("data in file did not match.")
	}

	return outbuf, nil
}

func getMany(file string, blocks []block) chan []byte {

	//create the output channel
	channel := make(chan []byte)

	//start the goroutine

	return channel
}

func putMany(file string, channel chan []byte) {

	//
}
