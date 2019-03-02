// Datahandling for swarm operations
package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/mr-tron/base58/base58"
	"github.com/spf13/viper"
)

const (
	blocksize int64 = 1 << (10 * 2) //maximal size of a data block
)

var (
	uploadSlots chan int = make(chan int, 3) //gobal maximal number of concurrent uploads
)

func init() {
	//populate the upload slots
	for i := 1; i <= 3; i++ {
		uploadSlots <- 1
	}
}

type block struct {
	Offset int64
	Size   int64
	Hash   [32]byte
}

func (b *block) toDict() Dict {
	var dict = make(Dict)
	dict["Hash"] = base58.Encode(b.Hash[:])
	dict["Offset"] = float64(b.Offset) //convert to float, as this happens when json serialized
	dict["Size"] = float64(b.Size)     //this way we achive always float64, also if not serialized
	return dict
}

type file struct {
	Blocks []block
	Hash   [32]byte
}

func (f *file) name() string {
	return base58.Encode(f.Hash[:])
}

func (f *file) calculateHash() [32]byte {

	data, err := json.Marshal(f.Blocks)
	if err != nil {
		var data [32]byte
		return data
	}
	return sha256.Sum256(data)
}

func (f *file) toDict() Dict {
	var dict = make(Dict)
	dict["Hash"] = f.name()
	blocks := make([]Dict, len(f.Blocks))
	for i, b := range f.Blocks {
		blocks[i] = b.toDict()
	}
	dict["Blocks"] = blocks
	return dict
}

type directory struct {
	Files       []file
	Directories []directory
	Hash        [32]byte
}

func (self *directory) name() string {
	return base58.Encode(self.Hash[:])
}

func (self *directory) calculateHash() [32]byte {

	//data, err := json.Marshal(self.Blocks)
	//if err != nil {
	var data [32]byte
	return data
	//}
	//return sha256.Sum256(data)
}

func (self *directory) toDict() Dict {
	var dict = make(Dict)
	dict["Hash"] = self.name()
	dirs := make([]Dict, len(self.Directories))
	for i, dir := range self.Directories {
		dirs[i] = dir.toDict()
	}
	dict["Directories"] = dirs

	files := make([]Dict, len(self.Files))
	for i, file := range self.Files {
		files[i] = file.toDict()
	}
	dict["Files"] = files

	return dict
}

func blockFromDict(val map[string]interface{}) (block, error) {

	hashSlice, ok := val["Hash"]
	if !ok {
		return block{}, fmt.Errorf("No Hash available")
	}
	data, err := base58.Decode(hashSlice.(string))
	if err != nil {
		return block{}, fmt.Errorf("Hash not correctly encoded")
	}
	var hash [32]byte
	copy(hash[:], data)

	_, ok = val["Offset"]
	if !ok {
		return block{}, fmt.Errorf("No offset available")
	}
	offset := int64(val["Offset"].(float64))

	_, ok = val["Size"]
	if !ok {
		return block{}, fmt.Errorf("No size available")
	}
	size := int64(val["Size"].(float64))

	return block{offset, size, hash}, nil
}

func blockFromInterface(data interface{}) (block, error) {

	dict, ok := data.(map[string]interface{})
	if !ok {
		return block{}, fmt.Errorf("Interface is not a Dict!")
	}
	return blockFromDict(Dict(dict))
}

func fileFromDict(val map[string]interface{}) (file, error) {

	hashSlice, ok := val["Hash"]
	if !ok {
		return file{}, fmt.Errorf("No hash available")
	}
	data, err := base58.Decode(hashSlice.(string))
	if err != nil {
		return file{}, fmt.Errorf("Wrongly encoded hash")
	}
	var hash [32]byte
	copy(hash[:], data)

	vals, ok := val["Blocks"]
	if !ok {
		return file{}, fmt.Errorf("No blocks available in file")
	}
	blocks := make([]block, len(vals.([]interface{})))
	for i, data := range vals.([]interface{}) {
		blocks[i], err = blockFromInterface(data)
		if err != nil {
			return file{}, err
		}
	}

	return file{blocks, hash}, nil
}

func fileFromInterface(data interface{}) (file, error) {

	dict, ok := data.(map[string]interface{})
	if !ok {
		return file{}, fmt.Errorf("interface is not Dict")
	}
	return fileFromDict(Dict(dict))
}

func fileHashFromName(name string) [32]byte {
	data, err := base58.Decode(name)
	if err != nil {
		panic("Wrongly encoded hash")
	}
	var hash [32]byte
	copy(hash[:], data)
	return hash
}

/* Data handling
 *************  */

func (s *Swarm) DistributeData(data []byte) string {

	return ""
}

func (s *Swarm) DistributeFile(path string) (string, error) {

	//add the file to our local store
	file, err := s.addLocalFile(path)
	if err != nil {
		return "", err
	}

	//and make everyone aware that it shall be distributed
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	//lets call all our peers!
	for _, data := range s.peers {
		if data.Event.Connected() {
			go func() {
				err := data.Event.WriteMsg(Event{"ocp.swarm.file", Dict{"file": file.toDict()}, List{}}, false)
				if err != nil {
					log.Printf("Error writing Event: %s", err)
				}
			}()
		}
	}

	return file.name(), nil
}

func (s *Swarm) DropDataOrFile(path string) string {
	return ""
}

//datahandling uses events, hence we need to setup a few callbacks first
func (s *Swarm) setupDataHandling() {

	//all new files need to reach our channel
	s.RegisterEventChannel("ocp.swarm.file", s.newFiles)

	//new files need to be added and prepared for fetching
	ctx, _ := context.WithCancel(s.ctx)
	go func() {
		for {
			select {
			case dict, ok := <-s.newFiles:
				if !ok {
					return
				}
				inter, ok := dict["file"]
				if !ok {
					log.Printf("Bad new file event received")
					continue
				}
				f, err := fileFromInterface(inter)
				if err != nil {
					log.Printf("Bad new file event received: %s", err)
					continue
				}
				log.Printf("New file shared: %s", f.name())
				if s.hasFile(f.Hash) {
					continue
				}
				//we don't have this file yet. Hence lets add it and fetch all relevant data
				s.addFile(f)
				s.fetchMissingBlocks(f.Hash)

			case <-ctx.Done():
				return
			}
		}
	}()

	//a limited amount of fetchers need to collect the blocks
	for i := 0; i < 4; i++ {
		ctx, _ := context.WithCancel(s.ctx)
		go func() {
			for {
				select {
				case request, ok := <-s.fetchBlock:
					if !ok {
						return
					}

					for {
						rqstBlock, err := blockFromDict(request.Block)
						if err != nil {
							log.Printf("Invalid request, ignoring: %s", err)
							continue
						}

						//iterate over all peers and see if one has the block
						//we copy the peer list over to not block the list forever
						//note: if no peers available retry later!
						peers := make([]peerConnection, 0)
						for len(peers) == 0 {
							s.peerLock.RLock()
							for _, pc := range s.peers {
								peers = append(peers, pc)
							}
							s.peerLock.RUnlock()
							if len(peers) == 0 {
								time.Sleep(1 * time.Second)
							}
						}

						//lets get the block!
						fetched := false
						fullQueue := 0
						for _, pc := range peers {

							//non-blocking request
							fmt.Println("Send request")
							ret, err := pc.Data.SendRequest(request, true)
							if err != nil {
								fmt.Printf("Error sending request: %s\n", err)
								continue
							}
							fmt.Println("Request returned")
							//seems we made the request
							switch ret.MessageType() {

							case ERROR:
								//block not evailable, no upload capa...could be any kind of reason
								if ret.(*Error).Reason == "Full Queue" {
									fullQueue++
								}
								fmt.Printf("Error received: %s\n", ret.(*Error).Reason)
								continue

							case BLOCKDATA:
								//we received the block. yeah
								fmt.Println("Block returned")
								msg := ret.(*BlockData)
								msgBlock, err := blockFromDict(msg.Block)
								if err != nil {
									log.Printf("Invalid block returned, ignoring: %s", err)
									continue
								}
								if msg.File != request.File || msgBlock != rqstBlock {
									log.Printf("Wrong block received, ignoring")
									continue
								}
								data, err := base64.StdEncoding.DecodeString(msg.Data)
								if err != nil {
									log.Printf("Invalid encoded Binary Data")
									continue
								}
								fmt.Println("Block is fine, write to file")
								s.writeBlock(fileHashFromName(msg.File), msgBlock, data)
								fetched = true
								break
							}
						}
						if fetched {
							break
						}

						//if we are here the block was not fetched
						if fullQueue == len(peers) {
							//all peers have the file but have a full queue, no reason to
							//try annother block, just wait till someone is available!
							time.Sleep(100 * time.Millisecond)
						} else {
							go func() { s.fetchBlock <- request }()
							break
						}
					}

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	//TODO: we should iterate over all files in our store and collect the missing blocks for fetching

}

//this function handles events. Note that this stream not only has Event messages,
//but is also used for some other internal messages, e.g. for data distribution
func (s *Swarm) handleDataStream(pid PeerID, messenger streamMessenger) {

	go func() {
		for {
			msg, err := messenger.ReadMsg(false)

			if err != nil {
				log.Printf("Error reading message: %s", err.Error())
				return
			}

			//verify the requester of the data and see if he is allowed to receive data
			//as data is only sent to one peer, not by flooding, signature is not required

			switch msg.MessageType() {

			case REQUESTBLOCK:

				rqst := msg.(*RequestBlock)
				rqstBlock, err := blockFromDict(rqst.Block)
				if err != nil {
					messenger.WriteMsg(Error{"Request wrongly formatted"}, false)
					continue
				}

				//check if we have the block
				if !s.hasBlock(fileHashFromName(rqst.File), rqstBlock) {
					messenger.WriteMsg(Error{"Block not available"}, false)
					continue
				}

				//check if we are alowed to upload (after checking if we have the
				//block, so that the peer who asked knows if we have it or not, even
				//if not provided
				select {
				case <-uploadSlots:
					//we have the slot, go on!
				default:
					messenger.WriteMsg(Error{"Full Queue"}, false)
					continue
				}

				//return the block
				data, err := s.getBlock(fileHashFromName(rqst.File), rqstBlock)
				if err != nil {
					messenger.WriteMsg(Error{"Block not available"}, false)
					uploadSlots <- 1
					continue
				}

				bdata := base64.StdEncoding.EncodeToString(data)
				if err != nil {
					messenger.Close()
					uploadSlots <- 1
					return
				}
				err = messenger.WriteMsg(BlockData{File: rqst.File, Block: rqst.Block, Data: bdata}, false)
				if err != nil {
					messenger.Close()
					uploadSlots <- 1
					return
				}

				uploadSlots <- 1

			default:
				messenger.WriteMsg(Error{"Message type not supportet"}, false)
				messenger.Close()
				return
			}
		}
	}()
}

func (s *Swarm) fetchMissingBlocks(file [32]byte) {

	//put all missing blocks into a channel
	ctx, _ := context.WithCancel(s.ctx)
	go func(ctx context.Context) {
		//collect all missing blocks
		blocks := make([]block, 0)
		s.fileStore.View(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(file[:]).Bucket([]byte("fetching"))
			cursor := bucket.Cursor()
			for _, data := cursor.First(); data != nil; _, data = cursor.Next() {
				b := block{}
				json.Unmarshal(data, &b)
				blocks = append(blocks, b)
			}
			return nil
		})

		//put them in! we don't want to block the database, hence we have a extra
		//loop to put the blocks into the channel
		for _, block := range blocks {
			select {
			case s.fetchBlock <- RequestBlock{Block: block.toDict(), File: base58.Encode(file[:])}:
				//do nothing
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
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
	name := f.name()
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

		exist = true
		return nil
	})
	if err != nil {
		log.Printf("Has block: %s\n", err)
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

	if s.hasBlock(fileHash, b) {
		return nil
	}

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

	if err != nil {
		return err
	}

	//consistency check
	if b.Size != storedBlock.Size || b.Offset != storedBlock.Offset {
		return fmt.Errorf("Block not consistent with stored information")
	}

	//store block
	dir := viper.GetString("directory")
	path = filepath.Join(dir, "files", path)
	err = putBlock(path, b, data)
	if err != nil {
		return err
	}

	//write the info about receiving into the storage
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

//adds a local file to the local storage as already fetched
func (s *Swarm) addLocalFile(path string) (file, error) {

	//build the blocks!
	file, err := blockifyFile(path)
	if err != nil {
		return file, err
	}

	//check if it exists already...
	err = s.addFile(file)
	if err != nil {
		return file, err
	}

	//we copy everything over
	fi, err := os.Open(path)
	if err != nil {
		return file, err
	}
	defer fi.Close()

	outbuf := make([]byte, blocksize)
	for _, block := range file.Blocks {

		_, err = fi.Seek(block.Offset, io.SeekStart)
		if err != nil {
			return file, err
		}

		buf := outbuf[:block.Size]
		_, err := io.ReadFull(fi, buf)
		if err != nil {
			return file, err
		}

		err = s.writeBlock(file.Hash, block, buf)
		if err != nil {
			return file, err
		}
	}

	return file, nil
}

//adds a blockified file to the local storage as unfetched
func (s *Swarm) addFile(f file) error {

	//check if it exists already...
	if s.hasFile(f.Hash) {
		return fmt.Errorf("File already exists as %s", f.name())
	}
	//create the data in our store
	return s.setupFile(f)
}

func (s *Swarm) Files() []string {

	//we iterate over the database and build all file objects
	var files []string
	s.fileStore.View(func(tx *bolt.Tx) error {

		fcursor := tx.Cursor()
		for fhash, _ := fcursor.First(); fhash != nil; fhash, _ = fcursor.Next() {

			name := base58.Encode(fhash)
			files = append(files, name)
		}

		return nil
	})

	return files
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
		return err
	}
	defer fi.Close()

	n, err := fi.WriteAt(data, info.Offset)
	if err != nil {
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
