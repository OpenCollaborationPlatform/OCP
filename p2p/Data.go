// Datahandling for swarm operations
package p2p

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
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

	fi, err := os.Open(file)
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

//swarm functions for handling blocks
//***********************************

func (s *Swarm) hasBlock(file [32]byte, b block) {

}

func (s *Swarm) getBlock(file [32]byte, b block) {

}

func (s *Swarm) writeBlock(file [32]byte, b block) {

}
