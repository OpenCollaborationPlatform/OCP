package p2p

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	utils "github.com/ipfs/go-ipfs-util"
)

//A type describing the block types
type BlockType uint

const (
	BlockRaw       = BlockType(0)
	BlockFile      = BlockType(1)
	BlockMultiFile = BlockType(2)
	BlockDirectory = BlockType(3)

	blocksize int64 = 1 << (10 * 2) //maximal size of a data block (1MB)
)

/******************************************************************************
							Block implementations
******************************************************************************/

//Basic block with all information needed for processing: Owner for authentication purposes
//as well as type for processing.
//Implements the ipfs Block interface to be used with bitswap.
type P2PDataBlock interface {
	blocks.Block

	Type() BlockType
	Owner() SwarmID
}

//A block describing a raw data part
type P2PRawBlock struct {
	Offset     int64
	Data       []byte
	BlockOwner SwarmID
	BlockCid   cid.Cid
}

func (self P2PRawBlock) Type() BlockType {
	return BlockRaw
}

func (self P2PRawBlock) Owner() SwarmID {
	return self.BlockOwner
}

func (self P2PRawBlock) RawData() []byte {
	oldcid := self.BlockCid
	self.BlockCid = cid.Cid{}

	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(self)

	self.BlockCid = oldcid
	return buf.Bytes()
}

func (self P2PRawBlock) Cid() cid.Cid {
	return self.BlockCid
}

func (self P2PRawBlock) String() string {
	return fmt.Sprintf("Raw block %v: %v bytes from %v on", self.BlockCid, len(self.Data), self.Offset)
}

func (self P2PRawBlock) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"Raw block": self.Cid().String(),
	}
}

//A block describing a file small enough for a single block
type P2PFileBlock struct {
	Name       string
	Data       []byte
	BlockOwner SwarmID
	BlockCid   cid.Cid
}

func (self P2PFileBlock) Type() BlockType {
	return BlockFile
}

func (self P2PFileBlock) Owner() SwarmID {
	return self.BlockOwner
}

func (self P2PFileBlock) RawData() []byte {
	oldcid := self.BlockCid
	self.BlockCid = cid.Cid{}

	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(self)

	self.BlockCid = oldcid
	return buf.Bytes()
}

func (self P2PFileBlock) Cid() cid.Cid {
	return self.BlockCid
}

func (self P2PFileBlock) String() string {
	return fmt.Sprintf("File block %v: %v, %v bytes", self.BlockCid, self.Name, len(self.Data))
}

func (self P2PFileBlock) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"File block": self.Cid().String(),
	}
}

//a block descibing a file which is too large for a single block
type P2PMultiFileBlock struct {
	Size       int64
	Name       string
	Blocks     []cid.Cid
	BlockOwner SwarmID
	BlockCid   cid.Cid
}

func (self P2PMultiFileBlock) Type() BlockType {
	return BlockFile
}

func (self P2PMultiFileBlock) Owner() SwarmID {
	return self.BlockOwner
}

func (self P2PMultiFileBlock) RawData() []byte {
	oldcid := self.BlockCid
	self.BlockCid = cid.Cid{}

	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(self)

	self.BlockCid = oldcid
	return buf.Bytes()
}

func (self P2PMultiFileBlock) Cid() cid.Cid {
	return self.BlockCid
}

func (self P2PMultiFileBlock) String() string {
	return fmt.Sprintf("MultiFile block %v: %v (%v bytes, %v blocks)", self.BlockCid, self.Name, self.Size, len(self.Blocks))
}

func (self P2PMultiFileBlock) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"MultiFile block": self.Cid().String(),
	}
}

//a block descibing a directory
type P2PDirectoryBlock struct {
	Name       string
	Blocks     []cid.Cid
	BlockOwner SwarmID
	BlockCid   cid.Cid
}

func (self P2PDirectoryBlock) Type() BlockType {
	return BlockDirectory
}

func (self P2PDirectoryBlock) Owner() SwarmID {
	return self.BlockOwner
}

func (self P2PDirectoryBlock) RawData() []byte {
	oldcid := self.BlockCid
	self.BlockCid = cid.Cid{}

	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(self)

	self.BlockCid = oldcid
	return buf.Bytes()
}

func (self P2PDirectoryBlock) Cid() cid.Cid {
	return self.BlockCid
}

func (self P2PDirectoryBlock) String() string {
	return fmt.Sprintf("Directory block %v: %v (%v blocks)", self.BlockCid, self.Name, len(self.Blocks))
}

func (self P2PDirectoryBlock) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"Directory block": self.Cid().String(),
	}
}

/******************************************************************************
							Block creation
******************************************************************************/

//generates a p2p file descriptor from a real file.
// - This does split the files into blocks
// - same file returns exactly the same blocks, undependend of path
// Returns all created blocks as well as the high level cid used to adress this file
func blockifyFile(path string, owner SwarmID) ([]blocks.Block, cid.Cid, error) {

	result := make([]blocks.Block, 0)

	fi, err := os.Open(path)
	if err != nil {
		return result, cid.Cid{}, err
	}
	defer fi.Close()

	info, err := fi.Stat()
	if err != nil {
		return result, cid.Cid{}, err
	}
	size := info.Size()
	if size == 0 {
		return result, cid.Cid{}, fmt.Errorf("File is empty, cannot blockify it")
	}

	name := filepath.Base(path)

	//we want 1Mb slices, lets see how much we need
	blocknum := int(math.Ceil(float64(size) / float64(blocksize)))

	data := make([]byte, blocksize)
	if blocknum == 1 {
		//we can put it in a single blockfile, no need for multifile block
		n, err := fi.Read(data)
		if err != nil && err != io.EOF {
			return result, cid.Cid{}, err
		}

		block := P2PFileBlock{name, data[:n], owner, cid.Cid{}}
		blockcid := cid.NewCidV1(cid.Raw, utils.Hash(block.RawData()))
		block.BlockCid = blockcid

		result = append(result, block)
		return result, blockcid, nil
	}

	result = make([]blocks.Block, blocknum+1)
	rawblocks := make([]cid.Cid, blocknum)
	for i := 0; i < blocknum; i++ {

		n, err := fi.Read(data)
		if err != nil && err != io.EOF {
			return make([]blocks.Block, 0), cid.Cid{}, err
		}

		//the last block can be smaller than blocksize, hence always use n
		block := P2PRawBlock{int64(i) * blocksize, data[:n], owner, cid.Cid{}}
		blockcid := cid.NewCidV1(cid.Raw, utils.Hash(block.RawData()))
		block.BlockCid = blockcid

		//store the blocks
		rawblocks[i] = blockcid
		result[i+1] = block
	}

	//build the fileblock:
	block := P2PMultiFileBlock{size, name, rawblocks, owner, cid.Cid{}}
	blockcid := cid.NewCidV1(cid.Raw, utils.Hash(block.RawData()))
	block.BlockCid = blockcid
	result[0] = block

	return result, blockcid, nil
}
