package p2p

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
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

func init() {
	gob.Register(P2PDirectoryBlock{})
	gob.Register(P2PFileBlock{})
	gob.Register(P2PMultiFileBlock{})
	gob.Register(P2PRawBlock{})
}

/******************************************************************************
							Block implementations
******************************************************************************/

//Basic block with all information needed for processing: Owner for authentication purposes
//as well as type for processing.
//Implements the ipfs Block interface to be used with bitswap.
type P2PDataBlock interface {
	Type() BlockType
	ToData() []byte
	ToBlock() blocks.Block
}

//Block with naming information attached to it
type P2PNamedBlock interface {
	P2PDataBlock
	FileName() string
}

//A block describing a raw data part
type P2PRawBlock struct {
	Offset int64
	Data   []byte
}

func (self P2PRawBlock) Type() BlockType {
	return BlockRaw
}

func (self P2PRawBlock) ToData() []byte {

	buf := new(bytes.Buffer)
	var inter P2PDataBlock = self
	gob.NewEncoder(buf).Encode(&inter)

	return buf.Bytes()
}

func (self P2PRawBlock) ToBlock() blocks.Block {

	return blocks.NewBlock(self.ToData())
}

//A block describing a file small enough for a single block
type P2PFileBlock struct {
	Name string
	Data []byte
}

func (self P2PFileBlock) Type() BlockType {
	return BlockFile
}

func (self P2PFileBlock) ToData() []byte {

	buf := new(bytes.Buffer)
	var inter P2PDataBlock = self
	gob.NewEncoder(buf).Encode(&inter)

	return buf.Bytes()
}

func (self P2PFileBlock) ToBlock() blocks.Block {

	return blocks.NewBlock(self.ToData())
}

func (self P2PFileBlock) FileName() string {

	return self.Name
}

//a block descibing a file which is too large for a single block
type P2PMultiFileBlock struct {
	Size   int64
	Name   string
	Blocks []cid.Cid
}

func (self P2PMultiFileBlock) Type() BlockType {
	return BlockMultiFile
}

func (self P2PMultiFileBlock) ToData() []byte {

	buf := new(bytes.Buffer)
	var inter P2PDataBlock = self
	gob.NewEncoder(buf).Encode(&inter)

	return buf.Bytes()
}

func (self P2PMultiFileBlock) ToBlock() blocks.Block {

	return blocks.NewBlock(self.ToData())
}

func (self P2PMultiFileBlock) Sort() {
	sort.Slice(self.Blocks, func(i, j int) bool { return self.Blocks[i].String() < self.Blocks[j].String() })
}

func (self P2PMultiFileBlock) FileName() string {

	return self.Name
}

//a block descibing a directory
type P2PDirectoryBlock struct {
	Name   string
	Blocks []cid.Cid
}

func (self P2PDirectoryBlock) Type() BlockType {
	return BlockDirectory
}

func (self P2PDirectoryBlock) ToData() []byte {

	buf := new(bytes.Buffer)
	var inter P2PDataBlock = self
	gob.NewEncoder(buf).Encode(&inter)

	return buf.Bytes()
}

func (self P2PDirectoryBlock) ToBlock() blocks.Block {

	return blocks.NewBlock(self.ToData())
}

func (self P2PDirectoryBlock) Sort() {
	sort.Slice(self.Blocks, func(i, j int) bool { return self.Blocks[i].String() < self.Blocks[j].String() })
}

func (self P2PDirectoryBlock) FileName() string {

	return self.Name
}

func getP2PBlock(block blocks.Block) (P2PDataBlock, error) {

	//check if it is a basic block that can be converted from rawdata
	buf := bytes.NewBuffer(block.RawData())
	var res P2PDataBlock
	err := gob.NewDecoder(buf).Decode(&res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

/******************************************************************************
							Block creation
******************************************************************************/

//generates a p2p file descriptor from a real file.
// - This does split the files into blocks
// - same file returns exactly the same blocks, undependend of path
// Returns all created blocks as well as the high level cid used to adress this file
func blockifyFile(path string) ([]blocks.Block, cid.Cid, error) {

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

		block := P2PFileBlock{name, data[:n]}
		result = append(result, block.ToBlock())

		return result, result[0].Cid(), nil
	}

	result = make([]blocks.Block, blocknum+1)
	rawblocks := make([]cid.Cid, blocknum)
	for i := 0; i < blocknum; i++ {

		n, err := fi.Read(data)
		if err != nil && err != io.EOF {
			return make([]blocks.Block, 0), cid.Cid{}, err
		}

		//the last block can be smaller than blocksize, hence always use n
		block := P2PRawBlock{int64(i) * blocksize, data[:n]}

		//store the blocks
		result[i+1] = block.ToBlock()
		rawblocks[i] = result[i+1].Cid()
	}

	//build the fileblock:
	block := P2PMultiFileBlock{size, name, rawblocks}
	block.Sort()
	result[0] = block.ToBlock()

	return result, result[0].Cid(), nil
}
