package p2p

import (
	"CollaborationNode/utils"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"

	blocks "github.com/ipfs/go-block-format"
	bserv "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
)

type DataService interface {
	AddFile(ctx context.Context, path string) (cid.Cid, error)
	GetFile(ctx context.Context, id cid.Cid) (*os.File, error)
	DropFile(ctx context.Context, id cid.Cid) error
	Close()
}

func NewDataService(host *Host) (DataService, error) {

	//check if we have the data dir, if not create it
	path := filepath.Join(host.path, "DataExchange")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}

	//build the blockstore
	bstore, err := NewBitswapStore(path)
	if err != nil {
		return nil, err
	}

	//build the blockservice from a blockstore and a bitswap
	bitswap, err := NewBitswap(bstore, host)
	if err != nil {
		return nil, err
	}
	blockservice := bserv.New(bstore, bitswap)

	return &hostDataService{path, blockservice, bstore}, nil
}

type hostDataService struct {
	datapath string
	service  bserv.BlockService
	store    BitswapStore
}

func (self *hostDataService) basicAddFile(ctx context.Context, path string) (cid.Cid, []blocks.Block, error) {

	//we first blockify the file bevore moving it into our store.
	//This is done to know the cid after which we name the file in the store
	//to avoid having problems with same filenames for different files
	blocks, filecid, err := blockifyFile(path)
	if err != nil {
		return filecid, nil, err
	}

	//make the blocks available
	err = self.service.AddBlocks(blocks)

	return filecid, blocks, err
}

func (self *hostDataService) AddFile(ctx context.Context, path string) (cid.Cid, error) {

	filecid, blocks, err := self.basicAddFile(ctx, path)
	if err != nil {
		return cid.Cid{}, err
	}

	//set ownership
	self.store.GetOwnership(blocks[0], "global")

	//return
	return filecid, err
}

func (self *hostDataService) basicGetFile(ctx context.Context, id cid.Cid) (blocks.Block, *os.File, error) {

	//get the root block
	block, err := self.service.GetBlock(ctx, id)
	if err != nil {
		return nil, nil, utils.StackError(err, "Unable to get root node")
	}
	if block.Cid() != id {
		return nil, nil, fmt.Errorf("Received wrong block from exchange service")
	}

	//check if we need additional blocks (e.g. it's a multifile)
	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return nil, nil, utils.StackError(err, "Node is not expected type")
	}

	switch p2pblock.Type() {
	case BlockRaw:
		return nil, nil, fmt.Errorf("cid does not belong to a file")

	case BlockDirectory:
		return nil, nil, fmt.Errorf("cid does not belong to a file")

	case BlockFile:
		//we are done.
		path := filepath.Join(self.datapath, block.Cid().String())
		file, err := os.Open(path)
		return block, file, err

	case BlockMultiFile:
		//get the rest of the blocks (TODO: Check if we have all blocks and simply
		//load the file)
		//test if we have all blocks
		mfileblock := p2pblock.(P2PMultiFileBlock)

		num := len(mfileblock.Blocks)
		channel := self.service.GetBlocks(ctx, mfileblock.Blocks)
		for range channel {
			num--
		}
		if num != 0 {
			return nil, nil, fmt.Errorf("File was only partially received: %v blocks missing", num)
		}
		path := filepath.Join(self.datapath, block.Cid().String())
		file, err := os.Open(path)
		return block, file, err

	default:
		return nil, nil, fmt.Errorf("Unknown block type")
	}

	return nil, nil, fmt.Errorf("something got wrong")
}

func (self *hostDataService) GetFile(ctx context.Context, id cid.Cid) (*os.File, error) {

	block, file, err := self.basicGetFile(ctx, id)
	if err != nil {
		return nil, err
	}

	//set ownership (maybe not done yet)
	err = self.store.GetOwnership(block, "global")

	return file, err
}

func (self *hostDataService) basicDropFile(block blocks.Block) error {

	//get all blocks to be deleted
	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return err
	}

	switch p2pblock.Type() {
	case BlockRaw:
		return fmt.Errorf("cid does not belong to a file")

	case BlockDirectory:
		return fmt.Errorf("cid does not belong to a file")

	case BlockFile:
		self.service.DeleteBlock(block.Cid())

	case BlockMultiFile:
		//get the rest of the blocks (TODO: Check if we have all blocks and simply
		//load the file)
		mfileblock := p2pblock.(P2PMultiFileBlock)
		for _, block := range mfileblock.Blocks {
			self.service.DeleteBlock(block)
		}

	default:
		return fmt.Errorf("Unknown block type")
	}

	return nil
}

func (self *hostDataService) DropFile(ctx context.Context, id cid.Cid) error {

	//get the root block
	block, err := self.service.GetBlock(ctx, id)
	if err != nil {
		return err
	}

	//release ownership
	last, err := self.store.ReleaseOwnership(block, "global")
	if err != nil {
		return err
	}

	//there maybe someone else that needs this file
	if last {
		return self.basicDropFile(block)
	}

	return nil
}

func (self *hostDataService) Close() {
	self.service.Close()
}

//SwarmDataService
//This dataservice behaves sligthly different than the normal one:
// - Adding/Dropping a file automatically distributes it within the swarm

type dataStateCommand struct {
	file   cid.Cid //the cid to add or remove from the list
	remove bool    //if true is removed from list, if false it is added
}

func (self dataStateCommand) toByte() ([]byte, error) {

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(self)
	return buf.Bytes(), err
}

func dataStateCommandFromByte(data []byte) (dataStateCommand, error) {

	cmd := dataStateCommand{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&cmd)
	return cmd, err
}

//a shared state that is a list of CIDs this swarm shares.
type dataState struct {
	files   []cid.Cid
	service *swarmDataService
	ctx     context.Context
	cancel  context.CancelFunc
}

func newDataState(service *swarmDataService) *dataState {

	ctx, cncl := context.WithCancel(context.Background())
	return &dataState{
		files:   make([]cid.Cid, 0),
		service: service,
		ctx:     ctx,
		cancel:  cncl,
	}
}

func (self *dataState) Apply(data []byte) error {

	cmd, err := dataStateCommandFromByte(data)
	if err != nil {
		return err
	}

	if cmd.remove {

		for i, val := range self.files {
			if val == cmd.file {
				self.files = append(self.files[:i], self.files[i+1:]...)
				break
			}
		}
		self.service.DropFile(context.Background(), cmd.file)

	} else {

		self.files = append(self.files, cmd.file)
		//this could take a while... let's do it in a goroutine!
		go func() {
			file, err := self.service.GetFile(self.ctx, cmd.file)
			if err != nil && file != nil {
				file.Close()
			}
		}()
	}

	return nil
}

func (self *dataState) Reset() error {

	//drop all files we currently have
	for _, file := range self.files {
		err := self.service.DropFile(context.Background(), file)
		if err != nil {
			return err
		}
	}

	self.files = make([]cid.Cid, 0)
	return nil
}

func (self *dataState) Snapshot() ([]byte, error) {

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(&self.files)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (self *dataState) EnsureSnapshot(snap []byte) error {

	buf := bytes.NewBuffer(snap)
	var list []cid.Cid
	err := gob.NewEncoder(buf).Encode(&list)
	if err != nil {
		return err
	}

	//compare lists
	if len(list) != len(self.files) {
		return fmt.Errorf("Snapshot does not match current state")
	}

	for i, file := range list {
		if self.files[i] != file {
			return fmt.Errorf("Snapshot does not match current state")
		}
	}
	return nil
}

func (self *dataState) LoadSnapshot(snap []byte) error {

	//we make it simple: remove all, add the new ones and than garbage collect
	buf := bytes.NewBuffer(snap)
	var list []cid.Cid
	err := gob.NewEncoder(buf).Encode(&list)
	if err != nil {
		return err
	}

	//drop ownership of all old files
	for _, file := range self.files {
		//get the root block
		block, err := self.service.data.service.GetBlock(self.ctx, file)
		if err != nil {
			return err
		}
		//release ownership
		_, err = self.service.data.store.ReleaseOwnership(block, string(self.service.id))
		if err != nil {
			return err
		}
	}

	//grab ownership of new files
	for _, file := range list {
		go func(file cid.Cid) {
			f, err := self.service.GetFile(self.ctx, file)
			if err != nil && f != nil {
				f.Close()
			}
		}(file)
	}
	self.files = list
	return nil
}

type swarmDataService struct {
	data  *hostDataService
	state *sharedStateService
	id    SwarmID
}

func newSwarmDataService(swarm *Swarm) DataService {

	hostdata := swarm.host.Data.(*hostDataService)
	return &swarmDataService{hostdata, swarm.State, swarm.ID}
}

func (self *swarmDataService) AddFile(ctx context.Context, path string) (cid.Cid, error) {

	filecid, blocks, err := self.data.basicAddFile(ctx, path)
	if err != nil {
		return cid.Cid{}, err
	}

	//set ownership
	self.data.store.GetOwnership(blocks[0], string(self.id))

	//return
	return filecid, err
}

func (self *swarmDataService) GetFile(ctx context.Context, id cid.Cid) (*os.File, error) {

	block, file, err := self.data.basicGetFile(ctx, id)
	if err != nil {
		return nil, err
	}

	//set ownership (maybe not done yet)
	err = self.data.store.GetOwnership(block, string(self.id))

	return file, err
}

func (self *swarmDataService) DropFile(ctx context.Context, id cid.Cid) error {

	//get the root block
	block, err := self.data.service.GetBlock(ctx, id)
	if err != nil {
		return err
	}

	//release ownership
	last, err := self.data.store.ReleaseOwnership(block, string(self.id))
	if err != nil {
		return err
	}

	//check if we can delete
	if last {
		return self.data.basicDropFile(block)
	}

	return nil
}

func (self *swarmDataService) Close() {

}
