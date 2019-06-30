package p2p

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	bs "github.com/ipfs/go-bitswap"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
)

type DataService interface {
	Add(path string) (cid.Cid, error)                                   //adds a file or directory
	Drop(id cid.Cid) error                                              //removes a file or directory
	Fetch(ctx context.Context, id cid.Cid) error                        //Fetches the given data
	FetchAsync(id cid.Cid) error                                        //Fetches the given data async
	GetFile(ctx context.Context, id cid.Cid) (*os.File, error)          //gets the file described by the id (fetches if needed)
	Write(ctx context.Context, id cid.Cid, path string) (string, error) //writes the file or directory to the given path (fetches if needed)
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

	return &hostDataService{path, bitswap, bstore}, nil
}

type hostDataService struct {
	datapath string
	bitswap  *bs.Bitswap
	store    BitswapStore
}

func (self *hostDataService) Add(path string) (cid.Cid, error) {

	filecid, _, err := self.basicAdd(path, "global")
	if err != nil {
		return cid.Cid{}, utils.StackError(err, "Unable to add path")
	}

	//return
	return filecid, err
}

func (self *hostDataService) Drop(id cid.Cid) error {

	err := self.basicDrop(id, "global")
	if err != nil {
		return utils.StackError(err, "Unable to drop id")
	}

	return nil
}

func (self *hostDataService) Fetch(ctx context.Context, id cid.Cid) error {

	err := self.basicFetch(ctx, id, self.bitswap, "global")
	if err != nil {
		return utils.StackError(err, "Unable to fetch id %v", id.String())
	}

	return nil
}

func (self *hostDataService) FetchAsync(id cid.Cid) error {

	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Hour)
		self.basicFetch(ctx, id, self.bitswap, "global")
	}()
	return nil
}

func (self *hostDataService) GetFile(ctx context.Context, id cid.Cid) (*os.File, error) {

	_, file, err := self.basicGetFile(ctx, id, self.bitswap, "global")
	if err != nil {
		return nil, utils.StackError(err, "Unable to get file %v", id.String())
	}

	return file, err
}

func (self *hostDataService) Write(ctx context.Context, id cid.Cid, path string) (string, error) {

	//make sure we have the data!
	return self.basicWrite(ctx, id, path, self.bitswap, "global")
}

func (self *hostDataService) Close() {
	self.bitswap.Close()
	self.store.Close()
}

/*************************** Internal Functions ******************************/

func (self *hostDataService) basicAddFile(path string) (cid.Cid, []blocks.Block, error) {

	//we first blockify the file bevore moving it into our store.
	//This is done to know the cid after which we name the file in the store
	//to avoid having problems with same filenames for different files
	blocks, filecid, err := blockifyFile(path)
	if err != nil {
		return filecid, nil, utils.StackError(err, "Unable to blockify file")
	}

	//make the blocks available in the exchange! (no need to add to store before
	//as bitswap does this anyway)
	for _, block := range blocks {
		if has, _ := self.store.Has(block.Cid()); !has {
			err = self.bitswap.HasBlock(block)
			if err != nil {
				self.Drop(filecid)
				return cid.Undef, nil, utils.StackError(err, "Unable to add file")
			}
		}
	}

	return filecid, blocks, err
}

func (self *hostDataService) basicAddDirectory(path string) (cid.Cid, []blocks.Block, error) {

	//the result
	result := make([]blocks.Block, 0)
	subcids := make([]cid.Cid, 0)

	//we iterate through all files and subdirs in the directory
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return cid.Undef, nil, utils.StackError(err, "Unable to add directory")
	}
	for _, file := range files {
		newpath := filepath.Join(path, file.Name())
		subcid, blocks, err := self.basicAdd(newpath, "") //no owner needed as parent has set ownership!
		if err != nil {
			return cid.Undef, nil, utils.StackError(err, "Unable to add directory")
		}
		result = append(result, blocks...) //collect all blocks
		subcids = append(subcids, subcid)  //collect all directory entry cids
	}

	//build the directory block
	name := filepath.Base(path)
	dirblock := P2PDirectoryBlock{name, subcids}
	dirblock.Sort()
	block := dirblock.ToBlock()

	//Store the block
	err = self.store.Put(block)
	if err != nil {
		return cid.Undef, nil, utils.StackError(err, "Unable to add directory")
	}

	//and the final list of all blocks with the prepended directory
	result = append([]blocks.Block{block}, result...)

	return block.Cid(), result, nil
}

func (self *hostDataService) basicAdd(path string, owner string) (cid.Cid, []blocks.Block, error) {

	//check if we have a directory or a file
	info, err := os.Stat(path)
	if err != nil {
		return cid.Undef, nil, utils.StackError(err, "Unable to add path")
	}

	var resCid cid.Cid
	var resBlocks []blocks.Block

	switch mode := info.Mode(); {
	case mode.IsDir():
		resCid, resBlocks, err = self.basicAddDirectory(path)
	case mode.IsRegular():
		resCid, resBlocks, err = self.basicAddFile(path)
	}

	if err != nil {
		return resCid, nil, utils.StackError(err, "Unable to add path")
	}

	//set ownership
	if owner != "" {
		err = self.store.SetOwnership(resBlocks[0], owner)
	}

	return resCid, resBlocks, err
}

//we start requesting a cid async, e.g. non blocking
func (self *hostDataService) basicFetch(ctx context.Context, id cid.Cid, fetcher exchange.Fetcher, owner string) error {

	//get the root block
	block, err := self.store.Get(id)
	if err != nil {
		var err error
		block, err = fetcher.GetBlock(ctx, id)
		if err != nil {
			return utils.StackError(err, "Unable to get root node")
		}
	}

	if block.Cid() != id {
		return fmt.Errorf("Received wrong block from exchange service")
	}

	//check if we need additional blocks (e.g. it's a multifile)
	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return utils.StackError(err, "Node is not expected type")
	}

	switch p2pblock.Type() {
	case BlockRaw:
		//we are done

	case BlockDirectory:
		//we go over all sub-cids and request them!
		dirblock := p2pblock.(P2PDirectoryBlock)
		needed, err := self.store.DoesNotHave(dirblock.Blocks)
		if err != nil {
			return err
		}
		//as we do not know which kind of block this cid is we use the normal request
		for _, sub := range needed {
			err := self.basicFetch(ctx, sub, fetcher, owner)
			if err != nil {
				return utils.StackError(err, "Unable to fetch id %v", id.String())
			}
		}

	case BlockFile:
		//we are done

	case BlockMultiFile:
		//get the rest of the needed blocks
		mfileblock := p2pblock.(P2PMultiFileBlock)
		needed, err := self.store.DoesNotHave(mfileblock.Blocks)
		if err != nil {
			return utils.StackError(err, "Unable to fetch id %v", id.String())
		}

		num := len(needed)
		channel, err := fetcher.GetBlocks(ctx, needed)
		if err != nil {
			return utils.StackError(err, "Unable to fetch id %v", id.String())
		}
		for range channel {
			num--
		}

		if num != 0 {
			return fmt.Errorf("Not all blocks could be fetched")
		}

	default:
		return fmt.Errorf("Unknown block type")
	}

	//set ownership
	err = self.store.SetOwnership(block, owner)
	if err != nil {
		utils.StackError(err, "Unable to fetch id %v", id.String())
	}

	return nil
}

func (self *hostDataService) basicGetFile(ctx context.Context, id cid.Cid, fetcher exchange.Fetcher, owner string) (blocks.Block, *os.File, error) {

	//make sure we have all blocks
	if err := self.basicFetch(ctx, id, fetcher, owner); err != nil {
		return nil, nil, utils.StackError(err, "Unable to get file %v", id.String())
	}

	//get the root
	block, err := self.store.Get(id)
	if err != nil {
		return nil, nil, utils.StackError(err, "Unable to get file %v", id.String())
	}

	//see what we need to return
	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return nil, nil, utils.StackError(err, "Node is not expected type")
	}

	switch p2pblock.Type() {
	case BlockRaw, BlockDirectory:
		return nil, nil, fmt.Errorf("cid does not belong to a file")

	case BlockFile, BlockMultiFile:
		path := filepath.Join(self.datapath, block.Cid().String())
		file, err := os.Open(path)
		return block, file, utils.StackError(err, "Unable to get file %v", id.String())

	default:
		return nil, nil, fmt.Errorf("Unknown block type")
	}

	return nil, nil, fmt.Errorf("Something got wrong")
}

func (self *hostDataService) basicWrite(ctx context.Context, id cid.Cid, path string, fetcher exchange.Fetcher, owner string) (string, error) {

	//make sure we have the data!
	if err := self.basicFetch(ctx, id, fetcher, owner); err != nil {
		return "", utils.StackError(err, "Unable to get write %v", id.String())
	}

	//get the root node
	block, err := self.store.Get(id)
	if err != nil {
		return "", utils.StackError(err, "Unable to get write %v", id.String())
	}

	//see what we need to do!
	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return "", utils.StackError(err, "Node is not expected type: Must be P2PDataBlock")
	}

	switch p2pblock.Type() {
	case BlockRaw:
		return "", fmt.Errorf("Cid does not belong to a raw block, which cannot be written")

	case BlockDirectory:
		//make sure we have the directory
		dirblock := p2pblock.(P2PDirectoryBlock)
		newpath := filepath.Join(path, dirblock.Name)
		os.MkdirAll(newpath, os.ModePerm)
		//we write every subfile to the directory!
		for _, sub := range dirblock.Blocks {
			self.basicWrite(ctx, sub, newpath, fetcher, owner)
		}

		return newpath, nil

	case BlockFile, BlockMultiFile:

		fpath := filepath.Join(self.datapath, block.Cid().String())
		file, err := os.Open(fpath)
		if err != nil {
			return "", utils.StackError(err, "Unable to get write %v", id.String())
		}
		defer file.Close()

		namedblock := p2pblock.(P2PNamedBlock)
		newpath := filepath.Join(path, namedblock.FileName())
		destination, err := os.Create(newpath)
		if err != nil {
			return "", utils.StackError(err, "Unable to get write %v", id.String())
		}
		defer destination.Close()
		io.Copy(destination, file)

		return newpath, nil

	default:
		return "", fmt.Errorf("Unknown block type")
	}

	return "", fmt.Errorf("Shoudn't be here...")
}

func (self *hostDataService) basicDrop(id cid.Cid, owner string) error {

	//get the root block
	block, err := self.store.Get(id)
	if err != nil {
		return utils.StackError(err, "Unable to drop %v", id.String())
	}
	//release ownership
	last, err := self.store.ReleaseOwnership(block, owner)
	if err != nil {
		return utils.StackError(err, "Unable to drop %v", id.String())
	}

	//there maybe someone else that needs this file, than we are done!
	if !last {
		return nil
	}

	//get all blocks to be deleted
	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return utils.StackError(err, "Unable to drop %v", id.String())
	}

	switch p2pblock.Type() {
	case BlockRaw:
		return fmt.Errorf("cid does not belong to a file or directory: cannot be dropped")

	case BlockDirectory, BlockFile, BlockMultiFile:
		//we need to do a garbage collect, as it may be that subfiles need deletion too
		files, err := self.store.GarbageCollect()
		if err != nil {
			return utils.StackError(err, "Unable to drop %v", id.String())
		}
		self.store.DeleteBlocks(files)

	default:
		return fmt.Errorf("Unknown block type")
	}

	return nil
}

/******************************************************************************
							SwarmDataService
*******************************************************************************/
/*
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
		self.service.Drop(context.Background(), cmd.file)

	} else {

		self.files = append(self.files, cmd.file)
		//this could take a while... let's do it async
		self.service.FetchAsync(cmd.file)
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
		//release ownership
		if block, err := self.service.data.store.Get(file); err != nil {

			_, err = self.service.data.store.ReleaseOwnership(block, string(self.service.id))
			if err != nil {
				return err
			}
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
}*/

type swarmDataService struct {
	data    *hostDataService
	state   *sharedStateService
	session exchange.Fetcher
	id      SwarmID
	ctx     context.Context
	cancel  context.CancelFunc
}

func newSwarmDataService(swarm *Swarm) DataService {

	ctx, cncl := context.WithCancel(context.Background())

	data := swarm.host.Data.(*hostDataService)
	state := swarm.State
	session := data.bitswap.NewSession(ctx)
	id := swarm.ID

	return &swarmDataService{data, state, session, id, ctx, cncl}
}

func (self *swarmDataService) Add(path string) (cid.Cid, error) {

	filecid, _, err := self.data.basicAdd(path, string(self.id))
	if err != nil {
		return cid.Cid{}, err
	}

	//return
	return filecid, err
}

func (self *swarmDataService) Drop(id cid.Cid) error {

	//get the root block
	if block, err := self.data.store.Get(id); err == nil {
		//release ownership
		last, err := self.data.store.ReleaseOwnership(block, string(self.id))
		if err != nil {
			return err
		}

		//there maybe someone else that needs this file
		if last {
			return self.data.basicDrop(id, string(self.id))
		}
	}

	return nil
}

func (self *swarmDataService) Fetch(ctx context.Context, id cid.Cid) error {

	return self.data.basicFetch(ctx, id, self.session, string(self.id))
}

func (self *swarmDataService) FetchAsync(id cid.Cid) error {

	go func() {
		self.data.basicFetch(self.ctx, id, self.session, string(self.id))
	}()

	return nil
}

func (self *swarmDataService) GetFile(ctx context.Context, id cid.Cid) (*os.File, error) {

	_, file, err := self.data.basicGetFile(ctx, id, self.session, string(self.id))
	if err != nil {
		return nil, err
	}

	return file, err
}

func (self *swarmDataService) Write(ctx context.Context, id cid.Cid, path string) (string, error) {

	//make sure we have the data!
	return self.data.basicWrite(ctx, id, path, self.session, string(self.id))
}

func (self *swarmDataService) Close() {

}
