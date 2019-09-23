package p2p

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	bs "github.com/ipfs/go-bitswap"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
)

type DataService interface {
	Add(ctx context.Context, path string) (cid.Cid, error)              //adds a file or directory
	AddAsync(path string) (cid.Cid, error)                              //adds a file or directory, but returns when the local operation is done
	Drop(ctx context.Context, id cid.Cid) error                         //removes a file or directory
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
	
	//start the service
	service := &hostDataService{path, bitswap, bstore, time.NewTicker(20*time.Hour), host.dht}
	
	//handle announcement: once initially and than periodically
	service.announceAllGlobal()
	go func() {
        for {
            select {
            case _, more := <-service.ticker.C:
            		if !more {
					return
				}
                 service.announceAllGlobal()
            }
        }
    }()

	return service, nil
}

type hostDataService struct {
	datapath string
	bitswap  *bs.Bitswap
	store    BitswapStore
	ticker   *time.Ticker
	dht 		*kaddht.IpfsDHT
}

func (self *hostDataService) Add(ctx context.Context, path string) (cid.Cid, error) {

	filecid, _, err := self.basicAdd(path, "global")
	if err != nil {
		return cid.Cid{}, utils.StackError(err, "Unable to add path")
	}

	//return
	return filecid, err
}

func (self *hostDataService) AddAsync(path string) (cid.Cid, error) {

	//we don't do any network operation...
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Hour)
	return self.Add(ctx, path)
}

func (self *hostDataService) Drop(ctx context.Context, id cid.Cid) error {

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

func (self *hostDataService)  announceAllGlobal() {
	
	globals, _ := self.store.GetCidsForOwner("global")
	go func() {
		for {
			repeats := make([]cid.Cid, 0)
			for _, id := range globals{
				ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
				err := self.dht.Provide(ctx, id, true)
				if err != nil {
					repeats = append(repeats, id)
				}
			} 
			//check if done
			if len(repeats) == 0 {
				return
			}
			//repeat all we not have been able to announce
			globals = repeats
		}
	}()
}

func (self *hostDataService) Close() {

	self.ticker.Stop()
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

	//Make the block available
	err = self.bitswap.HasBlock(block)
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
		self.basicDrop(resCid, owner)
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
		//set expected owner to make sure bitswap knows where to look
		self.store.SetExpectedOwnership(id, owner)
		var err error
		block, err = fetcher.GetBlock(ctx, id)
		if err != nil {
			self.store.ClearExpectedOwner(id)
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
		//as this are subblocks we do not need to add the owner
		for _, sub := range needed {
			err := self.basicFetch(ctx, sub, fetcher, "")
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
	if owner != "" {
		err = self.store.SetOwnership(block, owner)
		if err != nil {
			utils.StackError(err, "Unable to set correct owner for id %v", id.String())
		}
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

		if err != nil {
			return nil, nil, utils.StackError(err, "Unable to get file %v", id.String())
		}
		return block, file, nil

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
			_, err := self.basicWrite(ctx, sub, newpath, fetcher, owner)
			if err != nil {
				return "", utils.StackError(err, "Cannot write directory entry %v", sub.String())
			}
		}

		return newpath, nil

	case BlockFile, BlockMultiFile:

		fpath := filepath.Join(self.datapath, block.Cid().String())
		file, err := os.Open(fpath)
		if err != nil {
			return "", utils.StackError(err, "Unable to write %v", id.String())
		}
		defer file.Close()

		namedblock := p2pblock.(P2PNamedBlock)
		newpath := filepath.Join(path, namedblock.FileName())
		destination, err := os.Create(newpath)
		if err != nil {
			return "", utils.StackError(err, "Unable to write %v", id.String())
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

//This dataservice behaves sligthly different than the normal one:
// - Adding/Dropping a file automatically distributes it within the swarm

type dataStateCommand struct {
	File   cid.Cid //the cid to add or remove from the list
	Remove bool    //if true is removed from list, if false it is added
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
	mutex   sync.RWMutex //mutex needed as we access the state from outside the sharedStateService
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

func (self *dataState) Apply(data []byte) interface{} {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	cmd, err := dataStateCommandFromByte(data)
	if err != nil {
		return err
	}

	if cmd.Remove {

		for i, val := range self.files {
			if val == cmd.File {
				self.files = append(self.files[:i], self.files[i+1:]...)
				break
			}
		}
		self.service.internalDrop(cmd.File)

	} else {

		self.files = append(self.files, cmd.File)
		//this could take a while... let's do it async
		self.service.internalFetchAsync(cmd.File)
	}

	return len(self.files)
}

func (self *dataState) Snapshot() ([]byte, error) {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(&self.files)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (self *dataState) LoadSnapshot(snap []byte) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

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
		self.service.internalFetchAsync(file)
	}
	self.files = list

	//garbage collect
	files, err := self.service.data.store.GarbageCollect()
	if err != nil {
		return utils.StackError(err, "Unable to collect ownerless files during snapshot loading")
	}
	for _, file := range files {
		self.service.internalDrop(file)
	}

	return nil
}

func (self *dataState) HasFile(id cid.Cid) bool {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	for _, file := range self.files {
		if file == id {
			return true
		}
	}

	return false
}

type swarmDataService struct {
	data         *hostDataService
	stateService *sharedStateService
	state        *dataState
	session      exchange.Fetcher
	id           SwarmID
	ctx          context.Context
	cancel       context.CancelFunc
}

func newSwarmDataService(swarm *Swarm) DataService {

	ctx, cncl := context.WithCancel(context.Background())

	data := swarm.host.Data.(*hostDataService)
	session := data.bitswap.NewSession(ctx)

	service := &swarmDataService{data, swarm.State, nil, session, swarm.ID, ctx, cncl}
	service.state = newDataState(service)
	swarm.State.share(service.state)

	return service
}

func (self *swarmDataService) Add(ctx context.Context, path string) (cid.Cid, error) {

	//add the file
	filecid, _, err := self.data.basicAdd(path, string(self.id))
	if err != nil {
		return cid.Undef, err
	}

	//store in shared state
	cmd, err := dataStateCommand{filecid, false}.toByte()
	if err != nil {
		return cid.Undef, utils.StackError(err, "Unable to create command")
	}
	_, err = self.stateService.AddCommand(ctx, "dataState", cmd)
	if err != nil {
		self.data.Drop(ctx, filecid)
		return cid.Undef, utils.StackError(err, "Unable to share file with swarm members")
	}

	//return
	return filecid, nil
}

func (self *swarmDataService) AddAsync(path string) (cid.Cid, error) {

	//add the file
	filecid, _, err := self.data.basicAdd(path, string(self.id))
	if err != nil {
		return cid.Undef, err
	}

	go func() {
		//store in shared state
		cmd, _ := dataStateCommand{filecid, false}.toByte()
		ctx, _ := context.WithTimeout(self.ctx, 10*time.Hour)
		self.stateService.AddCommand(ctx, "dataState", cmd)
	}()

	//return
	return filecid, nil
}

func (self *swarmDataService) Drop(ctx context.Context, id cid.Cid) error {

	//drop the file in the swarm.
	cmd, err := dataStateCommand{id, true}.toByte()
	if err != nil {
		return utils.StackError(err, "Unable to create drop command")
	}
	_, err = self.stateService.AddCommand(ctx, "dataState", cmd)
	if err != nil {
		utils.StackError(err, "Unable to drop file within swarm")
	}

	return nil
}

func (self *swarmDataService) Fetch(ctx context.Context, id cid.Cid) error {

	//check if we have the file, if not fetching makes no sense in swarm context
	if !self.state.HasFile(id) {
		return fmt.Errorf("The file is not part of swarm, cannot be fetched")
	}
	//even if we have it in the state list, we fetch it anyway to make sure all blocks are received after the fetch call
	//(we could be in fetching phase)
	return self.data.basicFetch(ctx, id, self.session, string(self.id))
}

func (self *swarmDataService) FetchAsync(id cid.Cid) error {

	//check if we have the file, if not fetching makes no sense in swarm context
	if !self.state.HasFile(id) {
		return fmt.Errorf("The file is not part of swarm, cannot be fetched")
	}

	//we don't need to start a fetch, as it is to be expected that after this call the data may not be fully fetched yet
	return nil
}

func (self *swarmDataService) GetFile(ctx context.Context, id cid.Cid) (*os.File, error) {

	_, file, err := self.data.basicGetFile(ctx, id, self.session, string(self.id))
	if err != nil {
		return nil, err
	}

	//check if it is in the state already, if not we need to add
	if !self.state.HasFile(id) {
		//store in shared state
		cmd, _ := dataStateCommand{id, false}.toByte()
		self.stateService.AddCommand(ctx, "dataState", cmd)
	}

	return file, err
}

func (self *swarmDataService) Write(ctx context.Context, id cid.Cid, path string) (string, error) {

	//see if we can get the data
	path, err := self.data.basicWrite(ctx, id, path, self.session, string(self.id))
	if err != nil {
		return "", err
	}

	//check if it is in the state already, if not we need to add
	if !self.state.HasFile(id) {
		//store in shared state
		cmd, _ := dataStateCommand{id, false}.toByte()
		self.stateService.AddCommand(ctx, "dataState", cmd)
	}

	return path, nil
}

func (self *swarmDataService) Close() {

}

//internal data service functions to be called by data state

func (self *swarmDataService) internalFetch(ctx context.Context, id cid.Cid) error {

	err := self.data.basicFetch(ctx, id, self.session, string(self.id))
	if err != nil {
		return utils.StackError(err, "Unable to fetch id %v", id.String())
	}

	return nil
}

func (self *swarmDataService) internalFetchAsync(id cid.Cid) error {

	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Hour)
		self.data.basicFetch(ctx, id, self.session, string(self.id))
	}()
	return nil
}

func (self *swarmDataService) internalDrop(id cid.Cid) error {

	err := self.data.basicDrop(id, string(self.id))
	if err != nil {
		return utils.StackError(err, "Unable to drop id %v", id.String())
	}

	return nil
}
