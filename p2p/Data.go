package p2p

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	hclog "github.com/hashicorp/go-hclog"
	bs "github.com/ipfs/go-bitswap"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-ds-badger2"
	blockDS "github.com/ipfs/go-ipfs-blockstore"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	unixfile "github.com/ipfs/go-unixfs/file"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/ipfs/go-datastore/query"
)

//the data service interface!
type DataService interface {
	Add(ctx context.Context, path string) (utils.Cid, error)              //adds a file or directory
	AddData(ctx context.Context, data []byte) (utils.Cid, error)          //adds a Blockifyer
	AddAsync(path string) (utils.Cid, error)                              //adds a file or directory, but returns when the local operation is done
	Drop(ctx context.Context, id utils.Cid) error                         //removes a file or directory
	Fetch(ctx context.Context, id utils.Cid) error                        //Fetches the given data
	FetchAsync(id utils.Cid) error                                        //Fetches the given data async
	Get(ctx context.Context, id utils.Cid) (io.Reader, error)             //gets the file described by the id (fetches if needed)
	Write(ctx context.Context, id utils.Cid, path string) (string, error) //writes the file or directory to the given path (fetches if needed)
	ReadChannel(ctx context.Context, id utils.Cid) (chan []byte, error)   //reads the data in individual binary blocks (does not work for directory)
	HasLocal(id utils.Cid) bool                                           //checks if the given id is available locally
	Close()
}

//Helper for implementation of DataService interface ontop of a merkle DAG
//Note: does not implement Close()
type dagHelper struct {
	dag ipld.DAGService
}

func (self *dagHelper) Add(ctx context.Context, path string) (utils.Cid, error) {

	stat, _ := os.Stat(path)
	file, err := files.NewSerialFile(path, false, stat)
	if err != nil {
		return utils.Cid{}, wrapInternalError(err, Error_Data)
	}
	adder, err := NewAdder(ctx, self.dag)
	if err != nil {
		return utils.Cid{}, utils.StackError(err, "Unable to create file adder")
	}

	node, err := adder.Add(file)
	if err != nil {
		return utils.Cid{}, utils.StackError(err, "Unable to add file to DAG")
	}

	//return
	return utils.FromP2PCid(node.Cid()), nil
}

func (self *dagHelper) AddData(ctx context.Context, data []byte) (utils.Cid, error) {

	file := files.NewBytesFile(data)

	adder, err := NewAdder(ctx, self.dag)
	if err != nil {
		return utils.Cid{}, utils.StackError(err, "Unable to create data adder")
	}

	node, err := adder.Add(file)
	if err != nil {
		return utils.Cid{}, utils.StackError(err, "Unable to add data to DAG")
	}

	//return
	return utils.FromP2PCid(node.Cid()), nil
}

func (self *dagHelper) AddAsync(path string) (utils.Cid, error) {

	//we don't do any network operation...
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Hour)
	c, err := self.Add(ctx, path)
	if err != nil {
		return utils.CidUndef, wrapConnectionError(err, Error_Data)
	}
	return c, nil
}

func (self *dagHelper) Drop(ctx context.Context, id utils.Cid) error {

	err := self.dag.Remove(ctx, id.P2P())
	if err != nil {
		return wrapConnectionError(err, Error_Data)
	}
	return nil
}

func (self *dagHelper) Fetch(ctx context.Context, id utils.Cid) error {

	resnode, err := self.dag.Get(ctx, id.P2P())
	if err != nil {
		return wrapConnectionError(err, Error_Data)
	}

	//make sure we have the whole dag fetched by visiting it
	navnode := ipld.NewNavigableIPLDNode(resnode, self.dag)
	walker := ipld.NewWalker(ctx, navnode)
	err = walker.Iterate(func(node ipld.NavigableNode) error { return nil })

	//End Of Dag is default error when iteration has finished
	if err != ipld.EndOfDag {
		return wrapConnectionError(err, Error_Data)
	}
	return nil
}

func (self *dagHelper) FetchAsync(id utils.Cid) error {

	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 1*time.Hour)
		self.Fetch(ctx, id)
	}()
	return nil
}

func (self *dagHelper) Get(ctx context.Context, id utils.Cid) (io.Reader, error) {

	resnode, err := self.dag.Get(ctx, id.P2P())
	if err != nil {
		return nil, wrapConnectionError(err, Error_Data)
	}
	filenode, err := unixfile.NewUnixfsFile(ctx, self.dag, resnode)
	if err != nil {
		return nil, wrapInternalError(err, Error_Data)
	}
	return files.ToFile(filenode), nil
}

//write result into path (including file/dir name)
func (self *dagHelper) Write(ctx context.Context, id utils.Cid, path string) (string, error) {

	resnode, err := self.dag.Get(ctx, id.P2P())
	if err != nil {
		return "", wrapConnectionError(err, Error_Data)
	}
	resfile, err := unixfile.NewUnixfsFile(ctx, self.dag, resnode)
	if err != nil {
		return "", wrapInternalError(err, Error_Data)
	}

	err = files.WriteTo(resfile, path)
	if err != nil {
		err = wrapInternalError(err, Error_Data)
	}
	return path, err

}

func (self *dagHelper) ReadChannel(ctx context.Context, id utils.Cid) (chan []byte, error) {

	reader, err := self.Get(ctx, id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get cid")
	}

	result := make(chan []byte, 0)
	go func() {
		data := make([]byte, 1e6)
		for {
			n, err := reader.Read(data)
			if n > 0 {
				local := make([]byte, n)
				copy(local, data[:n])
				result <- local
			}

			if err != nil {
				break
			}
		}

		close(result)
	}()

	return result, nil
}

/******************************************************************************
							HostDataService
*******************************************************************************/

//Basically uses exposes dagHelper for functionality, and additional stores all
//the stuff needed to create a mekrle dag

type hostDataService struct {
	dagHelper

	datapath   string
	bitswap    *bs.Bitswap
	ownerStore datastore.TxnDatastore
	blockStore blockDS.Blockstore
	ticker     *time.Ticker
	dht        *kaddht.IpfsDHT
	ctx        context.Context
}

func NewDataService(host *Host) (DataService, error) {

	//check if we have the data dir, if not create it
	path := filepath.Join(host.path, "DataExchange")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}

	//create the stores (blocks and owners)
	dstore, err := ds.NewDatastore(path, &ds.DefaultOptions)
	if err != nil {
		return nil, wrapInternalError(err, Error_Data)
	}
	bstore := blockDS.NewBlockstore(dstore)

	//create bitswap and default global DAG service
	routing, err := NewOwnerAwareRouting(host, dstore)
	if err != nil {
		return nil, utils.StackError(err, "Unable to create routing service")
	}
	network := bsnetwork.NewFromIpfsHost(host.host, routing)
	bitswap := bs.New(host.serviceCtx, network, bstore).(*bs.Bitswap)
	dag := NewDAGService("global", bitswap, dstore, bstore)

	//start the service
	service := &hostDataService{dagHelper{dag}, path, bitswap, dstore, bstore, time.NewTicker(20 * time.Hour), host.dht, host.serviceCtx}

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

func (self *hostDataService) HasLocal(id utils.Cid) bool {
	val, _ := self.blockStore.Has(id.P2P())
	return val
}

func (self *hostDataService) Close() {
	self.bitswap.Close()
	self.ownerStore.Close()
}

func (self *hostDataService) announceAllGlobal() {

	//we need to check all owners and fetch out the globals
	filter := query.FilterValueCompare{Op: query.Equal, Value: []byte("global")}
	q := query.Query{Prefix: "/Owners/", Filters: []query.Filter{filter}}
	qr, err := self.ownerStore.Query(q)
	if err != nil {
		return
	}

	//get the cids
	globals := make([]string, 0)
	for result := range qr.Next() {

		if result.Error != nil {
			continue
		}

		key := datastore.NewKey(result.Entry.Key)
		globals = append(globals, key.List()[1])
	}

	//take our time to announce the cids
	go func() {
		for _, id := range globals {
			ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
			parsed, _ := utils.CidDecode(id)
			self.dht.Provide(ctx, parsed.P2P(), true)
		}
	}()
}

/******************************************************************************
							SwarmDataService
*******************************************************************************/

//This dataservice behaves sligthly different than the normal one:
// - Adding/Dropping a file automatically distributes it within the swarm

type dataStateCommand struct {
	File   utils.Cid //the cid to add or remove from the list
	Remove bool      //if true is removed from list, if false it is added
}

func (self dataStateCommand) toByte() ([]byte, error) {

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(self)
	if err != nil {
		err = wrapInternalError(err, Error_Invalid_Data)
	}
	return buf.Bytes(), err
}

func dataStateCommandFromByte(data []byte) (dataStateCommand, error) {

	cmd := dataStateCommand{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&cmd)
	if err != nil {
		err = wrapInternalError(err, Error_Invalid_Data)
	}
	return cmd, err
}

//a shared state that is a list of CIDs this swarm shares.
type dataState struct {
	files   []utils.Cid
	service *swarmDataService
	ctx     context.Context
	cancel  context.CancelFunc
	mutex   sync.RWMutex //mutex needed as we access the state from outside the sharedStateService
}

func newDataState(service *swarmDataService) *dataState {

	ctx, cncl := context.WithCancel(context.Background())
	return &dataState{
		files:   make([]utils.Cid, 0),
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
		self.service.logger.Error("Received invalid state update", "Error", err.Error())
		return utils.StackError(err, "Unable to load command from data")
	}

	self.service.logger.Debug("State update startet", "Cid", cmd.File.Encode(), "Remove", cmd.Remove)

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
		return nil, wrapInternalError(err, Error_Invalid_Data)
	}
	return buf.Bytes(), nil
}

func (self *dataState) LoadSnapshot(snap []byte) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//we make it simple: remove all, add the new ones and than garbage collect
	buf := bytes.NewBuffer(snap)
	var list []utils.Cid
	err := gob.NewEncoder(buf).Encode(&list)
	if err != nil {
		return wrapInternalError(err, Error_Invalid_Data)
	}

	txn, err := self.service.owner.NewTransaction(false)
	if err != nil {
		return utils.StackError(err, "Unable to open transaction")
	}

	//drop ownership of all old files (blocks are handled by garbage collect)
	for _, file := range self.files {
		key := datastore.NewKey(fmt.Sprintf("/Owners/%v/%v", file.String(), self.service.swarm.ID))
		txn.Delete(key)
	}

	//get new files
	for _, file := range list {
		self.service.FetchAsync(file)
	}
	self.files = list

	return nil
}

func (self *dataState) HasFile(id utils.Cid) bool {

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
	dag    dagHelper
	swarm  *Swarm
	owner  datastore.TxnDatastore
	state  *dataState
	ctx    context.Context
	cancel context.CancelFunc
	logger hclog.Logger
}

func newSwarmDataService(swarm *Swarm) DataService {

	//create the merkle dag helper
	hostService := swarm.host.Data.(*hostDataService)
	dag := NewDAGService(string(swarm.ID), hostService.bitswap, hostService.ownerStore, hostService.blockStore)

	//create the service
	ctx, cncl := context.WithCancel(swarm.ctx)
	service := &swarmDataService{dag: dagHelper{dag},
		swarm:  swarm,
		owner:  hostService.ownerStore,
		ctx:    ctx,
		cancel: cncl,
		logger: swarm.logger.Named("Data"),
	}

	//handle the data state
	service.state = newDataState(service)
	swarm.State.share(service.state)

	return service
}

func (self *swarmDataService) Add(ctx context.Context, path string) (utils.Cid, error) {

	self.logger.Trace("Add content path", "Path", path)

	//add the file
	filecid, err := self.dag.Add(ctx, path)
	if err != nil {
		self.logger.Error("Content adding to bitswap failed", "Error", err.Error())
		return utils.CidUndef, utils.StackError(err, "Unable to add path to dag")
	}
	self.logger.Debug("Content added to bitswap", "Cid", filecid.Encode())

	//store in shared state
	cmd, err := dataStateCommand{filecid, false}.toByte()
	if err != nil {
		return utils.CidUndef, utils.StackError(err, "Unable to create command")
	}
	_, err = self.swarm.State.AddCommand(ctx, "dataState", cmd)
	if err != nil {
		self.dag.Drop(ctx, filecid)
		self.logger.Error("Content distribution in swarm state failed", "Error", err.Error())
		return utils.CidUndef, utils.StackError(err, "Unable to share file with swarm members")
	}

	//return
	return filecid, nil
}

func (self *swarmDataService) AddData(ctx context.Context, data []byte) (utils.Cid, error) {

	self.logger.Trace("Add byte data")

	filecid, err := self.dag.AddData(ctx, data)
	if err != nil {
		self.logger.Error("Content adding to bitswap failed", "Error", err.Error())
		return utils.CidUndef, utils.StackError(err, "Unable to add data to dag")
	}
	self.logger.Debug("Content added to bitswap", "Cid", filecid.Encode())

	//store in shared state
	cmd, err := dataStateCommand{filecid, false}.toByte()
	if err != nil {
		return utils.CidUndef, utils.StackError(err, "Unable to create command")
	}
	_, err = self.swarm.State.AddCommand(ctx, "dataState", cmd)
	if err != nil {
		self.dag.Drop(ctx, filecid)
		self.logger.Error("Content distribution in swarm state failed", "Error", err.Error())
		return utils.CidUndef, utils.StackError(err, "Unable to share blocks with swarm members")
	}

	//return
	return filecid, err
}

func (self *swarmDataService) AddAsync(path string) (utils.Cid, error) {

	self.logger.Trace("Add path asyncronous", "Path", path)

	//add the file
	filecid, err := self.dag.AddAsync(path)
	if err != nil {
		return utils.CidUndef, utils.StackError(err, "Unable to add data to dag")
	}
	self.logger.Debug("Content added to bitswap", "Cid", filecid.Encode())

	go func() {
		//store in shared state
		cmd, _ := dataStateCommand{filecid, false}.toByte()
		ctx, _ := context.WithTimeout(self.ctx, 10*time.Hour)
		_, err := self.swarm.State.AddCommand(ctx, "dataState", cmd)
		if err != nil {
			self.logger.Error("Content distribution in swarm state failed", "Error", err.Error())
		}
	}()

	//return
	return filecid, nil
}

func (self *swarmDataService) Drop(ctx context.Context, id utils.Cid) error {

	self.logger.Trace("Drop content", "Cid", id.Encode())

	//drop the file in the swarm (real drop handled in state handler)
	cmd, err := dataStateCommand{id, true}.toByte()
	if err != nil {
		self.logger.Error("Droping content failed, unable to create command", "Error", err.Error())
		return utils.StackError(err, "Unable to create drop command")
	}
	_, err = self.swarm.State.AddCommand(ctx, "dataState", cmd)
	if err != nil {
		self.logger.Error("Droping content from state failed", "Error", err.Error())
		return utils.StackError(err, "Unable to drop file within swarm")
	}

	return nil
}

func (self *swarmDataService) Fetch(ctx context.Context, id utils.Cid) error {

	self.logger.Trace("Fetch content", "Cid", id.Encode())

	//check if we have the file, if not fetching makes no sense in swarm context
	if !self.state.HasFile(id) {
		self.logger.Error("Content is not part of state", "Cid", id.Encode())
		return newUserError(Error_Operation_Invalid, "The file is not part of swarm, cannot be fetched")
	}
	//even if we have it in the state list, we fetch it anyway to make sure all blocks are received after the fetch call
	//(we could be in fetching phase)
	err := self.dag.Fetch(ctx, id)
	if err != nil {
		self.logger.Error("Fetching content failed", "Cid", id.Encode(), "Error", err.Error())
		return wrapConnectionError(err, Error_Data)
	}

	self.logger.Debug("Content fetched", "Cid", id.Encode())
	return nil
}

func (self *swarmDataService) FetchAsync(id utils.Cid) error {

	self.logger.Trace("Fetch content asyncronous", "Cid", id.Encode())

	//check if we have the file, if not fetching makes no sense in swarm context
	if !self.state.HasFile(id) {
		self.logger.Error("Content is not part of state", "Cid", id.Encode())
		return newUserError(Error_Operation_Invalid, "The file is not part of swarm, cannot be fetched")
	}

	//we don't need to start a fetch, as it is to be expected that after this call the data may not be fully fetched yet
	//and if it is in the state fetching already takes place
	return nil
}

func (self *swarmDataService) Get(ctx context.Context, id utils.Cid) (io.Reader, error) {

	self.logger.Trace("Try to retrieve content", "Cid", id.Encode())

	reader, err := self.dag.Get(ctx, id)
	if err != nil {
		self.logger.Error("Retrieving content failed", "Cid", id.Encode(), "Error", err.Error())
		return nil, utils.StackError(err, "Unable to get cid from dag")
	}

	//check if it is in the state already, if not we need to add
	if !self.state.HasFile(id) {
		//store in shared state
		cmd, _ := dataStateCommand{id, false}.toByte()
		_, err := self.swarm.State.AddCommand(ctx, "dataState", cmd)
		if err != nil {
			self.logger.Error("Content adding to state failed", "Cid", id.Encode(), "Error", err.Error())
			return nil, err
		}
	}

	return reader, nil
}

func (self *swarmDataService) Write(ctx context.Context, id utils.Cid, path string) (string, error) {

	self.logger.Trace("Write content to path", "Cid", id.Encode(), "Path", path)

	//see if we can get the data
	path, err := self.dag.Write(ctx, id, path)
	if err != nil {
		return "", utils.StackError(err, "Unable to write cid from dag")
	}

	//check if it is in the state already, if not we need to add
	if !self.state.HasFile(id) {
		//store in shared state
		cmd, _ := dataStateCommand{id, false}.toByte()
		self.swarm.State.AddCommand(ctx, "dataState", cmd)
	}

	return path, nil
}

func (self *swarmDataService) ReadChannel(ctx context.Context, id utils.Cid) (chan []byte, error) {

	self.logger.Trace("Write content into channel", "Cid", id.Encode())

	c, err := self.dag.ReadChannel(ctx, id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to read cid from daf")
	}

	//check if it is in the state already, if not we need to add
	if !self.state.HasFile(id) {
		//store in shared state
		cmd, _ := dataStateCommand{id, false}.toByte()
		self.swarm.State.AddCommand(ctx, "dataState", cmd)
	}

	return c, nil
}

func (self *swarmDataService) HasLocal(id utils.Cid) bool {
	return self.state.HasFile(id)
}

func (self *swarmDataService) Close() {
	self.cancel()
}

//internal data service functions to be called by data state
//(without adding stuff to the state)

func (self *swarmDataService) internalFetch(ctx context.Context, id utils.Cid) error {

	err := self.dag.Fetch(ctx, id)
	if err != nil {
		return utils.StackError(err, "Unable to fetch id %v", id.String())
	}

	return nil
}

func (self *swarmDataService) internalFetchAsync(id utils.Cid) error {

	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Hour)
		err := self.dag.Fetch(ctx, id)
		if err != nil {
			self.logger.Error("Asyncronous state content fetch failed", "Cid", id.Encode(), "Error", err.Error())
		} else {
			self.logger.Debug("State content fetched", "Cid", id.Encode())
		}
	}()
	return nil
}

func (self *swarmDataService) internalDrop(id utils.Cid) error {

	ctx, _ := context.WithTimeout(self.ctx, 1*time.Hour)
	err := self.dag.Drop(ctx, id)
	if err != nil {
		self.logger.Error("State content drop failed", "Cid", id.Encode(), "Error", err.Error())
		return utils.StackError(err, "Unable to drop id %v", id.String())
	}

	self.logger.Debug("State content dropped", "Cid", id.Encode())
	return nil
}
