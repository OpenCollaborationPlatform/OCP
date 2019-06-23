package p2p

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"os"
	"path/filepath"

	bserv "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
)

type DataService interface {
	AddFile(ctx context.Context, path string) (cid.Cid, error)
	GetFile(ctx context.Context, id cid.Cid) (io.Reader, error)
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

	return &hostDataService{path, blockservice}, nil
}

type hostDataService struct {
	datapath string
	service  bserv.BlockService
}

func (self *hostDataService) AddFile(ctx context.Context, path string) (cid.Cid, error) {

	//we first blockify the file bevore moving it into our store.
	//This is done to know the cid after which we name the file in the store
	//to avoid having problems with same filenames for different files
	blocks, filecid, err := blockifyFile(path)
	if err != nil {
		return filecid, err
	}

	//copy the file over
	source, _ := os.Open(path) //no error checking, blockify did this already
	defer source.Close()

	destpath := filepath.Join(self.datapath, filecid.String())
	destination, err := os.Create(destpath)
	if err != nil {
		return cid.Cid{}, err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	if err != nil {
		return cid.Cid{}, err
	}

	//make the blocks available
	err = self.service.AddBlocks(blocks)

	return filecid, err
}

func (self *hostDataService) GetFile(ctx context.Context, id cid.Cid) (io.Reader, error) {
	/*
		//get the root node
		node, err := self.service.Get(ctx, id)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get root node")
		}

		//build the reader from the nodes DAG
		return unixfsio.NewDagReader(ctx, node, self.service)*/

	return nil, nil
}

func (self *hostDataService) DropFile(ctx context.Context, id cid.Cid) error {
	/*
		cids := make([]cid.Cid, 1)
		cids[0] = id

		visit := func(id cid.Cid) bool {
			cids = append(cids, id)
			return true
		}

		err := merkle.EnumerateChildren(ctx, merkle.GetLinksDirect(self.service), id, visit)
		if err != nil {
			return utils.StackError(err, "Unable to drop file")
		}

		return self.service.RemoveMany(ctx, cids)*/

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
	files []cid.Cid
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

	} else {
		self.files = append(self.files, cmd.file)
	}
	return nil
}

/*
	Reset() error       //reset state to initial value without any apply

	//snapshoting
	Snapshot() ([]byte, error)   //crete a snapshot from current state
	LoadSnapshot([]byte) error   //setup state according to snapshot
	EnsureSnapshot([]byte) error //make sure this snapshot represents the current state
}*/

type swarmDataService struct {
	data  *hostDataService
	event *swarmEventService
}

func newSwarmDataService(swarm *Swarm) DataService {

	hostdata := swarm.host.Data.(*hostDataService)
	return &swarmDataService{hostdata, swarm.Event}
}

func (self *swarmDataService) AddFile(ctx context.Context, path string) (cid.Cid, error) {

	cid, err := self.data.AddFile(ctx, path)
	/*
		//announce file if we have been successfull
		if err != nil {
			self.event.Publish("NewDataFile", cid.Bytes())
		}
	*/
	return cid, err
}

func (self *swarmDataService) GetFile(ctx context.Context, id cid.Cid) (io.Reader, error) {

	return self.data.GetFile(ctx, id)
}

func (self *swarmDataService) DropFile(ctx context.Context, id cid.Cid) error {

	err := self.data.DropFile(ctx, id)
	/*
		//announce file if we have been successfull
		if err != nil {
			self.event.Publish("DroppedDataFile", id.Bytes())
		}
	*/
	return err
}

func (self *swarmDataService) Close() {

}
