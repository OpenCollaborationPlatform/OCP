package p2p

import (
	"CollaborationNode/utils"
	"bufio"
	"context"
	"io"
	"os"

	bserv "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	merkle "github.com/ipfs/go-merkledag"
	unixfsimp "github.com/ipfs/go-unixfs/importer"
	unixfsio "github.com/ipfs/go-unixfs/io"
	"github.com/spf13/viper"
)

type DataService interface {
	AddFile(ctx context.Context, path string) (cid.Cid, error)
	GetFile(ctx context.Context, id cid.Cid) (io.Reader, error)
	DropFile(ctx context.Context, id cid.Cid) error
	Close()
}

func NewDataService(host *Host) (DataService, error) {

	//build the blockstore
	store, err := NewBitswapStore(viper.GetString("directory"))
	if err != nil {
		return nil, err
	}
	bstore := blockstore.NewBlockstore(store)

	//build the blockservice from a blockstore and a bitswap
	bitswap, err := NewBitswap(bstore, host)
	if err != nil {
		return nil, err
	}
	blockservice := bserv.New(bstore, bitswap)

	//build dagservice (merkledag) ontop of the blockservice
	dagservice := merkle.NewDAGService(blockservice)

	return &hostDataService{dagservice, blockservice}, nil
}

type hostDataService struct {
	service  ipld.DAGService
	blockser bserv.BlockService
}

func (self *hostDataService) AddFile(ctx context.Context, path string) (cid.Cid, error) {

	//read the file
	file, err := os.Open(path)
	if err != nil {
		return cid.Cid{}, err
	}

	//build the dag
	splitter := chunker.DefaultSplitter(bufio.NewReader(file))
	dagnode, err := unixfsimp.BuildDagFromReader(self.service, splitter)

	return dagnode.Cid(), err
}

func (self *hostDataService) GetFile(ctx context.Context, id cid.Cid) (io.Reader, error) {

	//get the root node
	node, err := self.service.Get(ctx, id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get root node")
	}

	//build the reader from the nodes DAG
	return unixfsio.NewDagReader(ctx, node, self.service)
}

func (self *hostDataService) DropFile(ctx context.Context, id cid.Cid) error {

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

	return self.service.RemoveMany(ctx, cids)
}

func (self *hostDataService) Close() {
	self.blockser.Close()
}

//SwarmDataService
//This dataservice behaves sligthly different than the normal one:
// - Adding/Dropping a file automatically distributes it within the swarm
// - Added files are encryptet, so that they are unreadable outside of the swarm

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
