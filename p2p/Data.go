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

func NewDataService(host *Host) (DataService, error) {

	//build the blockstore
	store, err := NewBitswapStore(viper.GetString("directory"))
	if err != nil {
		return DataService{}, err
	}
	bstore := blockstore.NewBlockstore(store)

	//build the blockservice from a blockstore and a bitswap
	bitswap, err := NewBitswap(bstore, host)
	if err != nil {
		return DataService{}, err
	}
	blockservice := bserv.New(bstore, bitswap)

	//build dagservice (merkledag) ontop of the blockservice
	dagservice := merkle.NewDAGService(blockservice)

	return DataService{dagservice}, nil
}

type DataService struct {
	service ipld.DAGService
}

func (self *DataService) AddFile(ctx context.Context, path string) (cid.Cid, error) {

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

func (self *DataService) GetFile(ctx context.Context, id cid.Cid) (io.Reader, error) {

	//get the root node
	node, err := self.service.Get(ctx, id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get root node")
	}

	//build the reader from the nodes DAG
	return unixfsio.NewDagReader(ctx, node, self.service)
}

func (self *DataService) DropFile(ctx context.Context, id cid.Cid) error {

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
