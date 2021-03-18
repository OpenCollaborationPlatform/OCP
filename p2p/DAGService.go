package p2p

import (
	"context"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP /utils"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	ipld "github.com/ipfs/go-ipld-format"
	merkle "github.com/ipfs/go-merkledag"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
)

/******************************************************************************
						DAG Service
******************************************************************************/

func NewDAGService(owner string, session exchange.Interface, ownerstore datastore.TxnDatastore, blockstore blockstore.Blockstore) ipld.DAGService {

	blksvc := NewOwnerAwareBlockService(owner, ownerstore, blockstore, session)
	return merkle.NewDAGService(blksvc)
}

/******************************************************************************
						Block handling
******************************************************************************/

//creates a blockservice with a datastore for ownership data and a blockstore for storing the block data. Note: Could be the same datastore for both
func NewOwnerAwareBlockService(owner string, ds datastore.TxnDatastore, bs blockstore.Blockstore, service exchange.Interface) *OwnerAwareBlockService {

	blksvc := blockservice.New(bs, service)

	return &OwnerAwareBlockService{owner, ds, bs, blksvc}
}

//implementation of the IPFS Blockservice interface.
//reason to have our own is to allow ownership of blocks for our swarm implementation
//it works together with OwnerAwareRouting
type OwnerAwareBlockService struct {
	owner      string
	datastore  datastore.TxnDatastore
	blockstore blockstore.Blockstore
	blocksvc   blockservice.BlockService
}

func (self *OwnerAwareBlockService) Close() error {
	return utils.StackOnError(self.blocksvc.Close(), "Unable to close internal blockservice")
}

// GetBlock gets the requested block.
func (self *OwnerAwareBlockService) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {

	//we simply add ourself as owner. If already in nothing happens, if not good.
	//this should be the fastest way of doing it
	//This is needed now so that the routing service knows where to look
	key := datastore.NewKey(fmt.Sprintf("/Owners/%v/%v", c.String(), self.owner))
	err := self.datastore.Put(key, []byte(self.owner))
	if err != nil {
		return nil, wrapInternalError(err, Error_Data)
	}

	blck, err := self.blocksvc.GetBlock(ctx, c)
	if err != nil {
		return nil, wrapConnectionError(err, Error_Data)
	}
	return blck, nil
}

// GetBlocks does a batch request for the given cids, returning blocks as
// they are found, in no particular order.
//
// It may not be able to find all requested blocks (or the context may
// be canceled). In that case, it will close the channel early. It is up
// to the consumer to detect this situation and keep track which blocks
// it has received and which it hasn't.
func (self *OwnerAwareBlockService) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {

	//we simply add the new owner. If already in nothing happens, if not good.
	//this should be the fastest way of doing it
	//This is needed now so that the routing service knows where to look
	txn, err := self.datastore.NewTransaction(false)
	if err != nil {
		return nil
	}
	for _, o := range ks {
		key := datastore.NewKey(fmt.Sprintf("/Owners/%v/%v", o.String(), self.owner))
		if err := self.datastore.Put(key, []byte(self.owner)); err != nil {
			txn.Discard()
			return nil
		}
	}
	txn.Commit()
	return self.blocksvc.GetBlocks(ctx, ks)
}

// Blockstore returns a reference to the underlying blockstore
func (self *OwnerAwareBlockService) Blockstore() blockstore.Blockstore {
	return self.blocksvc.Blockstore()
}

// Exchange returns a reference to the underlying exchange (usually bitswap)
func (self *OwnerAwareBlockService) Exchange() exchange.Interface {
	return self.blocksvc.Exchange()
}

// AddBlock puts a given block to the underlying datastore
func (self *OwnerAwareBlockService) AddBlock(o blocks.Block) error {

	c := o.Cid()

	//we simply add the new owner. If already in nothing happens, if not good.
	//this should be the fastest way of doing it
	key := datastore.NewKey(fmt.Sprintf("/Owners/%v/%v", c.String(), self.owner))
	err := self.datastore.Put(key, []byte(self.owner))

	if err != nil {
		return wrapInternalError(err, Error_Data)
	}

	//add to block service
	err = self.blocksvc.AddBlock(o)
	if err != nil {
		return wrapConnectionError(err, Error_Data)
	}
	return nil
}

// AddBlocks adds a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (self *OwnerAwareBlockService) AddBlocks(bs []blocks.Block) error {

	txn, err := self.datastore.NewTransaction(false)
	if err != nil {
		return wrapInternalError(err, Error_Data)
	}

	for _, o := range bs {
		key := datastore.NewKey(fmt.Sprintf("/Owners/%v/%v", o.Cid().String(), self.owner))
		if err := txn.Put(key, []byte(self.owner)); err != nil {
			txn.Discard()
			return wrapInternalError(err, Error_Data)
		}
	}
	txn.Commit()

	for _, o := range bs {
		err := self.blocksvc.AddBlock(o) //cannot use add blocks as bader ds does not check transaction size
		if err != nil {
			return wrapConnectionError(err, Error_Data)
		}
	}

	return nil
}

// DeleteBlock deletes the given block from the blockservice.
func (self *OwnerAwareBlockService) DeleteBlock(c cid.Cid) error {

	key := datastore.NewKey(fmt.Sprintf("/Owners/%v/%v", c.String(), self.owner))
	err := self.datastore.Delete(key)
	if err != nil {
		return wrapInternalError(err, Error_Data)
	}

	//check if there is a owner left, delete otherwise
	q := query.Query{Prefix: fmt.Sprintf("/Owners/%v", c.String()), Limit: 1}
	qr, err := self.datastore.Query(q)
	if err != nil {
		return wrapInternalError(err, Error_Data)
	}
	es, err := qr.Rest()
	if err != nil {
		return wrapInternalError(err, Error_Data)
	}
	if len(es) == 0 {
		err = self.blocksvc.DeleteBlock(c)
		if err != nil {
			return wrapConnectionError(err, Error_Data)
		}
	}

	return nil
}

/******************************************************************************
						Routing
******************************************************************************/

func NewOwnerAwareRouting(host *Host, ownerStore datastore.Datastore) (OwnerAwareRouting, error) {

	//return the routing
	return OwnerAwareRouting{host, ownerStore}, nil
}

//the routing type used by bitswap network
type OwnerAwareRouting struct {
	host  *Host
	owner datastore.Datastore
}

// Provide adds the given cid to the content routing system. If 'true' is
// passed, it also announces it, otherwise it is just kept in the local
// accounting of which objects are being provided.
func (self OwnerAwareRouting) Provide(ctx context.Context, id cid.Cid, announce bool) error {

	//check if we have a global owner
	key := datastore.NewKey(fmt.Sprintf("/Owners/%v/global", id.String()))
	isGlobal, err := self.owner.Has(key)
	if err != nil {
		return wrapInternalError(err, Error_Data)
	}

	//if global we announce it in the dht
	if isGlobal {
		err = self.host.dht.Provide(ctx, id, announce)
		if err != nil {
			return wrapConnectionError(err, Error_Data)
		}
	}

	return nil
}

// Search for peers who are able to provide a given key
func (self OwnerAwareRouting) FindProvidersAsync(ctx context.Context, id cid.Cid, num int) <-chan peerstore.PeerInfo {

	q := query.Query{Prefix: fmt.Sprintf("/Owners/%v/", id.String()), KeysOnly: true}
	qr, err := self.owner.Query(q)
	if err != nil {
		res := make(chan peerstore.PeerInfo, 0)
		close(res)
		return nil
	}

	owners := make([]string, 0)
	for result := range qr.Next() {

		if result.Error != nil {
			continue
		}

		key := datastore.NewKey(result.Entry.Key)
		owners = append(owners, key.Name())
	}

	//check if global owner and use dht if so
	for _, owner := range owners {
		if owner == "global" {
			return self.host.dht.FindProvidersAsync(ctx, id, num)
		}
	}

	//if not we use the swarm owner!
	infos := make([]peerstore.PeerInfo, 0)
	for _, owner := range owners {

		swarm, err := self.host.GetSwarm(SwarmID(owner))
		if err == nil {

			for _, peer := range swarm.GetPeers(AUTH_NONE) {
				if self.host.IsConnected(peer) {
					infos = append(infos, self.host.host.Peerstore().PeerInfo(peer))
				}
			}
		}
	}

	//check if we need all peers
	if len(infos) > num {
		infos = infos[:num-1]
	}

	//provide the data!
	result := make(chan peerstore.PeerInfo, 0)
	go func() {
		defer close(result)

		for _, info := range infos {
			select {
			case result <- info:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return result
}
