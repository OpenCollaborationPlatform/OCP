package p2p

/*Bitswap is the ipfs data trading system we reuse in OCP. To make it work we need to
  provide some building blocks:
  - Content Routing: Which peer has the requested CID?
  - Datastore: reused as blockstore for CID to data mapping*/

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"

	bs "github.com/ipfs/go-bitswap"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

/******************************************************************************
						Routing
******************************************************************************/

func NewBitswapRouting(host *Host, store blockstore.Blockstore) (BitswapRouting, error) {

	//setup the routing service
	err := host.Rpc.Register(&RoutingService{store})

	//return the routing
	return BitswapRouting{host}, err
}

//the routing type used by bitswap network
type BitswapRouting struct {
	host *Host
}

// Provide adds the given cid to the content routing system. If 'true' is
// passed, it also announces it, otherwise it is just kept in the local
// accounting of which objects are being provided.
func (self BitswapRouting) Provide(context.Context, cid.Cid, bool) error {
	//we do not need to advertise
	return nil
}

// Search for peers who are able to provide a given key
func (self BitswapRouting) FindProvidersAsync(ctx context.Context, val cid.Cid, num int) <-chan pstore.PeerInfo {

	result := make(chan pstore.PeerInfo)

	for _, peerid := range self.host.Peers(false) {
		go func(id PeerID) {
			var hasCID bool
			subctx, _ := context.WithCancel(ctx)
			err := self.host.Rpc.CallContext(subctx, id.ID, "RoutingService", "HasCID", val, &hasCID)
			if err != nil {
				log.Printf("Error in Routing service: %v", err)

			} else if hasCID {
				result <- self.host.host.Peerstore().PeerInfo(id.ID)
			}
		}(peerid)
	}

	return result
}

//the rpc service used to check if we can provide a CID
type RoutingService struct {
	store blockstore.Blockstore
}

func (self *RoutingService) HasCID(ctx context.Context, request cid.Cid, result *bool) error {
	if self.store == nil {
		return fmt.Errorf("Store is not correctly setup: Can't answer request")
	}

	has, err := self.store.Has(request)
	if err != nil {
		return utils.StackError(err, "Unable to query blockstore: Can't answer request")
	}
	*result = has

	return nil
}

/******************************************************************************
						Storage
******************************************************************************/

func NewBitswapStore(path string) (BitswapStore, error) {

	//make sure the path exist...
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return BitswapStore{}, utils.StackError(err, "Cannot open path %s", path)
	}

	//build the blt db
	path = filepath.Join(path, "bitswap.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return BitswapStore{}, utils.StackError(err, "Unable to open bolt db: %s", path)
	}

	//make sure we have the default bucket
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("data"))
		return err
	})

	return BitswapStore{db}, err
}

//A store based on boldDB that implements the ipfs Datastore, TxnDatastore and Batching interface
type BitswapStore struct {
	db *bolt.DB
}

func (self BitswapStore) Put(key ds.Key, value []byte) error {

	txn, err := self.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Commit()
	return txn.Put(key, value)
}

func (self BitswapStore) Delete(key ds.Key) error {

	txn, err := self.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Commit()

	return txn.Delete(key)
}

func (self BitswapStore) Get(key ds.Key) (value []byte, err error) {
	txn, err := self.NewTransaction(true)
	if err != nil {
		return nil, err
	}
	defer txn.Discard()
	return txn.Get(key)
}

func (self BitswapStore) Has(key ds.Key) (exists bool, err error) {
	txn, err := self.NewTransaction(true)
	if err != nil {
		return false, err
	}
	defer txn.Discard()
	return txn.Has(key)
}

func (self BitswapStore) GetSize(key ds.Key) (size int, err error) {
	txn, err := self.NewTransaction(true)
	if err != nil {
		return 0, err
	}
	defer txn.Discard()
	return txn.GetSize(key)
}

func (self BitswapStore) Query(q query.Query) (query.Results, error) {
	panic("Not implemented")
	return nil, nil
}

func (self BitswapStore) Batch() (ds.Batch, error) {

	return self.NewTransaction(false)
}

func (self BitswapStore) NewTransaction(readOnly bool) (ds.Txn, error) {
	tx, err := self.db.Begin(!readOnly)
	if err != nil {
		return nil, err
	}
	return &BitswapStoreTransaction{tx}, nil
}

func (self BitswapStore) Close() error {
	return self.db.Close()
}

//A transaction for BitswapStore that implements the ipfs Txn and Batch interface
type BitswapStoreTransaction struct {
	tx *bolt.Tx
}

func (self BitswapStoreTransaction) Put(key ds.Key, value []byte) error {

	bucket := self.tx.Bucket([]byte("data"))
	return bucket.Put(key.Bytes(), value)
}

func (self BitswapStoreTransaction) Delete(key ds.Key) error {

	bucket := self.tx.Bucket([]byte("data"))
	return bucket.Delete(key.Bytes())
}

func (self BitswapStoreTransaction) Get(key ds.Key) (value []byte, err error) {
	bucket := self.tx.Bucket([]byte("data"))
	res := bucket.Get(key.Bytes())
	if res == nil {
		return nil, ds.ErrNotFound
	}
	//res is only valid as long as the transaction is open
	ret := make([]byte, len(res))
	copy(ret, res)

	return ret, nil
}

func (self BitswapStoreTransaction) Has(key ds.Key) (exists bool, err error) {
	bucket := self.tx.Bucket([]byte("data"))
	res := bucket.Get(key.Bytes())
	return res != nil, nil
}

func (self BitswapStoreTransaction) GetSize(key ds.Key) (size int, err error) {
	bucket := self.tx.Bucket([]byte("data"))
	res := bucket.Get(key.Bytes())
	if res == nil {
		return 0, fmt.Errorf("Key not available in datastore")
	}
	//res is only valid as long as the transaction is open
	return len(res), nil
}

func (self BitswapStoreTransaction) Query(q query.Query) (query.Results, error) {
	panic("Not implemented")
	return nil, nil
}

func (self BitswapStoreTransaction) Commit() error {
	return self.tx.Commit()
}

func (self BitswapStoreTransaction) Discard() {
	self.tx.Rollback()
}

/******************************************************************************
						Bitswap
******************************************************************************/
func NewBitswap(bstore blockstore.Blockstore, host *Host) (*bs.Bitswap, error) {

	routing, err := NewBitswapRouting(host, bstore)
	if err != nil {
		return nil, err
	}

	network := bsnetwork.NewFromIpfsHost(host.host, routing)

	ctx := context.Background()
	exchange := bs.New(ctx, network, bstore)
	return exchange.(*bs.Bitswap), nil
}
