package p2p

/*Bitswap is the ipfs data trading system we reuse in OCP. To make it work we need to
  provide some building blocks:
  - Content Routing: Which peer has the requested CID?
  - Datastore: reused as blockstore for CID to data mapping*/

import (
	"context"
	"fmt"
	"log"

	"github.com/ickby/CollaborationNode/utils"

	bs "github.com/ipfs/go-bitswap"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	cid "github.com/ipfs/go-cid"
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
			err := self.host.Rpc.CallContext(subctx, id.pid(), "RoutingService", "HasCID", val, &hasCID)
			if err != nil {
				log.Printf("Error in Routing service: %v", err)

			} else if hasCID {
				result <- self.host.host.Peerstore().PeerInfo(id.pid())
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
