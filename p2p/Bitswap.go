package p2p

/*Bitswap is the ipfs data trading system we reuse in OCP. To make it work we need to
  provide some building blocks:
  - Content Routing: Which peer has the requested CID?
  - Datastore: reused as blockstore for CID to data mapping*/

import (
	"context"
	"fmt"

	"github.com/ickby/CollaborationNode/utils"

	bs "github.com/ipfs/go-bitswap"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

/******************************************************************************
						Routing
******************************************************************************/
func NewBitswapRouting(host *Host, store BitswapStore) (BitswapRouting, error) {

	//setup the routing service
	err := host.Rpc.Register(&RoutingService{store})

	//return the routing
	return BitswapRouting{host, store}, err
}

//the routing type used by bitswap network
type BitswapRouting struct {
	host  *Host
	store BitswapStore
}

// Provide adds the given cid to the content routing system. If 'true' is
// passed, it also announces it, otherwise it is just kept in the local
// accounting of which objects are being provided.
func (self BitswapRouting) Provide(ctx context.Context, id cid.Cid, announce bool) error {

	//we do not need to advertise if it is owned by a swarm, however, for global we need to advertise!
	block, err := self.store.Get(id)
	if err != nil {
		return utils.StackError(err, "Cannot get block for cid, hennce cannot announce")
	}
	owner, err := self.store.GetOwner(block)
	if err != nil {
		return utils.StackError(err, "Unable to find owner of block, cannot announce")
	}

	if len(owner) == 0 {
		return fmt.Errorf("No owner for cid known: cannot provide!")
	}

	//check if global is a owner and provide if so!
	for _, name := range owner {
		if name == "global" {
			return self.host.dht.Provide(ctx, id, announce) 
		}
	}

	return nil
}

// Search for peers who are able to provide a given key
func (self BitswapRouting) FindProvidersAsync(ctx context.Context, val cid.Cid, num int) <-chan pstore.PeerInfo {

	//if global is the owner, we are going to fetch it from the dht. Otherwise we simply
	//query the swarm members the file belongs to. As this is a limited set and we are
	//already connected to them this is a fast operation

	result := make(chan pstore.PeerInfo)

	//get the owner. if it is a swarm we only query swarm members
	var owner []string
	block, err := self.store.Get(val)
	if err == nil {
		owner, err = self.store.GetOwner(block)
		if err != nil {
			close(result)
			return result
		}

	} else {
		owner = self.store.GetExpectedOwner(val)
	}

	//check if it is a global cid and search globally if so
	swarms := make([]*Swarm, 0)
	if len(owner) != 0 {

		for _, name := range owner {
			if name == "global" {
				swarms = make([]*Swarm, 0)
				break

			} else {
				s, err := self.host.GetSwarm(SwarmID(name))
				if err != nil {
					continue
				}
				swarms = append(swarms, s)
			}
		}
	}
	if len(swarms) == 0 {
		return self.host.dht.FindProvidersAsync(ctx, val, num)
	}

	//search all possible peers and ask them!
	hasValues := make([]bool, 0)
	ret := make(chan *gorpc.Call, len(swarms)*5)
	goctx, cncl := context.WithCancel(ctx)
	for _, swarm := range swarms {
		for _, peer := range swarm.GetPeers(AUTH_NONE) {
			if self.host.IsConnected(peer) {
				hasValues = append(hasValues, false)
				self.host.Rpc.GoContext(goctx, peer, "RoutingService", "HasCID", val, &hasValues[len(hasValues)-1], ret)
			}
		}
	}

	//collect the needed results
	go func() {
		defer close(result)
		defer close(ret)

		//start the queries
		found := 0
		for call := range ret {

			if *(call.Reply.(*bool)) && call.Error == nil {
				found = found + 1
				result <- self.host.host.Peerstore().PeerInfo(call.Dest)

				if found >= num {
					cncl()
					return
				}
			}
		}
	}()

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
func NewBitswap(bstore BitswapStore, host *Host) (*bs.Bitswap, error) {

	routing, err := NewBitswapRouting(host, bstore)
	if err != nil {
		return nil, err
	}

	network := bsnetwork.NewFromIpfsHost(host.host, routing, bsnetwork.Prefix("ocp/"))

	ctx := context.Background()
	exchange := bs.New(ctx, network, bstore)
	return exchange.(*bs.Bitswap), nil
}
