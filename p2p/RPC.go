package p2p

import (
	"context"
	"reflect"

	"github.com/ickby/CollaborationNode/utils"

	gorpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

var protocolID = protocol.ID("ocp/rpc")

//The hostRpcService exposes the RpcService interface by combining the client and server type
//of gorpc
type hostRpcService struct {
	*gorpc.Client
	server    *gorpc.Server //cannot expose directly as Call is ambiguous
	authorize *authorizer   //share authorizer struct
}

//setup a new RpcService on the host and starts it
func newRpcService(host *Host) *hostRpcService {

	//build the authorisation function
	auther := newAuthorizer()
	fnc := func(pid peer.ID, service string, function string) bool {
		return auther.peerIsAuthorized(service, PeerID(pid))
	}

	server := gorpc.NewServer(host.host, protocolID, gorpc.WithAuthorizeFunc(fnc))
	client := gorpc.NewClientWithServer(host.host, protocolID, server)

	return &hostRpcService{client, server, auther}
}

func (self *hostRpcService) Register(rcvr interface{}) error {

	value := reflect.ValueOf(rcvr)
	name := reflect.Indirect(value).Type().Name()

	//we allow absolutely everyone to call the services that are registered at the host
	self.authorize.addAuth(name, AUTH_READONLY, &constantPeerAuth{AUTH_READONLY})

	return self.server.RegisterName(name, rcvr)
}

/*******************************************************************************
						Swarm RpcService
*******************************************************************************/

//implements the RpcService interface. It does not create a own server, but uses the host RpcService
//server. The difference is that it allows to register services per swarm
type swarmRpcService struct {
	rpc   *hostRpcService
	swarm *Swarm
}

//setups a swarm RpcService on the host
func newSwarmRpcService(swarm *Swarm) *swarmRpcService {

	return &swarmRpcService{swarm.host.Rpc, swarm}
}

func (self *swarmRpcService) swarmService(service string) string {
	return self.swarm.ID.Pretty() + "/" + service
}

func (self *swarmRpcService) Call(dest peer.ID, svcName, svcMethod string, args, reply interface{}) error {
	return self.rpc.Call(dest, self.swarmService(svcName), svcMethod, args, reply)
}

func (self *swarmRpcService) CallContext(ctx context.Context, dest peer.ID, svcName, svcMethod string, args, reply interface{}) error {
	return self.rpc.CallContext(ctx, dest, self.swarmService(svcName), svcMethod, args, reply)
}

func (self *swarmRpcService) Go(dest peer.ID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error {
	return self.rpc.Go(dest, self.swarmService(svcName), svcMethod, args, reply, done)
}

func (self *swarmRpcService) GoContext(ctx context.Context, dest peer.ID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error {
	return self.rpc.GoContext(ctx, dest, self.swarmService(svcName), svcMethod, args, reply, done)
}

func (self *swarmRpcService) MultiCall(ctxs []context.Context, dests []peer.ID, svcName, svcMethod string, args interface{}, replies []interface{}) []error {
	return self.rpc.MultiCall(ctxs, dests, self.swarmService(svcName), svcMethod, args, replies)
}

func (self *swarmRpcService) MultiGo(ctxs []context.Context, dests []peer.ID, svcName, svcMethod string, args interface{}, replies []interface{}, dones []chan *gorpc.Call) error {
	return self.rpc.MultiGo(ctxs, dests, self.swarmService(svcName), svcMethod, args, replies, dones)
}

func (self *swarmRpcService) Register(rcvr interface{}, required_auth AUTH_STATE) error {

	//get the name
	value := reflect.ValueOf(rcvr)
	name := self.swarmService(reflect.Indirect(value).Type().Name())

	//handle authorisation
	err := self.rpc.authorize.addAuth(name, required_auth, self.swarm)
	if err != nil {
		return utils.StackError(err, "Cannot register swarm RPC service")
	}

	//finally register in the RPC server
	return self.rpc.server.RegisterName(name, rcvr)
}
