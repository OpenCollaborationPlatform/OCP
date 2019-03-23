package p2p

import (
	"context"
	"reflect"

	gorpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

//interface abstracting RPC behaviour
type RpcService interface {

	// Call performs an RpcService call to a registered Server service and blocks until
	// completed. If dest is empty ("") or matches the Client's host ID, it will
	// attempt to use the local configured Server when possible.
	Call(dest peer.ID, svcName, svcMethod string, args, reply interface{}) error

	// CallContext performs a Call() with a user provided context. This gives
	// the user the possibility of cancelling the operation at any point.
	CallContext(ctx context.Context, dest peer.ID, svcName, svcMethod string, args, reply interface{}) error

	// Go performs an RpcService call asynchronously. The associated Call will be placed
	// in the provided channel upon completion, holding any Reply or Errors.
	// The provided done channel must be nil, or have capacity for 1 element
	// at least, or a panic will be triggered.
	// If dest is empty ("") or matches the Client's host ID, it will
	// attempt to use the local configured Server when possible.
	Go(dest peer.ID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error

	// GoContext performs a Go() call with the provided context, allowing
	// the user to cancel the operation. See Go() documentation for more
	// information.
	// The provided done channel must be nil, or have capacity for 1 element
	// at least, or a panic will be triggered.
	GoContext(ctx context.Context, dest peer.ID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error

	// MultiCall performs a CallContext() to multiple destinations, using the same
	// service name, method and arguments. It will not return until all calls have
	// done so. The contexts, destinations and replies must match in length and
	// will be used in order (ctxs[i] is used for dests[i] which obtains
	// replies[i] and error[i]).
	//
	// The calls will be triggered in parallel (with one goroutine for each).
	MultiCall(ctxs []context.Context, dests []peer.ID, svcName, svcMethod string, args interface{}, replies []interface{}) []error

	// MultiGo performs a GoContext() call to multiple destinations, using the same
	// service name, method and arguments. MultiGo will return as right after
	// performing all the calls. See the Go() documentation for more information.
	// The provided done channels must be nil, or have capacity for 1 element
	// at least, or a panic will be triggered.
	// The contexts, destinations, replies and done channels  must match in length
	// and will be used in order (ctxs[i] is used for dests[i] which obtains
	// replies[i] with dones[i] signalled upon completion).
	MultiGo(ctxs []context.Context, dests []peer.ID, svcName, svcMethod string, args interface{}, replies []interface{}, dones []chan *gorpc.Call) error

	// Register publishes  the set of methods of the
	// receiver value that satisfy the following conditions:
	//	- exported method of exported type
	//	- three arguments, first context and the other two both of exported type
	//	- the third argument is a pointer
	//	- one return value of type error
	// It returns an error if the receiver is not an exported type or has
	// no suitable methods. It also logs the error using package log.
	// The client accesses each method using a string of the form "Type.Method",
	// where Type is the receiver's concrete type.
	Register(rcvr interface{}) error
}

var protocolID = protocol.ID("ocp/rpc")

//The hostRpcService exposes the RpcService interface by combining the client and server type
//of gorpc
type hostRpcService struct {
	*gorpc.Client

	server *gorpc.Server //cannot expose directly as Call is ambiguous
}

//setup a new RpcService on the host and starts it
func newRpcService(host *Host) RpcService {

	server := gorpc.NewServer(host.host, protocolID)
	client := gorpc.NewClientWithServer(host.host, protocolID, server)

	return &hostRpcService{client, server}
}

func (self *hostRpcService) Register(rcvr interface{}) error {
	return self.server.Register(rcvr)
}

/*******************************************************************************
						Swarm RpcService
*******************************************************************************/

//implements the RpcService interface. It does not create a own server, but uses the host RpcService
//server. The difference is that it allows to register services per swarm
type swarmRpcService struct {
	rpc *hostRpcService
	id  SwarmID
}

//setups a swarm RpcService on the host
func newSwarmRpcService(swarm *Swarm) RpcService {

	return &swarmRpcService{swarm.host.Rpc.(*hostRpcService), swarm.ID}
}

func (self *swarmRpcService) swarmService(service string) string {
	return self.id.Pretty() + "/" + service
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

func (self *swarmRpcService) Register(rcvr interface{}) error {

	value := reflect.ValueOf(rcvr)
	name := self.swarmService(reflect.Indirect(value).Type().Name())
	return self.rpc.server.RegisterName(name, rcvr)
}
