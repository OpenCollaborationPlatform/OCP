package p2p

import (
	"context"
	"reflect"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	hclog "github.com/hashicorp/go-hclog"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

var protocolID = protocol.ID("ocp/rpc")

//The hostRpcService exposes the RpcService interface by combining the client and server type
//of gorpc
type hostRpcService struct {
	client    *gorpc.Client //the server we use to issue rpc calls
	server    *gorpc.Server //the server handling all RPC registration/messaging
	authorize *authorizer   //shared authorizer struct
	logger    hclog.Logger
}

//setup a new RpcService on the host and starts it
func newRpcService(host *Host) *hostRpcService {

	//build the authorisation function
	logger := host.logger.Named("RPC")
	auther := newAuthorizer()
	fnc := func(pid peer.ID, service string, function string) bool {
		auth := auther.peerIsAuthorized(service, PeerID(pid))
		logger.Debug("Authorisation checked", "peer", pid.Pretty(), "service", service, "function", function, "result", auth)
		return auth
	}

	server := gorpc.NewServer(host.host, protocolID, gorpc.WithAuthorizeFunc(fnc))
	client := gorpc.NewClientWithServer(host.host, protocolID, server)

	return &hostRpcService{client, server, auther, logger}
}

func (self *hostRpcService) Register(rcvr interface{}) error {

	value := reflect.ValueOf(rcvr)
	name := reflect.Indirect(value).Type().Name()

	//we allow absolutely everyone to call the services that are registered at the host
	self.authorize.addAuth(name, AUTH_READONLY, &constantPeerAuth{AUTH_READONLY})

	err := self.server.RegisterName(name, rcvr)
	err = wrapConnectionError(err, Error_Process)
	return err
}

func (self *hostRpcService) Call(dest PeerID, svcName, svcMethod string, args, reply interface{}) error {
	err := self.client.Call(dest, svcName, svcMethod, args, reply)
	err = wrapConnectionError(err, Error_Process)
	return err
}

func (self *hostRpcService) CallContext(ctx context.Context, dest PeerID, svcName, svcMethod string, args, reply interface{}) error {
	err := self.client.CallContext(ctx, dest, svcName, svcMethod, args, reply)
	err = wrapConnectionError(err, Error_Process)
	return err
}

func (self *hostRpcService) Go(dest PeerID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error {
	err := self.client.Go(dest, svcName, svcMethod, args, reply, done)
	err = wrapConnectionError(err, Error_Process)
	return err
}

func (self *hostRpcService) GoContext(ctx context.Context, dest PeerID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error {
	err := self.client.GoContext(ctx, dest, svcName, svcMethod, args, reply, done)
	err = wrapConnectionError(err, Error_Process)
	return err
}

func (self *hostRpcService) MultiCall(ctxs []context.Context, dests []PeerID, svcName, svcMethod string, args interface{}, replies []interface{}) []error {
	err := self.client.MultiCall(ctxs, dests, svcName, svcMethod, args, replies)
	for i, e := range err {
		err[i] = wrapConnectionError(e, Error_Process)
	}
	return err
}

func (self *hostRpcService) MultiGo(ctxs []context.Context, dests []PeerID, svcName, svcMethod string, args interface{}, replies []interface{}, dones []chan *gorpc.Call) error {
	err := self.client.MultiGo(ctxs, dests, svcName, svcMethod, args, replies, dones)
	err = wrapConnectionError(err, Error_Process)
	return err
}

func (self *hostRpcService) Close() {}

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

func (self *swarmRpcService) Call(dest PeerID, svcName, svcMethod string, args, reply interface{}) error {
	return self.rpc.Call(dest, self.swarmService(svcName), svcMethod, args, reply)
}

func (self *swarmRpcService) CallContext(ctx context.Context, dest PeerID, svcName, svcMethod string, args, reply interface{}) error {
	return self.rpc.CallContext(ctx, dest, self.swarmService(svcName), svcMethod, args, reply)
}

func (self *swarmRpcService) Go(dest PeerID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error {
	return self.rpc.Go(dest, self.swarmService(svcName), svcMethod, args, reply, done)
}

func (self *swarmRpcService) GoContext(ctx context.Context, dest PeerID, svcName, svcMethod string, args, reply interface{}, done chan *gorpc.Call) error {
	return self.rpc.GoContext(ctx, dest, self.swarmService(svcName), svcMethod, args, reply, done)
}

func (self *swarmRpcService) MultiCall(ctxs []context.Context, dests []PeerID, svcName, svcMethod string, args interface{}, replies []interface{}) []error {
	return self.rpc.MultiCall(ctxs, dests, self.swarmService(svcName), svcMethod, args, replies)
}

func (self *swarmRpcService) MultiGo(ctxs []context.Context, dests []PeerID, svcName, svcMethod string, args interface{}, replies []interface{}, dones []chan *gorpc.Call) error {
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
