package p2p

import (
	"CollaborationNode/p2p/replica"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"

	crypto "github.com/libp2p/go-libp2p-crypto"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
)

/******************************************************************************
							libp2p replica transport
*******************************************************************************/

type RpcEvent struct {
	api *replica.ReadEventAPI
}

func (self *RpcEvent) InvalidLogReceived(ctx context.Context, arg uint64, result *struct{}) error {
	//call the api in an event like manner: no blocking
	go func() {
		self.api.InvalidLogReceived(arg)
	}()
	return nil
}

//a replica transport implementation based on events and rpcs
type p2pTransport struct {
	swarm *Swarm
	id    PeerID
}

func newP2pTransport() {

}

func (self *p2pTransport) setupEvent(name string, auth AUTH_STATE, fnc func(data *gob.Decoder)) error {

	sub, err := self.swarm.Event.Subscribe(name, auth)
	if err != nil {
		return err
	}

	go func() {
		for {
			event, err := sub.Next(self.swarm.ctx)
			if err != nil {
				return
			}
			b := bytes.NewBuffer(event.Data)
			dec := gob.NewDecoder(b)
			fnc(dec)
		}
	}()

	return nil
}

//Register APIs to be accessbile by other replicas using their transports Call functions.
//APIs have two different function syntaxes:
//with return value, used for "Call" and "CallAny": 	fnc(ctx context.Context, arg Type1, result *Type2) error
//without return value, used for "Send": 				fnc(arg Type1)
//The register function should handle all API functions according to their structure for the call or send.
func (self *p2pTransport) RegisterReadAPI(rpc *replica.ReadRPCAPI, event *replica.ReadEventAPI) error {

	//registering rpc functions is easy!
	err := self.swarm.Rpc.Register(rpc, AUTH_READONLY)
	if err != nil {
		return err
	}

	//we implement this send function also via RPC as event is overkill: should only go to one address
	err = self.swarm.Rpc.Register(&RpcEvent{event}, AUTH_READONLY)
	if err != nil {
		return err
	}

	return nil
}

func (self *p2pTransport) RegisterWriteAPI(rpc *replica.WriteRPCAPI, event *replica.WriteEventAPI) error {

	//registering rpc functions is easy!
	self.swarm.Rpc.Register(rpc, AUTH_READWRITE)

	//now go over all event functions. We don't use reflect to keep it simple
	err := self.setupEvent("WriteEventAPI/NewLog", AUTH_READWRITE, func(dec *gob.Decoder) {

		var log replica.Log
		err := dec.Decode(&log)
		if err != nil {
			fmt.Printf("\nError decoding log %v!\n", err)
			return
		}
		event.NewLog(log)
	})
	if err != nil {
		return err
	}

	err = self.setupEvent("WriteEventAPI/NewBeacon", AUTH_READWRITE, func(dec *gob.Decoder) {

		var beacon replica.Beacon
		err := dec.Decode(&beacon)
		if err != nil {
			return
		}
		event.NewBeacon(beacon)
	})
	if err != nil {
		return err
	}

	return nil
}

//call a replica with the given adress. Usally used for the leader. If an error occurs during the call
//it will be returned
func (self *p2pTransport) Call(ctx context.Context, addr replica.Address, api string, fnc string, arguments interface{}, reply interface{}) error {

	peer, err := PeerIDFromString(string(addr))
	if err != nil {
		return err
	}
	return self.swarm.Rpc.CallContext(ctx, peer.pid(), api, fnc, arguments, reply)
}

//call a random replica (except self). The function should try all replicas till one does successfully execute the call
//(no error returned). CallAny fails only if no replica can execute the called function. It does return the Address which
//responded.
func (self *p2pTransport) CallAny(ctx context.Context, api string, fnc string, arguments interface{}, reply interface{}) (replica.Address, error) {

	var peerauth AUTH_STATE
	if api == `ReadAPI` {
		peerauth = AUTH_READONLY

	} else if api == `WriteAPI` {
		peerauth = AUTH_READWRITE

	} else {
		return replica.Address(""), fmt.Errorf("No such API known")
	}

	peers := self.swarm.GetPeers(peerauth)
	if len(peers) == 0 {
		return replica.Address(""), fmt.Errorf("No replicas available to call")
	}

	//we start with a single peer, and gradually add more calls till one returns.
	callctx, cncl := context.WithCancel(ctx)
	ticker := time.NewTicker(100 * time.Millisecond)
	idxs := rand.Perm(len(peers))
	done := make(chan *gorpc.Call, len(peers))
	calls := 0

	for _, idx := range idxs {

		//do not call ourself
		if peers[idx] == self.id {
			continue
		}

		//start next call
		if err := self.swarm.Rpc.GoContext(callctx, peers[idx].pid(), api, fnc, arguments, reply, done); err == nil {
			calls++
		} else {
			continue
		}

		select {
		case <-ticker.C:
			//just go on with the next loop iteraction

		case <-callctx.Done():
			//we failed to return something useful: error out
			cncl()
			close(done)
			return replica.Address(""), fmt.Errorf("Call canceled bevor returning calls from any client")

		case call := <-done:
			//if successfull we can stop all queries and return
			if call.Error == nil {
				cncl()
				close(done)
				return PeerID(call.Dest).String(), nil
			} else {
				calls--
			}
		}
	}

	//when we arrived here all peers are called, but none have sucessfully returned.
	//lets wait a bit longer
	for call := range done {

		if call.Error == nil {
			cncl()
			close(done)
			return PeerID(call.Dest).String(), nil
		} else {
			calls--
		}

		//check if all functions have returned with error
		if calls == 0 {
			cncl()
			close(done)
		}
	}

	return replica.Address(""), fmt.Errorf("No replica could be called successfully")

}

//Calls a function on a given replicas but does not get any return value, hence does not wait for the execution.
//As it does not wait no context is required. There is no indication if the replica was reached.
func (self *p2pTransport) Send(addr replica.Address, api string, fnc string, arguments interface{}) error {
	//individual sends are not done as event but by rpc
	peer := PeerID(string(addr))
	reply := struct{}{}
	return self.swarm.Rpc.Go(peer.pid(), "RpcEvent", fnc, arguments, &reply, nil)
}

//Calls a function on all replicas but does not get any return value, hence does not wait for the execution.
//As it does not wait no context is required. There is no indicator if the call was successfull for all replicas.
func (self *p2pTransport) SendAll(api string, fnc string, arguments interface{}) error {

	//publish as event!
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(arguments)
	if err != nil {
		return err
	}
	return self.swarm.Event.Publish(api+`/`+fnc, buf.Bytes())
}

/******************************************************************************
							Replica Types
*******************************************************************************/
type Overlord = replica.Overlord
type State = replica.State

/******************************************************************************
							shared state service
*******************************************************************************/

type sharedStateService struct {
	transport p2pTransport
	replica   *replica.Replica
}

func newSharedStateService(swarm *Swarm, overlord Overlord) *sharedStateService {

	//tansport
	trans := p2pTransport{swarm, swarm.host.ID()}

	//options
	opts := replica.DefaultOptions()

	//replica
	addr := swarm.host.ID().String()
	priv, pub := swarm.host.Keys()
	rsaPriv, ok := priv.(*crypto.RsaPrivateKey)
	if !ok {
		return nil //sharedStateService{}, fmt.Errorf("Host keys are not RSA, but is required")
	}
	rsaPub, ok := pub.(*crypto.RsaPublicKey)
	if !ok {
		return nil //sharedStateService{}, fmt.Errorf("Host keys are not RSA, but is required")
	}
	rep, err := replica.NewReplica(fmt.Sprintf("Rep_%v", addr), addr, &trans, overlord, *rsaPriv, *rsaPub, opts)
	if err != nil {
		return nil //sharedStateService{}, utils.StackError(err, "Unable to create replica for shared state")
	}
	rep.Start()

	//service
	return &sharedStateService{trans, rep}
}

func (self *sharedStateService) Share(state State) uint8 {

	return self.replica.AddState(state)
}

func (self *sharedStateService) AddCommand(ctx context.Context, state uint8, cmd []byte) error {

	return self.replica.AddCommand(ctx, state, cmd)
}

func (self *sharedStateService) Close() {

	self.replica.Stop()
}
