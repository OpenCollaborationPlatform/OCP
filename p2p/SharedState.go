package p2p

import (
	"CollaborationNode/p2p/replica"
)

type eventHandler struct {
	sub *Subscription
}

//a replica transport implementation based on events and rpcs
type swarmTransport struct {
	swarm *Swarm
	eventhandler
}

//Register APIs to be accessbile by other replicas using their transports Call functions.
//APIs have two different function syntaxes:
//with return value, used for "Call" and "CallAny": 	fnc(ctx context.Context, arg Type1, result *Type2) error
//without return value, used for "Send": 				fnc(arg Type1)
//The register function should handle all API functions according to their structure for the call or send.
func (self *swarmTransport) RegisterReadAPI(api *replica.ReadAPI) error {

	//registering rpc functions is easy!
	self.swarm.Rpc.Register(api, AUTH_READONLY)

	//now go over all event functions. We don't use reflect to keep it simple
	sub, err := self.swarm.Event.Subscribe("SharedState/NewBeacon", AUTH_READONLY)
	go func() {
		for {
			event, err := sub.Next()
			if err != nil {
				return
			}
			event.Data
		}
	}()
	api.NewBeacon()
	api.NewLog()
}

func (self *swarmTransport) RegisterWriteAPI(api *replica.WriteAPI) error {

	//registering rpc functions is easy!
	self.swarm.Rpc.Register(api, AUTH_READWRITE)
}

//call a replica with the given adress. Usally used for the leader. If an error occurs during the call
//it will be returned
func (self *replicaSwarmTransport) Call(ctx context.Context, addr replica.Address, api string, fnc string, arguments interface{}, reply interface{}) error {

}

//call a random replica (except self). The function should try all replicas till one does successfully execute the call
//(no error returned). CallAny fails only if no replica can execute the called function. It does return the Address which
//responded.
func (self *replicaSwarmTransport) CallAny(ctx context.Context, api string, fnc string, arguments interface{}, reply interface{}) (Address, error) {
}

//Calls a function on a given replicas but does not get any return value, hence does not wait for the execution.
//As it does not wait no context is required. There is no indication if the replica was reached.
func (self *replicaSwarmTransport) Send(addr replica.Address, api string, fnc string, arguments interface{}) error {
}

//Calls a function on all replicas but does not get any return value, hence does not wait for the execution.
//As it does not wait no context is required. There is no indicator if the call was successfull for all replicas.
func (self *replicaSwarmTransport) SendAll(api string, fnc string, arguments interface{}) error {
}
