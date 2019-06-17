package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"time"
)

//A way to specify the
type Address = string

/* Interface that allows technology agnostic inter replica communication */
type Transport interface {

	//Register APIs to be accessbile by other replicas using their transports Call functions.
	//APIs have two different function syntaxes:
	//with return value, used for "Call" and "CallAny": 	fnc(ctx context.Context, arg Type1, result *Type2) error
	//without return value, used for "Send": 				fnc(arg Type1)
	//The register function should handle all API functions according to their structure for the call or send.
	RegisterReadAPI(api *ReadAPI) error
	RegisterWriteAPI(api *WriteAPI) error

	//call a replica with the given adress. Usally used for the leader. If an error occurs during the call
	//it will be returned
	Call(ctx context.Context, addr Address, api string, fnc string, arguments interface{}, reply interface{}) error
	//call a random replica (except self). The function should try all replicas till one does successfully execute the call
	//(no error returned). CallAny fails only if no replica can execute the called function.
	CallAny(ctx context.Context, api string, fnc string, arguments interface{}, reply interface{}) error
	//Calls a function on all replicas but does not get any return value, hence does not wait for the execution.
	//As it does not wait no context is required.
	Send(api string, fnc string, arguments interface{}) error
}

//A simple test transport for local replica communication. Allows randomized delay
type testTransport struct {
	rndDelay     time.Duration
	timeoutDelay time.Duration
	readAPIs     []*ReadAPI
	writeAPIs    []*WriteAPI
	unreachable  []int
}

func newTestTransport() *testTransport {

	return &testTransport{
		rndDelay:     0 * time.Millisecond,
		timeoutDelay: 0 * time.Millisecond,
		readAPIs:     make([]*ReadAPI, 0),
		writeAPIs:    make([]*WriteAPI, 0),
	}
}

func (self *testTransport) isReachable(idx int) bool {
	for _, val := range self.unreachable {
		if val == idx {
			return false
		}
	}
	return true
}

func (self *testTransport) RegisterReadAPI(api *ReadAPI) error {
	self.readAPIs = append(self.readAPIs, api)
	return nil
}

func (self *testTransport) RegisterWriteAPI(api *WriteAPI) error {

	self.writeAPIs = append(self.writeAPIs, api)
	return nil
}

func (self *testTransport) Call(ctx context.Context, target Address, api string, fnc string, arguments interface{}, reply interface{}) error {

	//get the address: its an int!
	idx, err := strconv.Atoi(target)
	if err != nil {
		return err
	}
	if idx >= len(self.readAPIs) {
		return fmt.Errorf("Invalid address (%v): no such replica known", idx)
	}

	//see if it is reachable
	if !self.isReachable(idx) {
		time.Sleep(self.timeoutDelay)
		return fmt.Errorf("Unable to reach: timeout")
	}

	//get the correct API to use
	var apiObj interface{}
	switch api {
	case "ReadAPI":
		apiObj = self.readAPIs[idx]
	case "WriteAPI":
		apiObj = self.writeAPIs[idx]
	default:
		return fmt.Errorf("Unknown API: don't know what to do")
	}

	//call the function
	err = callFncByName(self.rndDelay, apiObj, fnc, ctx, arguments, reply)
	if err != nil {
		return utils.StackError(err, "Unable to call replica")
	}

	return nil
}

func (self *testTransport) CallAny(ctx context.Context, api string, fnc string, argument interface{}, reply interface{}) error {

	//try each replica in random order
	idxs := rand.Perm(len(self.readAPIs))
	for _, idx := range idxs {

		//see if it is reachable
		if !self.isReachable(idx) {
			time.Sleep(self.timeoutDelay)
			return fmt.Errorf("Unable to reach: timeout")
		}

		//get the correct API to use
		var apiObj interface{}
		switch api {
		case `ReadAPI`:
			apiObj = self.readAPIs[idx]
		case `WriteAPI`:
			apiObj = self.writeAPIs[idx]
		default:
			return fmt.Errorf("Unknown API: don't know what to do")
		}

		//call the function
		err := callFncByName(self.rndDelay, apiObj, fnc, ctx, argument, reply)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("No replica could be called")
}

func (self *testTransport) Send(api string, fnc string, argument interface{}) error {

	idxs := rand.Perm(len(self.readAPIs))
	for _, idx := range idxs {

		//see if it is reachable
		if !self.isReachable(idx) {
			continue
		}

		//get the correct API to use
		var apiObj interface{}
		switch api {
		case `ReadAPI`:
			apiObj = self.readAPIs[idx]
		case `WriteAPI`:
			apiObj = self.writeAPIs[idx]
		default:
			return fmt.Errorf("Unknown API: don't know what to do")
		}

		go func(api interface{}) {
			callFncByName(self.rndDelay, api, fnc, argument)
		}(apiObj)
	}

	return nil
}

func callFncByName(delay time.Duration, obj interface{}, fncName string, params ...interface{}) error {

	if delay > 0 {
		r := rand.Intn(int(delay.Nanoseconds()))
		time.Sleep(time.Duration(r) * time.Nanosecond)
	}

	objValue := reflect.ValueOf(obj)
	method := objValue.MethodByName(fncName)
	if !method.IsValid() {
		return fmt.Errorf("Method not found \"%s\"", fncName)
	}

	in := make([]reflect.Value, len(params))
	for i, param := range params {
		in[i] = reflect.ValueOf(param)
	}
	out := method.Call(in)
	if len(out) == 0 {
		return nil
	}

	if out[0].IsNil() {
		return nil
	}

	return out[0].Interface().(error)
}
