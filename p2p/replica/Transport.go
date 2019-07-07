package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
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
	RegisterReadAPI(rpc *ReadRPCAPI, event *ReadEventAPI) error
	RegisterWriteAPI(rpc *WriteRPCAPI, event *WriteEventAPI) error

	//call a replica with the given adress. Usally used for the leader. If an error occurs during the call
	//it will be returned
	Call(ctx context.Context, addr Address, api string, fnc string, arguments interface{}, reply interface{}) error
	//call a random replica (except self). The function should try all replicas till one does successfully execute the call
	//(no error returned). CallAny fails only if no replica can execute the called function. It does return the Address which
	//responded.
	CallAny(ctx context.Context, api string, fnc string, arguments interface{}, reply interface{}) (Address, error)

	//Calls a function on a given replicas but does not get any return value, hence does not wait for the execution.
	//As it does not wait no context is required. There is no indication if the replica was reached.
	Send(addr Address, api string, fnc string, arguments interface{}) error
	//Calls a function on all replicas but does not get any return value, hence does not wait for the execution.
	//As it does not wait no context is required. There is no indicator if the call was successfull for all replicas.
	SendAll(api string, fnc string, arguments interface{}) error
}

//A simple test transport for local replica communication. Allows randomized delay
type testTransport struct {
	rndDelay       time.Duration
	timeoutDelay   time.Duration
	readRPCAPIs    []*ReadRPCAPI
	readEventAPIs  []*ReadEventAPI
	writeRPCAPIs   []*WriteRPCAPI
	writeEventAPIs []*WriteEventAPI
	unreachable    []int
	mutex          sync.RWMutex //for unreachable
}

func newTestTransport() *testTransport {

	return &testTransport{
		rndDelay:       0 * time.Millisecond,
		timeoutDelay:   0 * time.Millisecond,
		readRPCAPIs:    make([]*ReadRPCAPI, 0),
		readEventAPIs:  make([]*ReadEventAPI, 0),
		writeRPCAPIs:   make([]*WriteRPCAPI, 0),
		writeEventAPIs: make([]*WriteEventAPI, 0),
	}
}

func (self *testTransport) isReachable(idx int) bool {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	for _, val := range self.unreachable {
		if val == idx {
			return false
		}
	}
	return true
}

func (self *testTransport) RegisterReadAPI(rpc *ReadRPCAPI, event *ReadEventAPI) error {
	self.readRPCAPIs = append(self.readRPCAPIs, rpc)
	self.readEventAPIs = append(self.readEventAPIs, event)
	return nil
}

func (self *testTransport) RegisterWriteAPI(rpc *WriteRPCAPI, event *WriteEventAPI) error {

	self.writeEventAPIs = append(self.writeEventAPIs, event)
	self.writeRPCAPIs = append(self.writeRPCAPIs, rpc)
	return nil
}

func (self *testTransport) Call(ctx context.Context, target Address, api string, fnc string, arguments interface{}, reply interface{}) error {

	//get the address: its an int!
	idx, err := strconv.Atoi(target)
	if err != nil {
		return err
	}
	if idx >= len(self.readRPCAPIs) {
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
	case "ReadRPCAPI":
		apiObj = self.readRPCAPIs[idx]
	case "WriteRPCAPI":
		apiObj = self.writeRPCAPIs[idx]
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

func (self *testTransport) CallAny(ctx context.Context, api string, fnc string, argument interface{}, reply interface{}) (Address, error) {

	//try each replica in random order
	idxs := rand.Perm(len(self.readRPCAPIs))
	for _, idx := range idxs {

		//see if it is reachable. If not we pretend to wait for the timeout 100ms
		//bevore trying the next one
		if !self.isReachable(idx) {
			time.Sleep(100 * time.Millisecond)
		}

		//get the correct API to use
		var apiObj interface{}
		switch api {
		case `ReadRPCAPI`:
			apiObj = self.readRPCAPIs[idx]
		case `WriteRPCAPI`:
			apiObj = self.writeRPCAPIs[idx]
		default:
			return Address(""), fmt.Errorf("Unknown API: don't know what to do")
		}

		//call the function
		err := callFncByName(self.rndDelay, apiObj, fnc, ctx, argument, reply)
		if err == nil {
			return fmt.Sprintf("%v", idx), nil
		}
	}

	return Address(""), fmt.Errorf("No replica could be called")
}

func (self *testTransport) Send(addr Address, api string, fnc string, arguments interface{}) error {

	//get the address: its an int!
	idx, err := strconv.Atoi(addr)
	if err != nil {
		return err
	}
	if idx >= len(self.readEventAPIs) {
		return fmt.Errorf("Invalid address (%v): no such replica known", idx)
	}

	//see if it is reachable
	if !self.isReachable(idx) {
		return nil
	}

	//get the correct API to use
	var apiObj interface{}
	switch api {
	case "ReadEventAPI":
		apiObj = self.readEventAPIs[idx]
	case "WriteEventAPI":
		apiObj = self.writeEventAPIs[idx]
	default:
		return fmt.Errorf("Unknown API: don't know what to do")

	}

	go func(api interface{}) {
		//call the function
		callFncByName(self.rndDelay, api, fnc, arguments)

	}(apiObj)

	return nil
}

func (self *testTransport) SendAll(api string, fnc string, argument interface{}) error {

	idxs := rand.Perm(len(self.readEventAPIs))
	for _, idx := range idxs {

		//see if it is reachable
		if !self.isReachable(idx) {
			continue
		}

		//get the correct API to use
		var apiObj interface{}
		switch api {
		case `ReadEventAPI`:
			apiObj = self.readEventAPIs[idx]
		case `WriteEventAPI`:
			apiObj = self.writeEventAPIs[idx]
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
