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
	delay     time.Duration
	readAPIs  []*ReadAPI
	writeAPIs []*WriteAPI
}

func newTestTransport() *testTransport {

	return &testTransport{
		delay:     0 * time.Millisecond,
		readAPIs:  make([]*ReadAPI, 0),
		writeAPIs: make([]*WriteAPI, 0),
	}
}

func (self *testTransport) RegisterReadAPI(api *ReadAPI) error {
	if len(self.readAPIs) != len(self.writeAPIs) {
		return fmt.Errorf("Something wrong with replica data, cannot register new")
	}
	self.readAPIs = append(self.readAPIs, api)
	return nil
}

func (self *testTransport) RegisterWriteAPI(api *WriteAPI) error {

	if len(self.readAPIs) != len(self.writeAPIs) {
		return fmt.Errorf("Something wrong with replica data, cannot register new")
	}
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
		return fmt.Errorf("Invalid address: no such replica known")
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
	result, err := callFncByName(apiObj, fnc, arguments)
	if err != nil {
		return utils.StackError(err, "Unable to call replica")
	}
	reply = result[0].Interface()

	return nil
}

func (self *testTransport) CallAny(ctx context.Context, api string, fnc string, argument interface{}, reply interface{}) error {

	//try each replica in random order
	idxs := rand.Perm(len(self.readAPIs))
	for _, idx := range idxs {

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
		newctx, _ := context.WithCancel(ctx)
		result, err := callFncByName(apiObj, fnc, newctx, argument, reply)
		if err == nil {
			reply = result[0].Interface()
			return nil
		}
	}

	return fmt.Errorf("No replica couldd be called")
}

func (self *testTransport) Send(api string, fnc string, argument interface{}) error {

	idxs := rand.Perm(len(self.readAPIs))
	for _, idx := range idxs {
		go func() {
			callFncByName(self.readAPIs[idx], fnc, argument)
		}()
	}

	return nil
}

func callFncByName(obj interface{}, fncName string, params ...interface{}) (out []reflect.Value, err error) {

	objValue := reflect.ValueOf(obj)
	method := objValue.MethodByName(fncName)
	if !method.IsValid() {
		return make([]reflect.Value, 0), fmt.Errorf("Method not found \"%s\"", fncName)
	}

	in := make([]reflect.Value, len(params))
	for i, param := range params {
		in[i] = reflect.ValueOf(param)
	}
	out = method.Call(in)
	return
}