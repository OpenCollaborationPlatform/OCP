package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"reflect"
	"time"
)

//A way to specify the
type Address = string

/* Interface that allows technology agnostic inter replica communication */
type Transport interface {
	//call a replica with the given adress. Usally used for the leader. If an error occurs during the call
	//it will be returned
	Call(ctx context.Context, addr Address, fnc string, arguments interface{}, reply interface{}) error
	//call any random replica. The function should try all replicas till one does successfully execute the call
	//(no error returned). CallAny fails only if no replica can execute the called function.
	CallAny(ctx context.Context, fnc string, arguments interface{}, reply interface{}) error
	//Calls a function on all replicas but does not get any return value, hence does not wait for the execution.
	//As it does not wait no context is required.
	Send(fnc string, arguments interface{}) error
}

//A simple test transport for local replica communication. Allows randomized delay
type testTransport struct {
	delay        time.Duration
	followerAPIs map[Address]ReadAPI
}

func (self *testTransport) Call(ctx context.Context, target Address, fnc string, arguments interface{}, reply interface{}) error {

	replica, ok := self.followerAPIs[target]
	if !ok {
		return fmt.Errorf("No such replica available")
	}

	result, err := callFncByName(replica, fnc, arguments)
	if err != nil {
		return utils.StackError(err, "Unable to call replica")
	}
	reply = result[0].Interface()

	return nil
}

func (self *testTransport) CallAny(ctx context.Context, fnc string, arguments interface{}, reply interface{}) error {
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
