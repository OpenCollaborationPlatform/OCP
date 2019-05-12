package replica

import (
	"CollaborationNode/utils"
	"context"
	"fmt"
	"reflect"
	"time"
)

type Follower = string

type Transport interface {
	FollowerCall(ctx context.Context, target Follower, fnc string, arguments interface{}, reply interface{}) error
	LeaderCall(ctx context.Context, target Follower, fnc string, arguments interface{}, reply interface{}) error
}

//A simple test transport for local replica communication. Allows randomized delay
type testTransport struct {
	delay        time.Duration
	followerAPIs map[Follower]FollowerAPI
}

func (self *testTransport) FollowerCall(ctx context.Context, target Follower, fnc string, arguments interface{}, reply interface{}) error {

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

func (self *testTransport) LeaderCall(ctx context.Context, target Follower, fnc string, arguments interface{}, reply interface{}) error {
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
