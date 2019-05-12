package replica

import (
	"context"
)

type Follower = string

type Transport interface {
	FollowerCall(ctx context.Context, target Follower, fnc string, arguments interface{}, reply interface{}) error
	LeaderCall(ctx context.Context, target Follower, fnc string, arguments interface{}, reply interface{}) error
}
