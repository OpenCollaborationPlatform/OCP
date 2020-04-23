package user

import (
	"fmt"
	"time"
)

import (
	"context"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
)

/*extremely simple user handling... no authorisation, identification etc... just
  something to make finding easy!
*/
type UserHandler struct {

	//connection handling
	client *nxclient.Client
	host   *p2p.Host

	//user handling
	user   UserID
	ticker *time.Ticker
}

func NewUserHandler(router *connection.Router, host *p2p.Host) (*UserHandler, error) {

	client, err := router.GetLocalClient("user")
	if err != nil {
		return nil, utils.StackError(err, "Could not setup document handler")
	}

	uh := &UserHandler{
		client: client,
		host:   host,
		user:   UserID(""),
		ticker: time.NewTicker(20 * time.Hour),
	}

	//reannouncement or our user name!
	go func() {

		for {
			select {
			case _, more := <-uh.ticker.C:
				if !more {
					return
				}
				for {
					ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
					err := host.Provide(ctx, uh.user.Cid())
					if err == nil {
						break
					}
					time.Sleep(1 * time.Minute)
				}
			}
		}
	}()

	//here we create all general document related RPCs and Topic
	client.Register("ocp.users.set", uh.setUser, wamp.Dict{})
	client.Register("ocp.users.find", uh.findUser, wamp.Dict{})

	return uh, nil
}

func (self *UserHandler) Close() {
	self.ticker.Stop()
}

func (self *UserHandler) ID() UserID {
	return self.user
}

func (self *UserHandler) SetUser(ctx context.Context, id UserID) error {

	err := self.host.Provide(ctx, id.Cid())
	if err != nil {
		return utils.StackError(err, "Unable to set username")
	}
	self.user = id
	return nil
}

func (self *UserHandler) setUser(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be user name"}, Err: wamp.URI("ocp.error")}
	}

	name, ok := inv.Arguments[0].(string)
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be user name as string"}, Err: wamp.URI("ocp.error")}
	}

	//create user and provide!
	id := UserID(name)
	err := self.SetUser(ctx, id)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}
	return nxclient.InvokeResult{}
}

func (self *UserHandler) FindUser(ctx context.Context, id UserID, num int) (p2p.PeerID, error) {

	result, err := self.host.FindProviders(ctx, id.Cid(), num)
	if err != nil {
		return p2p.PeerID(""), err
	}
	if len(result) == 0 {
		return p2p.PeerID(""), fmt.Errorf("Unable to find user. Are both of you conencted to the network?")
	}

	return result[0], nil
}

func (self *UserHandler) findUser(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) < 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be user name, optional amount of nodes to find"}, Err: wamp.URI("ocp.error")}
	}

	name, ok := inv.Arguments[0].(string)
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be user name as string"}, Err: wamp.URI("ocp.error")}
	}

	num := 1
	if len(inv.Arguments) == 2 {
		n, ok := inv.Arguments[1].(int)
		if ok {
			num = n
		}
	}

	//create user and provide!
	id := UserID(name)
	result, err := self.FindUser(ctx, id, num)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{Args: wamp.List{result.Pretty()}}
}
