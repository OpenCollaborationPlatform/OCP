package user

import (
	"context"
	"fmt"
	"time"

	nxclient "github.com/gammazero/nexus/v3/client"
	hclog "github.com/hashicorp/go-hclog"

	"github.com/OpenCollaborationPlatform/OCP/connection"
	"github.com/OpenCollaborationPlatform/OCP/p2p"
	"github.com/OpenCollaborationPlatform/OCP/utils"
	wamp "github.com/gammazero/nexus/v3/wamp"
)

/* +extract
User
----
User handling is almost non-existent at the moment. Their is no user index,
identification or data handling. All there is at the moment is a way to set a
username for a node to make it easier findable for humans.
*/

type UserHandler struct {

	//connection handling
	client *nxclient.Client
	host   *p2p.Host

	//user handling
	user   UserID
	ticker *time.Ticker
}

func NewUserHandler(router *connection.Router, host *p2p.Host, logger hclog.Logger) (*UserHandler, error) {

	client, err := router.GetLocalClient("user", logger.Named("api"))
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

/* +extract
.. wamp:procedure:: ocp.users.set(name)

	Sets the user name for this node. User names are just hints at the moment,
	any node can use any name, and it is not checked for duplicates. This
	is only to make searching a node id possible.

	:param str name: Name for the given node
*/
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

/* +extract
.. wamp:procedure:: ocp.users.find(name)

	Searches the network for a node with the given user name. The name must be
	exact, including cases, there is no matching done.

	:param str name: Name to search node for
	:return: NodeID of the first found node with the given username
*/
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
