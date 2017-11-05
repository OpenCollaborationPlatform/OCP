// Client
package connection

import (
	"sync"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
)

type Client struct {
	SessionID wamp.ID
	AuthID    string
	Role      string
	Token     string

	client  *nxclient.Client
	closeCB []func(*Client)
	mutex   *sync.RWMutex
}

func (c *Client) Close() error {

	//inform all interested parties in us closing
	for _, cb := range c.closeCB {
		cb(c)
	}

	return c.client.Close()
}

func (c *Client) AddCallbackBeforeClose(cb func(*Client)) {

	c.closeCB = append(c.closeCB, cb)
}

func (c *Client) Register(uri string, fn nxclient.InvocationHandler, options wamp.Dict) error {
	return c.client.Register(uri, fn, options)
}
