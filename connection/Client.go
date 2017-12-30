// Client
package connection

import (
	"fmt"
	"sync"

	"github.com/gammazero/nexus/wamp"
)

type Client struct {
	AuthID string
	Role   string
	Token  string

	sessionIDs []wamp.ID
	server     *Server
	closeCB    []func(*Client)
	mutex      *sync.RWMutex
}

func MakeClient(server *Server, id string, role string, token string) *Client {

	return &Client{
		AuthID:     id,
		Role:       role,
		Token:      token,
		server:     server,
		sessionIDs: make([]wamp.ID, 0),
		closeCB:    make([]func(*Client), 0),
		mutex:      &sync.RWMutex{}}
}

func (c *Client) Close() error {

	//inform all interested parties in us closing
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	for _, cb := range c.closeCB {
		cb(c)
	}

	return nil
}

func (c *Client) AddCallbackBeforeClose(cb func(*Client)) {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.closeCB = append(c.closeCB, cb)
}

func (c *Client) HasSession(id wamp.ID) bool {

	c.mutex.RLock()
	defer c.mutex.RUnlock()
	for _, id_ := range c.sessionIDs {
		if id_ == id {
			return true
		}
	}
	return false
}

func (c *Client) AddSession(id wamp.ID) error {

	if c.HasSession(id) {
		return fmt.Errorf("Session already included")
	}

	c.mutex.Lock()
	c.sessionIDs = append(c.sessionIDs, id)
	c.mutex.Unlock()

	return nil
}

func (c *Client) RemoveSession(id wamp.ID) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, id_ := range c.sessionIDs {
		if id_ == id {
			c.sessionIDs = append(c.sessionIDs[:i], c.sessionIDs[i+1:]...)

			//TODO: remove client if last session closed
			return nil
		}
	}

	return fmt.Errorf("Sessin not available, nothing removed")
}
