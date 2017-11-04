// Client
package connection

import nxclient "github.com/gammazero/nexus/client"

type Client struct {
	client *nxclient.Client
}

func (c *Client) Close() error {
	return c.client.Close()
}
