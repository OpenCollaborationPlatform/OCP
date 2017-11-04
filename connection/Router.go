// Router
package connection

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	nxclient "github.com/gammazero/nexus/client"
	nxrouter "github.com/gammazero/nexus/router"
	"github.com/gammazero/nexus/router/auth"
	"github.com/gammazero/nexus/wamp"
	"github.com/spf13/viper"
)

type Router struct {
	wamp     *nxrouter.WebsocketServer
	server   *Server
	lclients map[string]*nxclient.Client
}

func (ls *Router) Start(quit chan string) {

	routerConfig := &nxrouter.RouterConfig{
		RealmConfigs: []*nxrouter.RealmConfig{
			&nxrouter.RealmConfig{
				URI:            wamp.URI("ocp"),
				AllowDisclose:  true,
				Authenticators: []auth.Authenticator{ticketAuthenticator{timeout: time.Second, server: ls.server}},
			},
		},
	}
	nxr, err := nxrouter.NewRouter(routerConfig, nil)
	s := nxrouter.NewWebsocketServer(nxr)

	ls.wamp = s

	//we need a custom listener to ensure server is really ready for conections when
	//creating the local client
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", viper.GetInt("connection.port")))
	if err != nil {
		panic(fmt.Sprintf("unable to setup listener: %s", err))
	}
	log.Printf("Local wamp server successfully started on port %v", viper.GetInt("connection.port"))

	go func() {
		if err := http.Serve(listener, ls.wamp); err != nil {
			// cannot panic, because this probably is an intentional close
			quit <- err.Error()
		}
	}()
}

func (ls *Router) Stop() {

	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	//if err := ls.server.Shutdown(ctx); err != nil {
	//	panic(err) // failure/timeout shutting down the server gracefully
	//}
	log.Println("Local wamp server has shut down")
}

func (ls *Router) GetLocalClient(name string) *Client {

	if ls.lclients == nil {
		ls.lclients = make(map[string]*nxclient.Client)
	}

	c, ok := ls.lclients[name]

	if !ok {

		//connect the local client
		authFunc := func(c *wamp.Challenge) (string, wamp.Dict) {
			return "", wamp.Dict{}
		}
		cfg := nxclient.ClientConfig{
			Realm:        "ocp",
			HelloDetails: wamp.Dict{"authid": name, "role": "local"},
			AuthHandlers: map[string]nxclient.AuthFunc{"ticket": authFunc},
		}
		c, err := nxclient.ConnectNet(fmt.Sprintf("ws://localhost:%v", viper.GetInt("connection.port")), cfg)

		if err != nil {
			fmt.Printf("Problem with local client: %s", err)
			return nil
		}
		ls.lclients[name] = c
		return &Client{c}
	} else {
		return &Client{c}
	}
}
