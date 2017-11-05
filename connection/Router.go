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
	router  *nxrouter.Router
	wamp    *nxrouter.WebsocketServer
	server  *Server
	clients map[string]*Client
}

func NewRouter(s *Server) *Router {

	return &Router{server: s}
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
	ls.router = &nxr
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

	//we need meta events, and now we are able to make clients
	meta, err := ls.GetLocalClient("meta")
	if err != nil {
		panic(fmt.Sprintf("Unable to create meta client: %s", err))
	}
	err = meta.client.Subscribe("wamp.session.on_leave", ls.onSessionLeave, wamp.Dict{})
	if err != nil {
		panic(fmt.Sprintf("Registering leave event failed: %s", err))
	}
}

func (ls *Router) Stop() {

	//ls.router.Close()
	log.Println("Local wamp server has shut down")
}

func (ls *Router) GetLocalClient(name string) (*Client, error) {

	if ls.clients == nil {
		ls.clients = make(map[string]*Client)
	}

	c, ok := ls.clients[name]

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
		c, err := nxclient.ConnectLocal(*ls.router, cfg)

		if err != nil {
			return nil, fmt.Errorf("Problem with local client: %s", err)
		}
		client := &Client{client: c, Role: "local", AuthID: name, SessionID: c.ID()}
		ls.clients[name] = client
		return client, nil
	}

	return c, nil
}

func (ls *Router) onSessionLeave(args wamp.List, kwargs, details wamp.Dict) {

	session, ok := args[0].(wamp.ID)
	if !ok {
		return
	}

	log.Printf("Closing session %v", session)
	ls.server.RemoveRouterSession(session)
	return

}
