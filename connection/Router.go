// Router
package connection

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"context"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	nxrouter "github.com/gammazero/nexus/v3/router"
	nxserialize "github.com/gammazero/nexus/v3/transport/serialize"
	"github.com/gammazero/nexus/v3/wamp"
	"github.com/spf13/viper"
)

type Router struct {
	router nxrouter.Router
	server *http.Server
	closer io.Closer
}

func NewRouter() *Router {

	return &Router{}
}

func (ls *Router) Start(quit chan string) error {

	routerConfig := &nxrouter.Config{
		RealmConfigs: []*nxrouter.RealmConfig{
			&nxrouter.RealmConfig{
				URI:           wamp.URI("ocp"),
				AllowDisclose: true,
				AnonymousAuth: true,
			},
		},
	}
	nxr, err := nxrouter.NewRouter(routerConfig, nil)

	ls.router = nxr
	wss := nxrouter.NewWebsocketServer(nxr)

	//start connecting. We use our own listener to be sure that we really listen once this function returns
	wsAddr := fmt.Sprintf("%v:%v", viper.GetString("connection.uri"), viper.GetInt("connection.port"))
	listener, err := net.Listen("tcp", wsAddr)
	if err != nil {
	    return utils.StackError(err, "Unable to setup router: Cannot listen on %v", wsAddr)
	}
	
	//now all requests will be handled, as the listener is up. Start serving it to the router
	ls.server = &http.Server{Handler: wss}
	go func(){
		// always returns error. ErrServerClosed on graceful close
        if err := ls.server.Serve(listener); err != http.ErrServerClosed {
            // unexpected error. port in use?
            log.Fatalf("Router shut down: %v", err)
        }
	}()
	
	log.Println("Local wamp server started")

	return nil
}

func (ls *Router) Stop() {

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	ls.server.Shutdown(ctx)
	ls.router.Close()
	log.Println("Local wamp server has shut down")
}

func (ls *Router) GetLocalClient(name string) (*nxclient.Client, error) {

	//connect the local client
	authFunc := func(c *wamp.Challenge) (string, wamp.Dict) {
		return "", wamp.Dict{}
	}
	cfg := nxclient.Config{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": name, "role": "local"},
		AuthHandlers: map[string]nxclient.AuthFunc{"ticket": authFunc},
		Serialization: nxserialize.MSGPACK,
	}
	c, err := nxclient.ConnectLocal(ls.router, cfg)

	if err != nil {
		return nil, fmt.Errorf("Problem with local client: %s", err)
	}

	return c, nil

}
