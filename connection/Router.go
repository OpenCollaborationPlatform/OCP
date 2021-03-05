// Router
package connection

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	nxrouter "github.com/gammazero/nexus/v3/router"
	nxserialize "github.com/gammazero/nexus/v3/transport/serialize"
	"github.com/gammazero/nexus/v3/wamp"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

type Router struct {
	router nxrouter.Router
	server *http.Server
	closer io.Closer
	logger hclog.Logger
}

func NewRouter(logger hclog.Logger) *Router {

	return &Router{logger: logger}
}

func (self *Router) Start(quit chan string) error {

	routerConfig := &nxrouter.Config{
		RealmConfigs: []*nxrouter.RealmConfig{
			&nxrouter.RealmConfig{
				URI:           wamp.URI("ocp"),
				AllowDisclose: true,
				AnonymousAuth: true,
			},
		},
	}

	stdLogger := self.logger.StandardLogger(&hclog.StandardLoggerOptions{ForceLevel: hclog.Debug})
	nxr, err := nxrouter.NewRouter(routerConfig, stdLogger)

	self.router = nxr
	wss := nxrouter.NewWebsocketServer(nxr)

	//start connecting. We use our own listener to be sure that we really listen once this function returns
	wsAddr := fmt.Sprintf("%v:%v", viper.GetString("api.uri"), viper.GetInt("api.port"))
	listener, err := net.Listen("tcp", wsAddr)
	if err != nil {
		return utils.StackError(err, "Unable to setup router: Cannot listen on %v", wsAddr)
	}

	//now all requests will be handled, as the listener is up. Start serving it to the router
	self.server = &http.Server{Handler: wss}
	go func(logger hclog.Logger, server *http.Server, listener net.Listener) {
		// always returns error. ErrServerClosed on graceful close
		if err := server.Serve(listener); err != http.ErrServerClosed {
			// unexpected error
			logger.Error("Unexpected shut down", "error", err)
		}
	}(self.logger, self.server, listener)

	self.logger.Info("Server started", "uri", viper.GetString("api.uri"), "port", viper.GetString("api.port"))

	return nil
}

func (self *Router) Stop() {

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	self.server.Shutdown(ctx)
	self.router.Close()
	self.logger.Info("Local wamp server has shut down")
}

func (self *Router) GetLocalClient(name string) (*nxclient.Client, error) {

	//connect the local client
	authFunc := func(c *wamp.Challenge) (string, wamp.Dict) {
		return "", wamp.Dict{}
	}
	cfg := nxclient.Config{
		Realm:         "ocp",
		HelloDetails:  wamp.Dict{"authid": name, "role": "local"},
		AuthHandlers:  map[string]nxclient.AuthFunc{"ticket": authFunc},
		Serialization: nxserialize.MSGPACK,
	}
	c, err := nxclient.ConnectLocal(self.router, cfg)

	if err != nil {
		return nil, fmt.Errorf("Problem with local client: %s", err)
	}

	return c, nil

}
