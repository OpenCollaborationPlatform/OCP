// Router
package connection

import (
	"fmt"
	"io"
	"log"

	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/client"
	nxrouter "github.com/gammazero/nexus/router"
	"github.com/gammazero/nexus/wamp"
	"github.com/spf13/viper"
)

type Router struct {
	router nxrouter.Router
	wamp   *nxrouter.WebsocketServer
	closer io.Closer
}

func NewRouter() *Router {

	return &Router{}
}

func (ls *Router) Start(quit chan string) error {

	routerConfig := &nxrouter.RouterConfig{
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
	ls.wamp = wss

	wsAddr := fmt.Sprintf("%v:%v", viper.GetString("connection.uri"), viper.GetInt("connection.port"))
	wsCloser, err := wss.ListenAndServe(wsAddr)
	if err != nil {
		return utils.StackError(err, "Unable to setup router")
	}

	ls.closer = wsCloser
	return nil
}

func (ls *Router) Stop() {

	ls.closer.Close()
	ls.router.Close()
	log.Println("Local wamp server has shut down")
}

func (ls *Router) GetLocalClient(name string) (*nxclient.Client, error) {

	//connect the local client
	authFunc := func(c *wamp.Challenge) (string, wamp.Dict) {
		return "", wamp.Dict{}
	}
	cfg := nxclient.ClientConfig{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": name, "role": "local"},
		AuthHandlers: map[string]nxclient.AuthFunc{"ticket": authFunc},
	}
	c, err := nxclient.ConnectLocal(ls.router, cfg)

	if err != nil {
		return nil, fmt.Errorf("Problem with local client: %s", err)
	}

	return c, nil

}
