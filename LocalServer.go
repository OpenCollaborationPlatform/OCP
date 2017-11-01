// LocalServer
package main

import (
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/router"
	"github.com/gammazero/nexus/wamp"
	"github.com/spf13/viper"
)

// this is just an example, please don't actually use it
type ForwardAuth struct {
	server *CollaborationServer
}

func (auth *ForwardAuth) Authenticate(details map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("Authenticate: %v", details)
	/*	user := details["userID"].(string)

		if auth.server.hasClient(user) {

			if auth.server.tokens[user] != signature {
				return nil, fmt.Errorf("Invalid ticket")
			}
		} else {
			auth.server.connectClient(user, signature)
		}*/
	return nil, nil
}

type LocalServer struct {
	wamp    *router.WebsocketServer
	collab  *CollaborationServer
	lclient *client.Client
}

func (ls *LocalServer) start(quit chan string) {

	routerConfig := &router.RouterConfig{
		RealmConfigs: []*router.RealmConfig{
			&router.RealmConfig{
				URI:           wamp.URI("ocp"),
				AnonymousAuth: true,
			},
		},
	}
	nxr, err := router.NewRouter(routerConfig, nil)
	s := router.NewWebsocketServer(nxr)

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

	//connect the local client
	cfg := client.ClientConfig{
		Realm: "ocp",
	}
	c, err := client.ConnectNet(fmt.Sprintf("ws://localhost:%v", viper.GetInt("connection.port")), cfg)

	if err != nil {
		fmt.Printf("Problem with local client: %s", err)
	}
	ls.lclient = c
}

func (ls *LocalServer) stop() {

	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	//if err := ls.server.Shutdown(ctx); err != nil {
	//	panic(err) // failure/timeout shutting down the server gracefully
	//}
	log.Println("Local wamp server has shut down")
}

func (ls *LocalServer) getLocalClient() *client.Client {
	return ls.lclient
}
