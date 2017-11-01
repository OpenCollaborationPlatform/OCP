// LocalServer
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/beatgammit/turnpike"
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
	server *http.Server
	wamp   *turnpike.WebsocketServer
	collab *CollaborationServer
}

func (ls *LocalServer) start(quit chan string) {

	//setup wamp server
	s, err := turnpike.NewWebsocketServer(map[string]turnpike.Realm{
		"ocp": {
		/*Authenticators: map[string]turnpike.Authenticator{
			"authenticate": &ForwardAuth{server: ls.collab},
		},*/
		},
	})
	ls.wamp = s
	ls.server = &http.Server{
		Handler: ls.wamp,
		Addr:    fmt.Sprintf(":%v", viper.GetInt("connection.port")),
	}
	log.Printf("Local wamp server starting on port %v", viper.GetInt("connection.port"))

	go func() {
		if err := ls.server.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			quit <- err.Error()
		}
	}()

	client, err := ls.getClient()
	if err != nil {
		panic(err)
	}
	client.Subscribe("wamp.session.on_join", ls.onSessionOpened)
	client.Subscribe("wamp.session.on_leave", ls.onSessionClosed)
}

func (ls *LocalServer) stop() {

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	if err := ls.server.Shutdown(ctx); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}
	log.Println("Local wamp server has shut down")
}

func (ls *LocalServer) getClient() (*turnpike.Client, error) {

	client, err := ls.wamp.GetLocalClient("ocp", nil)
	return client, err
}

func (ls *LocalServer) onSessionOpened(args []interface{}, caller map[string]interface{}) {

	log.Printf("Session %v opened with caller %v", args, caller)

	ls.collab.connectClient("test", "123")
}

func (ls *LocalServer) onSessionClosed(args []interface{}, caller map[string]interface{}) {

	if len(args) != 1 {
		panic("No args send on connection close")
	}
	id, ok := args[0].(*turnpike.ID)
	if !ok {
		panic(fmt.Sprintf("No ID send on connection close, but %T instead", args[0]))
	}
	log.Printf("Session %v closed with caller %v", id, caller)
}
