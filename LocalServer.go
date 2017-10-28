// LocalServer
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/spf13/viper"

	"gopkg.in/jcelliott/turnpike.v2"
)

type WampServer struct {
	server *http.Server
	wamp   *turnpike.WebsocketServer
}

func (ws *WampServer) start(quit chan string) {

	//setup wamp server
	ws.wamp = turnpike.NewBasicWebsocketServer("ocp")
	ws.server = &http.Server{
		Handler: ws.wamp,
		Addr:    fmt.Sprintf(":%v", viper.GetInt("connection.port")),
	}
	log.Printf("Local wamp server starting on port %v", viper.GetInt("connection.port"))

	go func() {
		if err := ws.server.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			quit <- err.Error()
		}
	}()
}

func (ws *WampServer) stop() {

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	if err := ws.server.Shutdown(ctx); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}
	log.Println("Local wamp server has shut down")
}

func (ws *WampServer) getClient() (*turnpike.Client, error) {

	client, err := ws.wamp.GetLocalClient("ocp", nil)
	return client, err
}
