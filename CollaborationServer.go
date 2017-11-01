// CollaborationServer
package main

import (
	"fmt"
	"log"

	"github.com/gammazero/nexus/client"
	"github.com/spf13/viper"
)

type CollaborationServer struct {
	connections map[string]*client.Client
	tokens      map[string]string
}

func (cs *CollaborationServer) start(quit chan string) {

}

func (cs *CollaborationServer) stop() {

	for key, client := range cs.connections {

		err := client.Close()
		if err != nil {
			fmt.Printf("Warning: closing connection of client %s failed", key)
		}
	}
}

func (cs *CollaborationServer) hasClient(name string) bool {
	_, ok := cs.connections[name]
	return ok
}

func (cs *CollaborationServer) getClient(name string) (*client.Client, string, error) {

	client, ok := cs.connections[name]
	//if no connection established yet we create one
	if !ok {
		return nil, "", fmt.Errorf("No client \"%v\" available", name)
	}

	token, ok := cs.tokens[name]
	//if no token is available something is horribly wrong
	if !ok {
		panic(fmt.Sprintf("No token for client \"%v\" available", name))
	}

	return client, token, nil
}

func authFunc(details map[string]interface{}, data map[string]interface{}) (string, map[string]interface{}, error) {
	method, ok := data["method"].(string)
	if !ok {
		log.Fatal("no method data recieved")
	}
	if method != "ticket" {
		return "", nil, fmt.Errorf("Auth method not supported: %v", method)
	}

	fmt.Println("Authmethod for server-client called")
	fmt.Println(details)
	fmt.Println(data)

	return "joh", nil, nil
}

func (cs *CollaborationServer) connectClient(name, token string) *client.Client {

	uri := viper.GetString("server.uri")
	port := viper.GetInt("server.port")

	cfg := client.ClientConfig{
		Realm: "ocp",
	}
	c, err := client.ConnectNet(fmt.Sprintf("ws://%v:%v/ws", uri, port), cfg)
	if err != nil {
		panic(fmt.Sprintf("Unable to conenct to server %s on port %v: %s", uri, port, err))
	}

	cs.connections[name] = c
	cs.tokens[name] = token
	return c
}
