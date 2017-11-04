// Server
package connection

import (
	"fmt"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/spf13/viper"
)

type Server struct {
	connections map[string]*Client
	tokens      map[string]string
}

func (s *Server) Start(quit chan string) {

	s.connections = make(map[string]*Client)
	s.tokens = make(map[string]string)
}

func (s *Server) Stop() {

	for key, nxclient := range s.connections {

		err := nxclient.Close()
		if err != nil {
			fmt.Printf("Warning: closing connection of nxclient %s failed", key)
		}
	}
}

func (s *Server) HasClient(name string) bool {
	_, ok := s.connections[name]
	return ok
}

func (s *Server) GetClient(name string) (*Client, string, error) {

	nxclient, ok := s.connections[name]
	//if no connection established yet we create one
	if !ok {
		return nil, "", fmt.Errorf("No nxclient \"%v\" available", name)
	}

	token, ok := s.tokens[name]
	//if no token is available something is horribly wrong
	if !ok {
		panic(fmt.Sprintf("No token for nxclient \"%v\" available", name))
	}

	return nxclient, token, nil
}

func (s *Server) ConnectClient(name, token string) (*Client, error) {

	uri := viper.GetString("server.uri")
	port := viper.GetInt("server.port")

	s.tokens[name] = token
	cfg := nxclient.ClientConfig{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": name},
		AuthHandlers: map[string]nxclient.AuthFunc{"ticket": s.authFunc},
	}
	c, err := nxclient.ConnectNet(fmt.Sprintf("ws://%v:%v/ws", uri, port), cfg)
	if err != nil {
		return nil, err
	}

	client := Client{c}
	s.connections[name] = &client
	s.tokens[name] = token
	return &client, nil
}

func (s *Server) authFunc(c *wamp.Challenge) (string, wamp.Dict) {
	/*method, ok := data["method"].(string)
	if !ok {
		log.Fatal("no method data recieved")
	}
	if method != "ticket" {
		return "", nil, fmt.Errorf("Auth method not supported: %v", method)
	}*/

	fmt.Println("Authmethod for server-nxclient called")
	fmt.Println(c)

	return "testticket", wamp.Dict{}
}
