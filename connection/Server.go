// Server
package connection

import (
	"fmt"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/spf13/viper"
)

type Server struct {
	connections []*Client
	sessions    map[*Client][]wamp.ID
}

func (s *Server) Start(quit chan string) {

	s.connections = make([]*Client, 0)
	s.sessions = make(map[*Client][]wamp.ID)
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

	for _, client := range s.connections {
		if client.AuthID == name {
			return true
		}
	}
	return false
}

func (s *Server) GetClient(name string) (*Client, error) {

	for _, client := range s.connections {
		if client.AuthID == name {
			return client, nil
		}
	}
	//no connection established yet
	return nil, fmt.Errorf("No client \"%v\" available", name)
}

func (s *Server) ConnectClient(name, token string) (*Client, error) {

	uri := viper.GetString("server.uri")
	port := viper.GetInt("server.port")

	//we add first to ensure token is available in auth func
	client := Client{AuthID: name, Token: token, Role: "collaborator"}
	s.connections = append(s.connections, &client)

	cfg := nxclient.ClientConfig{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": name},
		AuthHandlers: map[string]nxclient.AuthFunc{"ticket": s.authFunc},
	}
	c, err := nxclient.ConnectNet(fmt.Sprintf("ws://%v:%v/ws", uri, port), cfg)
	if err != nil {
		//we need to remove the client again...
		for i, value := range s.connections {
			if &client == value {
				s.connections = append(s.connections[:i], s.connections[i+1:]...)
				break
			}
		}
		return nil, err
	}

	client.client = c
	client.SessionID = c.ID()
	return &client, nil
}

func (s *Server) AddRouterSessionToClient(authid string, id wamp.ID) error {

	for _, value := range s.connections {
		if value.AuthID == authid {
			s.sessions[value] = append(s.sessions[value], id)
			return nil
		}
	}
	return fmt.Errorf("no client exists with given authid %s", authid)
}

func (s *Server) RemoveRouterSession(id wamp.ID) error {

	//we look for all clients, if they use the session
	for clI, client := range s.connections {

		//search the index of the session
		sessions := s.sessions[client]
		for i, session := range sessions {
			if session == id {
				// now remove it
				s.sessions[client] = append(sessions[:i], sessions[i+1:]...)

				//if sessions are empty we can close the client
				if len(s.sessions[client]) == 0 {
					client.Close()
					s.connections = append(s.connections[:clI], s.connections[clI+1:]...)
				}
				break
			}
		}
	}
	return nil
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
