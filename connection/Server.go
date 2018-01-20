// Server
package connection

import (
	"context"
	"fmt"
	"log"
	"sync"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/libp2p/go-libp2p-peer"
	"github.com/spf13/viper"
)

//WampGroup: store multiple connected registered and subscribed URIs
//******************************************************************

type wampGroup struct {
	Registered []string
	Subscribed []string
}

func (w *wampGroup) addRegistered(uri string) {
	w.Registered = append(w.Registered, uri)
}

func (w *wampGroup) addSubscribed(uri string) {
	w.Subscribed = append(w.Subscribed, uri)
}

func newWampGroup() *wampGroup {
	return &wampGroup{
		Registered: make([]string, 0),
		Subscribed: make([]string, 0)}
}

//Server: A connection to a OCP server
//************************************

type Server struct {
	nodeID     peer.ID
	connection *nxclient.Client
	clients    []*Client
	groups     map[string]*wampGroup
	mutex      *sync.RWMutex
}

func NewServer(id peer.ID) *Server {

	return &Server{
		nodeID:  id,
		mutex:   &sync.RWMutex{},
		clients: make([]*Client, 0),
		groups:  make(map[string]*wampGroup)}
}

func (s *Server) Start(quit chan string) error {

	//we setup the connection to the server
	uri := viper.GetString("server.uri")
	port := viper.GetInt("server.port")

	cfg := nxclient.ClientConfig{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": s.nodeID},
		AuthHandlers: map[string]nxclient.AuthFunc{"ticket": s.authFunc},
	}
	var adress string
	if port > 0 {
		adress = fmt.Sprintf("ws://%v:%v/ws", uri, port)
	} else {
		adress = fmt.Sprintf("ws://%v/ws", uri)
	}
	log.Printf("Connect to server: %s", adress)
	c, err := nxclient.ConnectNet(adress, cfg)
	if err != nil {
		return err
	}
	s.connection = c

	return nil
}

func (s *Server) Stop() {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, client := range s.clients {

		err := client.Close()
		if err != nil {
			fmt.Printf("Warning: closing connection of nxclient %s failed with %v", client.AuthID, err)
		}
	}

	err := s.connection.Close()
	if err != nil {
		fmt.Printf("Warning: closing connection failed with %v", err)
	}
}

func (s *Server) HasClient(name string) bool {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, client := range s.clients {
		if client.AuthID == name {
			return true
		}
	}
	return false
}

func (s *Server) GetClient(name string) (*Client, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, client := range s.clients {
		if client.AuthID == name {
			return client, nil
		}
	}
	//no connection established yet
	return nil, fmt.Errorf("No client \"%v\" available", name)
}

func (s *Server) ConnectClient(name, token string) (*Client, error) {

	if s.HasClient(name) {
		return nil, fmt.Errorf("Client already connected")
	}

	ctx := context.Background()
	_, err := s.connection.Call(ctx, "ocp.nodes.registerUser", wamp.Dict{}, wamp.List{name, token}, wamp.Dict{}, "")

	if err != nil {
		log.Printf("Connecting client %s failed: %s", name, err)
		return nil, err
	}

	//we now add the client as we successfully joined
	client := MakeClient(s, name, token, "collaborator")

	s.mutex.Lock()
	s.clients = append(s.clients, client)
	s.mutex.Unlock()

	log.Printf("Connected client %s", name)
	return client, nil
}

func (s *Server) GetClientByRouterSession(id wamp.ID) (*Client, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, client := range s.clients {
		if client.HasSession(id) {
			return client, nil
		}
	}
	return nil, fmt.Errorf("The given session ID %v is unknown\n", id)
}

func (s *Server) Register(uri string, fn nxclient.InvocationHandler, options wamp.Dict) error {
	return s.connection.Register(uri, fn, options)
}

func (s *Server) Unregister(uri string) error {
	return s.connection.Unregister(uri)
}

func (s *Server) GroupRegister(group string, uri string, fn nxclient.InvocationHandler, options wamp.Dict) error {

	s.mutex.Lock()
	wampgroup, ok := s.groups[group]
	if !ok {
		wampgroup = newWampGroup()
		s.groups[group] = wampgroup
	}
	wampgroup.addRegistered(uri)
	s.mutex.Unlock()
	return s.connection.Register(uri, fn, options)
}

func (s *Server) Call(uri string, options wamp.Dict, args wamp.List, kwargs wamp.Dict) (*wamp.Result, error) {

	ctx := context.Background()
	return s.connection.Call(ctx, uri, options, args, kwargs, "")
}

func (s *Server) Subscribe(uri string, fn nxclient.EventHandler, options wamp.Dict) error {
	return s.connection.Subscribe(uri, fn, options)
}

func (s *Server) Unsubscribe(uri string) error {
	return s.connection.Unsubscribe(uri)
}

func (s *Server) GroupSubscribe(group string, uri string, fn nxclient.EventHandler, options wamp.Dict) error {

	s.mutex.Lock()
	wampgroup, ok := s.groups[group]
	if !ok {
		wampgroup = newWampGroup()
		s.groups[group] = wampgroup
	}
	wampgroup.addSubscribed(uri)
	s.mutex.Unlock()
	return s.connection.Subscribe(uri, fn, options)
}

func (s *Server) Publish(uri string, options wamp.Dict, args wamp.List, kwargs wamp.Dict) error {
	return s.connection.Publish(uri, options, args, kwargs)
}

func (s *Server) GroupRemove(group string) error {

	s.mutex.RLock()
	wampgroup, ok := s.groups[group]
	if ok {
		for _, uri := range wampgroup.Registered {
			err := s.connection.Unregister(uri)
			if err != nil {
				return err
			}
		}
		for _, uri := range wampgroup.Subscribed {
			err := s.connection.Unsubscribe(uri)
			if err != nil {
				return err
			}
		}
	}
	s.mutex.RUnlock()
	s.mutex.Lock()
	delete(s.groups, group)
	s.mutex.Unlock()

	return nil
}

func (s *Server) authFunc(c *wamp.Challenge) (string, wamp.Dict) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	/*method, ok := data["method"].(string)
	if !ok {
		log.Fatal("no method data recieved")
	}
	if method != "ticket" {
		return "", nil, fmt.Errorf("Auth method not supported: %v", method)
	}*/

	fmt.Println("Authmethod for server-nxclient called")
	fmt.Println(c)

	return "defaultNodeTicket", wamp.Dict{}
}

func (s *Server) clientAuthFunc(ctx context.Context, args wamp.List, kwargs wamp.Dict, details wamp.Dict) *nxclient.InvokeResult {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	/*method, ok := data["method"].(string)
	if !ok {
		log.Fatal("no method data recieved")
	}
	if method != "ticket" {
		return "", nil, fmt.Errorf("Auth method not supported: %v", method)
	}*/

	fmt.Println("Authmethod for server-nxclient called")
	fmt.Println(args)

	return &nxclient.InvokeResult{Args: wamp.List{"MyUserTicket"}}
}
