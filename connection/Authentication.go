// LocalServer
package connection

import (
	"errors"
	"fmt"
	"time"

	"github.com/gammazero/nexus/wamp"
)

type ticketAuthenticator struct {
	timeout time.Duration
	server  *Server
}

func (ta ticketAuthenticator) AuthMethod() string {
	return "ticket"
}

func (ta ticketAuthenticator) Authenticate(sid wamp.ID, details wamp.Dict, client wamp.Peer) (*wamp.Welcome, error) {

	authid := wamp.OptionString(details, "authid")
	if authid == "" {
		return nil, errors.New("missing authid")
	}

	//local client is fine TODO: currently every client can just call itself local...
	role := wamp.OptionString(details, "role")
	if role == "local" {
		welcomeDetails := wamp.Dict{
			"authid":       authid,
			"authrole":     role,
			"authmethod":   ta.AuthMethod(),
			"authprovider": "router",
		}
		return &wamp.Welcome{Details: welcomeDetails}, nil
	}

	//get the token, either from the details or by auth func
	jwt := wamp.OptionString(details, "ticket")
	if jwt == "" {
		// Ask for the ticket
		err := client.Send(&wamp.Challenge{
			AuthMethod: ta.AuthMethod(),
			Extra:      wamp.Dict{},
		})
		if err != nil {
			return nil, fmt.Errorf("Unable to get ticket: %v", err)
		}

		// Read AUTHENTICATE response from client.
		msg, err := wamp.RecvTimeout(client, ta.timeout)
		if err != nil {
			return nil, err
		}
		authRsp, ok := msg.(*wamp.Authenticate)
		if !ok {
			return nil, fmt.Errorf("unexpected %v message received from client %v",
				msg.MessageType(), client)
		}

		jwt = authRsp.Signature
	}
	if jwt == "" {
		return nil, fmt.Errorf("No ticket received")
	}

	//connect to the server if needed
	if ta.server.HasClient(authid) {
		if ta.server.tokens[authid] != jwt {
			return nil, fmt.Errorf("Wrong ticket for multi connected client")
		}
	} else {
		_, err := ta.server.ConnectClient(authid, jwt)
		if err != nil {
			return nil, err
		}
	}

	// Create welcome details containing auth info.
	welcomeDetails := wamp.Dict{
		"authid":       authid,
		"authrole":     "collaborate",
		"authmethod":   ta.AuthMethod(),
		"authprovider": "server",
	}

	return &wamp.Welcome{Details: welcomeDetails}, nil
}
