package p2p

import (
	"fmt"
	"strings"
	"sync"
)

/******************************************************************************
			Authorisation of calls to services
******************************************************************************/

//A type describing a state of authorisation
type AUTH_STATE uint

//possible states
const (
	AUTH_NONE      = AUTH_STATE(0)
	AUTH_READONLY  = AUTH_STATE(1)
	AUTH_READWRITE = AUTH_STATE(2)
)

func AuthStateFromString(state string) (AUTH_STATE, error) {

	switch strings.ToLower(state) {

	case "read", "readonly", "auth_readonly":
		return AUTH_READONLY, nil
	case "write", "readwrite", "auth_readwrite":
		return AUTH_READWRITE, nil
	case "none", "auth_none":
		return AUTH_NONE, nil
	default:
		return AUTH_NONE, fmt.Errorf("Not valid auth state string")
	}
	return AUTH_NONE, fmt.Errorf("Not valid auth state string")
}

func AuthStateToString(state AUTH_STATE) string {

	switch state {

	case AUTH_NONE:
		return "None"
	case AUTH_READWRITE:
		return "Write"
	case AUTH_READONLY:
		return "Read"
	}
	return "None"
}

//a interface that allows to query the authorisation state of a peer
//*****************************************************************
type peerAuthorizer interface {
	PeerAuth(peer PeerID) AUTH_STATE
}

//a helper type that returns the same authorisation state for each peer
//implements  peerAuthorizer
//*********************************************************************
type constantPeerAuth struct {
	state AUTH_STATE
}

func (self *constantPeerAuth) PeerAuth(peer PeerID) AUTH_STATE {
	return self.state
}

//central authorizer struct to manage authorisation requirements and states
//*************************************************************************
type authorizer struct {
	lock     sync.RWMutex
	peerAuth map[string]peerAuthorizer
	authReq  map[string]AUTH_STATE
}

func newAuthorizer() *authorizer {
	return &authorizer{peerAuth: make(map[string]peerAuthorizer), authReq: make(map[string]AUTH_STATE)}
}

func (self *authorizer) isKnown(name string) bool {
	_, ok := self.peerAuth[name]
	return ok
}

func (self *authorizer) addAuth(name string, auth_requirement AUTH_STATE, auther peerAuthorizer) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	_, has := self.peerAuth[name]
	if has {
		return fmt.Errorf("Authorizer for service already available, cannot override")
	}

	_, has = self.authReq[name]
	if has {
		return fmt.Errorf("Authorisation requirement for service already set, cannot override")
	}
	self.peerAuth[name] = auther
	self.authReq[name] = auth_requirement

	return nil
}

func (self *authorizer) getPeerAuth(name string) (peerAuthorizer, error) {

	self.lock.RLock()
	defer self.lock.RUnlock()

	auther, ok := self.peerAuth[name]
	if !ok || auther == nil {
		return nil, fmt.Errorf("No peer authorizer available for this service")
	}

	return auther, nil
}

func (self *authorizer) getRequirement(name string) (AUTH_STATE, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	req, ok := self.authReq[name]
	if !ok {
		return AUTH_NONE, fmt.Errorf("No such service registered")
	}
	return req, nil
}

func (self *authorizer) peerIsAuthorized(name string, peer PeerID) bool {

	self.lock.RLock()
	defer self.lock.RUnlock()

	//get the auth requirement for the service
	req, ok := self.authReq[name]
	if !ok {
		return false
	}

	//if everybody is allowed we are finished!
	if req == AUTH_NONE {
		return true
	}

	//Need to check in detail. Get the authstate of the peer
	auth, ok := self.peerAuth[name]
	if !ok || auth == nil {
		return false
	}
	authstate := auth.PeerAuth(peer)

	if req == AUTH_READONLY {
		//all registered peers are allowed
		return authstate == AUTH_READONLY || authstate == AUTH_READWRITE

	} else if req == AUTH_READWRITE {
		//only read write peers are allowed
		return authstate == AUTH_READWRITE
	}

	//we do not know the AUTH_STATE required... hence we don't allow anything
	return false
}
