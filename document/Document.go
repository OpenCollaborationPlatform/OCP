package document

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
)

type Document struct {

	//internals
	client *nxclient.Client //the client with which this doc is represented on the router
	swarm  *p2p.Swarm
	subs   []p2p.Subscription
	docCtx context.Context
	ctxCnl context.CancelFunc

	//DML
	cid           utils.Cid
	datastructure Datastructure
	ID            string
}

func NewDocument(ctx context.Context, router *connection.Router, host *p2p.Host, dml utils.Cid, id string, join bool) (Document, error) {

	//lets create the folder for the document
	path := filepath.Join(host.GetPath(), "Documents", id)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return Document{}, utils.StackError(err, "Unable to create folder for document")
	}

	//add the dml file
	_, err = host.Data.Write(ctx, dml, filepath.Join(path, "Dml"))
	if err != nil {
		os.RemoveAll(path)
		return Document{}, utils.StackError(err, "Unable to fetch document data description (dml file)")
	}

	//get our very own client!
	client, err := router.GetLocalClient(id)
	if err != nil {
		os.RemoveAll(path)
		return Document{}, utils.StackError(err, "Unable to connect document to wamp router")
	}

	//setup the datastructure
	prefix := "ocp.documents." + id
	ds, err := NewDatastructure(path, prefix, client)
	if err != nil {
		client.Close()
		os.RemoveAll(path)
		return Document{}, utils.StackError(err, "Unable to crete datastructure for document")
	}

	//create the p2p swarm!
	var swarm *p2p.Swarm
	if !join {
		swarm, err = host.CreateSwarmWithID(ctx, p2p.SwarmID(id), p2p.SwarmStates(ds.GetState()))

	} else {
		swarm, err = host.JoinSwarm(ctx, p2p.SwarmID(id), p2p.SwarmStates(ds.GetState()), p2p.NoPeers())
	}

	if err != nil {
		ds.Close()
		client.Close()
		os.RemoveAll(path)
		return Document{}, utils.StackError(err, "Unable to setup p2p connections for swarm")
	}

	//Startup the datastructure
	ds.Start(swarm)

	//build the document
	docCtx, ctxCnl := context.WithCancel(context.Background())
	doc := Document{
		client:        client,
		swarm:         swarm,
		datastructure: ds,
		ID:            id,
		cid:           dml,
		docCtx:        docCtx,
		ctxCnl:        ctxCnl,
	}

	errS := []error{}

	//connect to all P2P events
	errS = append(errS, doc.handleEvent("peerAdded"))
	errS = append(errS, doc.handleEvent("peerRemoved"))
	errS = append(errS, doc.handleEvent("peerAuthChanged"))
	errS = append(errS, doc.handleEvent("state.peerActivityChanged"))

	//peer handling
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.addPeer", doc.ID), doc.addPeer, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.removePeer", doc.ID), doc.removePeer, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.setPeerAuth", doc.ID), doc.setPeerAuth, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.getPeerAuth", doc.ID), doc.getPeerAuth, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.listPeers", doc.ID), doc.listPeers, wamp.Dict{}))

	options := wamp.SetOption(wamp.Dict{}, wamp.OptDiscloseCaller, true)
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.view", doc.ID), doc.view, options))

	for _, err := range errS {
		if err != nil {
			ds.Close()
			client.Close()
			os.RemoveAll(path)
			return Document{}, err
		}
	}

	return doc, nil
}

func (self Document) Close(ctx context.Context) {

	self.ctxCnl()
	for _, sub := range self.subs {
		sub.Cancel()
	}

	self.datastructure.Close()
	self.client.Close()
	self.swarm.Close(ctx)
}

func (self Document) handleEvent(topic string) error {

	sub, err := self.swarm.Event.Subscribe(topic)
	if err != nil {
		return err
	}

	self.subs = append(self.subs, sub)

	go func(sub p2p.Subscription, client *nxclient.Client, id string) {
		for {
			evt, err := sub.Next(self.docCtx)
			if err != nil {
				//subscription canceld, return
				return
			}
			topics := strings.Split(evt.Topic, ".")
			uri := fmt.Sprintf("ocp.documents.%s.%s", id, topics[len(topics)-1])
			args := make(wamp.List, len(evt.Arguments))
			for i, argument := range evt.Arguments {
				args[i] = argument
			}
			client.Publish(uri, wamp.Dict{}, args, wamp.Dict{})
		}
	}(sub, self.client, self.ID)

	return nil
}

//							Peer Handling
//******************************************************************************

func getPeer(args wamp.List) (p2p.PeerID, error) {
	//get the peer to add and the wanted AUTH state
	if len(args) < 1 {
		return p2p.PeerID(""), fmt.Errorf("First Argument must be peer id")
	}
	peer, ok := args[0].(string)
	if !ok {
		return p2p.PeerID(""), fmt.Errorf("First Argument must be peer id")
	}
	pid, err := p2p.PeerIDFromString(peer)
	if err != nil {
		return p2p.PeerID(""), fmt.Errorf("Invalid peer id provided")
	}
	return pid, nil
}
func getPeerAuthData(args wamp.List) (p2p.PeerID, p2p.AUTH_STATE, error) {

	if len(args) != 2 {
		return p2p.PeerID(""), p2p.AUTH_NONE, fmt.Errorf("Arguments must be peer id and auth state")
	}

	pid, err := getPeer(args)
	if err != nil {
		return pid, p2p.AUTH_NONE, err
	}

	auth, ok := args[1].(string)
	if !ok {
		return p2p.PeerID(""), p2p.AUTH_NONE, fmt.Errorf("Second Argument must be auth state (read, write or none)")
	}
	pidauth, err := p2p.AuthStateFromString(auth)
	if err != nil {
		return p2p.PeerID(""), p2p.AUTH_NONE, fmt.Errorf("Invalid auth state provided (muste be read, write or none)")
	}

	return pid, pidauth, nil
}

func (self Document) addPeer(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, auth, err := getPeerAuthData(inv.Arguments)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	err = self.swarm.AddPeer(ctx, pid, auth)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{}
}

func (self Document) setPeerAuth(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, auth, err := getPeerAuthData(inv.Arguments)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	err = self.swarm.ChangePeer(ctx, pid, auth)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{}
}

func (self Document) getPeerAuth(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, err := getPeer(inv.Arguments)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	auth := self.swarm.PeerAuth(pid)
	return nxclient.InvokeResult{Args: wamp.List{auth}}
}

func (self Document) removePeer(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, err := getPeer(inv.Arguments)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	err = self.swarm.RemovePeer(ctx, pid)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{}
}

func (self Document) listPeers(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {
	// returns all peers with possible sorting. Supported keyword args:
	//   - "auth": 		only peers with that auth are returned. Valid args are "Read" and "Write"
	//   - "joined": 	only peers currently joined in the document are returned. Valid args are booleans
	//
	// Note: both args can be combined

	var err error
	var peers []p2p.PeerID

	if joined, ok := inv.ArgumentsKw["joined"]; ok && joined.(bool) {

		//get all joined peers in the shared states
		peers, err = self.swarm.State.ActivePeers()
		if err != nil {
			return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
		}

	} else {
		// all peers
		peers = self.swarm.GetPeers(p2p.AUTH_NONE)
	}

	//filter out the auth if required
	if auth, ok := inv.ArgumentsKw["auth"]; ok {
		authstate, err := p2p.AuthStateFromString(auth.(string))
		if err != nil {
			return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
		}
		if authstate == p2p.AUTH_NONE {
			return nxclient.InvokeResult{Args: wamp.List{"None auth not supported. If all peers are wanted do not use auth keyword"}, Err: wamp.URI("ocp.error")}
		}

		result := make([]p2p.PeerID, 0)
		for _, peer := range peers {
			if self.swarm.PeerAuth(peer) == authstate {
				result = append(result, peer)
			}
		}
		peers = result
	}

	resargs := make([]string, len(peers))
	for i, p := range peers {
		resargs[i] = p.Pretty()
	}

	return nxclient.InvokeResult{Args: wamp.List{resargs}}
}

//							View Handling
//******************************************************************************
func (self Document) view(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	session := wamp.OptionID(inv.Details, "caller")

	if len(inv.Arguments) == 0 {
		//return if view is open or not
		return nxclient.InvokeResult{Args: wamp.List{self.datastructure.HasView(session)}}

	} else if len(inv.Arguments) > 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Single bool argument required: True for opening view, False for closing"}, Err: wamp.URI("ocp.error")}
	}

	arg, ok := wamp.AsBool(inv.Arguments[0])
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Bool argument required: True for opening view, False for closing"}, Err: wamp.URI("ocp.error")}
	}

	var err error
	if arg {
		err = self.datastructure.OpenView(session)

	} else {
		err = self.datastructure.CloseView(session)
	}

	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}
	return nxclient.InvokeResult{}
}
