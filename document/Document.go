package document

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OpenCollaborationPlatform/OCP/connection"
	"github.com/OpenCollaborationPlatform/OCP/p2p"
	"github.com/OpenCollaborationPlatform/OCP/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	hclog "github.com/hashicorp/go-hclog"
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
	logger        hclog.Logger
}

func NewDocument(ctx context.Context, router *connection.Router, host *p2p.Host, dml utils.Cid, id string, join bool, logger hclog.Logger) (Document, error) {

	//lets create the folder for the document
	path := filepath.Join(host.GetPath(), "Documents", id)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return Document{}, wrapInternalError(err, Error_Filesytem)
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
		logger:        logger,
	}

	errS := []error{}

	//connect to all P2P events
	errS = append(errS, doc.handleEvent("peerAdded"))
	errS = append(errS, doc.handleEvent("peerRemoved"))
	errS = append(errS, doc.handleEvent("peerAuthChanged"))
	errS = append(errS, doc.handleEvent("peerActivityChanged"))

	//peer handling
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.addPeer", doc.ID), doc.addPeer, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.removePeer", doc.ID), doc.removePeer, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.setPeerAuth", doc.ID), doc.setPeerAuth, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.getPeerAuth", doc.ID), doc.getPeerAuth, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.listPeers", doc.ID), doc.listPeers, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.hasMajority", doc.ID), doc.majority, wamp.Dict{}))

	options := wamp.SetOption(wamp.Dict{}, wamp.OptDiscloseCaller, true)
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.view", doc.ID), doc.view, options))

	for _, err := range errS {
		if err != nil {
			ds.Close()
			client.Close()
			os.RemoveAll(path)
			return Document{}, utils.StackError(err, "Document setup failed")
		}
	}

	return doc, nil
}

func (self Document) Close(ctx context.Context) {

	active, err := self.swarm.State.ActivePeers()
	if err == nil && len(active) == 1 {

		//we are the last peer. Uninvite everyone we can reach
		toCtx := make([]context.Context, 0)
		ret := make([]bool, 0)
		replies := make([]interface{}, 0)
		calls := make([]p2p.PeerID, 0)
		for _, peer := range self.swarm.GetPeers(p2p.AUTH_NONE) {
			if peer == self.swarm.GetHost().ID() {
				continue
			}
			if self.swarm.GetHost().IsConnected(peer) {
				calls = append(calls, peer)
				c, _ := context.WithTimeout(ctx, 1*time.Second)
				toCtx = append(toCtx, c)
				ret = append(ret, false)
				replies = append(replies, &ret[len(ret)-1])
			}
		}
		if len(calls) > 0 {
			self.swarm.GetHost().Rpc.MultiCall(toCtx, calls, "DocumentAPI", "Uninvite", self.ID, replies)
		}
	}

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
		return utils.StackError(err, "Unable to subscribe document event")
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
			self.logger.Debug("Emit event", "uri", uri, "arguments", args)
			client.Publish(uri, wamp.Dict{}, args, wamp.Dict{})
		}
	}(sub, self.client, self.ID)

	return nil
}

func (self Document) stateEventLoop(obs p2p.StateObserver) {

	//read events and do something useful with it!
	for evt := range obs.EventChannel() {

		if evt.Event == p2p.STATE_EVENT_MAJORITY_AVAILABLE {
			uri := fmt.Sprintf("ocp.documents.%s.majorityChanged", self.ID)
			self.client.Publish(uri, wamp.Dict{}, wamp.List{true}, wamp.Dict{})

		} else if evt.Event == p2p.STATE_EVENT_PEER_INACTIVE {
			uri := fmt.Sprintf("ocp.documents.%s.majorityChanged", self.ID)
			self.client.Publish(uri, wamp.Dict{}, wamp.List{false}, wamp.Dict{})
		}
	}
}

//							Peer Handling
//******************************************************************************

func getPeer(args wamp.List) (p2p.PeerID, error) {
	//get the peer to add and the wanted AUTH state
	if len(args) < 1 {
		return p2p.PeerID(""), newUserError(Error_Arguments, "First Argument must be peer id")
	}
	peer, ok := args[0].(string)
	if !ok {
		return p2p.PeerID(""), newUserError(Error_Arguments, "First Argument must be peer id")
	}
	pid, err := p2p.PeerIDFromString(peer)
	if err != nil {
		return p2p.PeerID(""), newUserError(Error_Arguments, "Invalid peer id provided")
	}
	return pid, nil
}
func getPeerAuthData(args wamp.List) (p2p.PeerID, p2p.AUTH_STATE, error) {

	if len(args) != 2 {
		return p2p.PeerID(""), p2p.AUTH_NONE, newUserError(Error_Arguments, "Arguments must be peer id and auth state")
	}

	pid, err := getPeer(args)
	if err != nil {
		return pid, p2p.AUTH_NONE, err
	}

	auth, ok := args[1].(string)
	if !ok {
		return p2p.PeerID(""), p2p.AUTH_NONE, newUserError(Error_Arguments, "Second Argument must be auth state (read, write or none)")
	}
	pidauth, err := p2p.AuthStateFromString(auth)
	if err != nil {
		return p2p.PeerID(""), p2p.AUTH_NONE, newUserError(Error_Arguments, "Invalid auth state provided (muste be read, write or none)")
	}

	return pid, pidauth, nil
}

func (self Document) addPeer(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, auth, err := getPeerAuthData(inv.Arguments)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	err = self.swarm.AddPeer(ctx, pid, auth)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	//invite the peer! (no error handling, could be offline and is still ok)
	go func() {
		err := self.swarm.GetHost().Connect(self.docCtx, pid, false)
		if err == nil {
			var ret bool
			err := self.swarm.GetHost().Rpc.CallContext(self.docCtx, pid, "DocumentAPI", "Invite", self.ID, &ret)
			if err != nil {
				self.logger.Debug("Document invite failed", "peer", pid.Pretty())
			}
		}
	}()

	return nxclient.InvokeResult{}
}

func (self Document) setPeerAuth(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, auth, err := getPeerAuthData(inv.Arguments)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	err = self.swarm.ChangePeer(ctx, pid, auth)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	return nxclient.InvokeResult{}
}

func (self Document) getPeerAuth(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, err := getPeer(inv.Arguments)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	auth := self.swarm.PeerAuth(pid)
	return nxclient.InvokeResult{Args: wamp.List{auth}}
}

func (self Document) removePeer(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	pid, err := getPeer(inv.Arguments)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	err = self.swarm.RemovePeer(ctx, pid)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	//uninvite the peer! (no error handling, could be offline and is still ok)
	go func() {
		err := self.swarm.GetHost().Connect(self.docCtx, pid, false)
		if err == nil {
			var ret bool
			err = self.swarm.GetHost().Rpc.CallContext(self.docCtx, pid, "DocumentAPI", "Uninvite", self.ID, &ret)
			if err != nil {
				self.logger.Debug("Document uninvite failed", "peer", pid.Pretty())
			}
		}
	}()

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
			return utils.ErrorToWampResult(err)
		}

	} else {
		// all peers
		peers = self.swarm.GetPeers(p2p.AUTH_NONE)
	}

	//filter out the auth if required
	if auth, ok := inv.ArgumentsKw["auth"]; ok {
		authstate, err := p2p.AuthStateFromString(auth.(string))
		if err != nil {
			return utils.ErrorToWampResult(err)
		}
		if authstate == p2p.AUTH_NONE {
			err := newUserError(Error_Operation_Invalid, "None auth not supported. If all peers are wanted do not use auth keyword")
			return utils.ErrorToWampResult(err)
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

func (self Document) majority(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		err := newUserError(Error_Arguments, "Function does not support arguments")
		return utils.ErrorToWampResult(err)
	}
	if len(inv.ArgumentsKw) != 0 {
		err := newUserError(Error_Arguments, "Function does not support arguments")
		return utils.ErrorToWampResult(err)
	}

	hasM := self.swarm.State.HasMajority()
	return nxclient.InvokeResult{Args: wamp.List{hasM}}
}

//							View Handling
//******************************************************************************
func (self Document) view(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	session := wamp.OptionID(inv.Details, "caller")

	if len(inv.Arguments) == 0 {
		//return if view is open or not
		return nxclient.InvokeResult{Args: wamp.List{self.datastructure.HasView(session)}}

	} else if len(inv.Arguments) > 1 {
		err := newUserError(Error_Arguments, "Single bool argument required (or none): True for opening view, False for closing")
		return utils.ErrorToWampResult(err)
	}

	arg, ok := wamp.AsBool(inv.Arguments[0])
	if !ok {
		err := newUserError(Error_Arguments, "Single bool argument required (or none): True for opening view, False for closing")
		return utils.ErrorToWampResult(err)
	}

	var err error
	if arg {
		err = self.datastructure.OpenView(session)

	} else {
		err = self.datastructure.CloseView(session)
	}

	if err != nil {
		return utils.ErrorToWampResult(err)
	}
	return nxclient.InvokeResult{}
}
