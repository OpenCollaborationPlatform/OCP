package document

import (
	"context"
	"sync"
	"time"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/wamp"
	hclog "github.com/hashicorp/go-hclog"
	uuid "github.com/satori/go.uuid"
)

//A p2p RPC API that allows querying some document details
type DocumentAPI struct {
	handler *DocumentHandler
}

//Allows to query the DML cid for a document ID
func (self DocumentAPI) DocumentDML(ctx context.Context, val string, ret *utils.Cid) error {

	//find the correct document
	self.handler.mutex.RLock()
	defer self.handler.mutex.RUnlock()

	for _, doc := range self.handler.documents {
		if doc.ID == val {
			*ret = doc.cid
			return nil
		}
	}
	return newUserError(Error_Operation_Invalid, "No document with given ID available")
}

//Allows to send an invitation to us for a document ID
func (self DocumentAPI) Invite(ctx context.Context, docId string, ret *bool) error {

	//find the correct document
	self.handler.mutex.Lock()
	defer self.handler.mutex.Unlock()

	//check if we have this invitation already
	*ret = true
	for _, id := range self.handler.invitations {
		if id == docId {
			return nil
		}
	}

	//a new one! add it to our list
	self.handler.invitations = append(self.handler.invitations, docId)

	//let clients know
	self.handler.client.Publish("ocp.documents.invited", wamp.Dict{}, wamp.List{docId, true}, wamp.Dict{})
	return nil
}

//Allows to send an invitation to us for a document ID
func (self DocumentAPI) Uninvite(ctx context.Context, docId string, ret *bool) error {

	//find the correct document
	self.handler.mutex.Lock()
	defer self.handler.mutex.Unlock()

	//check if we have this invitation already
	*ret = true
	for i, id := range self.handler.invitations {
		if id == docId {
			self.handler.invitations = append(self.handler.invitations[:i], self.handler.invitations[i+1:]...)

			//let clients know
			self.handler.client.Publish("ocp.documents.invited", wamp.Dict{}, wamp.List{docId, false}, wamp.Dict{})
			return nil
		}
	}

	return nil
}

type DocumentHandler struct {

	//connection handling
	client *nxclient.Client
	router *connection.Router
	host   *p2p.Host

	//document handling
	documents   []Document
	invitations []string
	mutex       *sync.RWMutex
	inviteSub   p2p.Subscription
	logger      hclog.Logger
}

func NewDocumentHandler(router *connection.Router, host *p2p.Host, logger hclog.Logger) (*DocumentHandler, error) {

	mutex := &sync.RWMutex{}
	client, err := router.GetLocalClient("document")
	if err != nil {
		return nil, utils.StackError(err, "Could not setup document handler")
	}

	//watch out for relevant events
	err = host.Event.RegisterTopic("Documents.InvitationRequest")
	if err != nil {
		return nil, utils.StackError(err, "Unable to register invite event topic")
	}
	inviteSub, err := host.Event.Subscribe("Documents.InvitationRequest")
	if err != nil {
		return nil, utils.StackError(err, "Unable to subscribe to invitation events")
	}

	dh := &DocumentHandler{
		client:      client,
		router:      router,
		host:        host,
		documents:   make([]Document, 0),
		invitations: make([]string, 0),
		mutex:       mutex,
		logger:      logger,
		inviteSub:   inviteSub,
	}

	//here we create all general document related RPCs and Topic
	client.Register("ocp.documents.create", dh.createDoc, wamp.Dict{})
	client.Register("ocp.documents.open", dh.openDoc, wamp.Dict{})
	client.Register("ocp.documents.list", dh.listDocs, wamp.Dict{})
	client.Register("ocp.documents.close", dh.closeDoc, wamp.Dict{})
	client.Register("ocp.documents.invitations", dh.invitedDocs, wamp.Dict{})
	client.Register("ocp.documents.updateInvitations", dh.searchInvitations, wamp.Dict{})

	//register the RPC api
	err = host.Rpc.Register(DocumentAPI{dh})
	err = utils.StackOnError(err, "Unable to register DocumentAPI")

	//start handling invitations
	go dh.handleInvitationRequest(inviteSub)
	go func() {
		time.Sleep(1 * time.Second)
		host.Event.Publish("Documents.InvitationRequest")
	}()

	return dh, err
}

func (self *DocumentHandler) Close(ctx context.Context) {

	//no more invites
	self.inviteSub.Cancel()

	//go over all documents and close them!
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for _, doc := range self.documents {
		doc.Close(ctx)
	}

	self.documents = make([]Document, 0)
	self.invitations = make([]string, 0)
}

func (self *DocumentHandler) CreateDocument(ctx context.Context, path string) (Document, error) {

	//add the dml folder to the data exchange!
	cid, err := self.host.Data.Add(ctx, path)
	if err != nil {
		return Document{}, utils.StackError(err, "Unable to share DML data during document creation")
	}

	//create the document
	id := uuid.NewV4().String()
	doc, err := NewDocument(ctx, self.router, self.host, cid, id, false, self.logger.Named("Document"))
	if err != nil {
		return Document{}, utils.StackError(err, "Unable to create document")
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.documents = append(self.documents, doc)

	//inform everyone about the new doc
	self.client.Publish("ocp.documents.created", wamp.Dict{}, wamp.List{doc.ID}, wamp.Dict{})

	return doc, nil
}

func (self *DocumentHandler) createDoc(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//the user needs to provide the dml folder!
	if len(inv.Arguments) != 1 {
		err := newUserError(Error_Arguments, "Argument must be path to dml folder")
		return utils.ErrorToWampResult(err)
	}
	dmlpath, ok := inv.Arguments[0].(string)
	if !ok {
		err := newUserError(Error_Arguments, "Argument must be path to dml folder")
		return utils.ErrorToWampResult(err)
	}
	if dmlpath[(len(dmlpath)-3):] != "Dml" {
		err := newUserError(Error_Operation_Invalid, "Path is not valid Dml folder (must be named Dml)")
		return utils.ErrorToWampResult(err)
	}

	doc, err := self.CreateDocument(ctx, dmlpath)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	return nxclient.InvokeResult{Args: wamp.List{doc.ID}}
}

func (self *DocumentHandler) OpenDocument(ctx context.Context, docID string) error {

	//check if already open /unlock afterwards to not lock during potenially long
	//swarm operation)
	self.mutex.RLock()
	for _, doc := range self.documents {
		if doc.ID == docID {
			return newUserError(Error_Operation_Invalid, "Document already open")
		}
	}
	self.mutex.RUnlock()

	//we know doc id == swarm id... hence use it to find an active peer!
	swarmID := p2p.SwarmID(docID)
	peer, err := self.host.FindSwarmMember(ctx, swarmID)
	if err != nil {
		return utils.StackError(err, "Unable to find swarm member for doc ID")
	}

	//ask the peer what the correct dml cid for this document is!
	var cid utils.Cid
	err = self.host.Rpc.Call(peer, "DocumentAPI", "DocumentDML", docID, &cid)
	if err != nil {
		return utils.StackError(err, "Unable to inquery dml cid for doc ID")
	}

	//create the document by joining it
	doc, err := NewDocument(ctx, self.router, self.host, cid, docID, true, self.logger.Named("Document"))
	if err != nil {
		return utils.StackError(err, "Unable to create new document")
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.documents = append(self.documents, doc)

	//inform everyone about the newly opened doc
	self.client.Publish("ocp.documents.opened", wamp.Dict{}, wamp.List{doc.ID}, wamp.Dict{})

	return nil
}

func (self *DocumentHandler) openDoc(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//the user needs to provide the doc id!
	if len(inv.Arguments) != 1 {
		err := newUserError(Error_Arguments, "Argument must be document id")
		return utils.ErrorToWampResult(err)
	}
	docID, ok := inv.Arguments[0].(string)
	if !ok {
		err := newUserError(Error_Arguments, "Argument must be document id")
		return utils.ErrorToWampResult(err)
	}

	err := self.OpenDocument(ctx, docID)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	return nxclient.InvokeResult{}
}

func (self *DocumentHandler) CloseDocument(ctx context.Context, docID string) error {

	self.mutex.Lock()
	defer self.mutex.Unlock()

	//find the document to close!
	for i, doc := range self.documents {
		if doc.ID == docID {
			doc.Close(ctx)
			self.documents = append(self.documents[:i], self.documents[i+1:]...)

			//inform everyone about the closed doc
			self.client.Publish("ocp.documents.closed", wamp.Dict{}, wamp.List{docID}, wamp.Dict{})
			return nil
		}
	}

	return newUserError(Error_Operation_Invalid, "No document for given ID found")
}

func (self *DocumentHandler) closeDoc(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//the user needs to provide the doc id!
	if len(inv.Arguments) != 1 {
		err := newUserError(Error_Arguments, "Argument must be document id")
		return utils.ErrorToWampResult(err)
	}
	docID, ok := inv.Arguments[0].(string)
	if !ok {
		err := newUserError(Error_Arguments, "Argument must be document id")
		return utils.ErrorToWampResult(err)
	}

	err := self.CloseDocument(ctx, docID)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	return nxclient.InvokeResult{}
}

func (self *DocumentHandler) ListDocuments() []string {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	res := make([]string, len(self.documents))
	for i, doc := range self.documents {
		res[i] = doc.ID
	}

	return res
}

func (self *DocumentHandler) listDocs(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		err := newUserError(Error_Arguments, "No arguments supportet")
		return utils.ErrorToWampResult(err)
	}

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	res := make([]string, len(self.documents))
	for i, doc := range self.documents {
		res[i] = doc.ID
	}

	return nxclient.InvokeResult{Args: wamp.List{res}}
}

func (self *DocumentHandler) Invitations() []string {

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	res := make([]string, len(self.invitations))
	for i, doc := range self.invitations {
		res[i] = doc
	}

	return res
}

func (self *DocumentHandler) invitedDocs(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	if len(inv.Arguments) != 0 {
		err := newUserError(Error_Arguments, "No arguments supportet")
		return utils.ErrorToWampResult(err)
	}

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	res := make([]string, len(self.invitations))
	for i, doc := range self.invitations {
		res[i] = doc
	}

	return nxclient.InvokeResult{Args: wamp.List{res}}
}

func (self *DocumentHandler) searchInvitations(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//uninvite all currently known
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for _, invitation := range self.invitations {
		self.client.Publish("ocp.documents.invited", wamp.Dict{}, wamp.List{invitation, false}, wamp.Dict{})
	}
	self.invitations = make([]string, 0)

	//request invitations
	err := self.host.Event.Publish("Documents.InvitationRequest")
	if err != nil {
		return utils.ErrorToWampResult(err)
	}
	return nxclient.InvokeResult{}
}

func (self *DocumentHandler) handleInvitationRequest(sub p2p.Subscription) {

	for {
		evt, err := sub.Next(context.Background())
		if err != nil {
			// subscription closed
			return
		}

		//we do not invite ourself
		if evt.Source == self.host.ID() {
			continue
		}

		self.logger.Debug("Invitation request received", "peer", evt.Source)

		//check if we have a document with the source as peer
		self.mutex.RLock()

		invite := make([]string, 0)
		for _, doc := range self.documents {
			peers := doc.swarm.GetPeers(p2p.AUTH_NONE)
			for _, peer := range peers {
				if peer == evt.Source {
					invite = append(invite, doc.ID)
					break
				}
			}
		}

		//send out the invites!
		for _, invite := range self.invitations {
			var ret bool
			err := self.host.Rpc.Call(evt.Source, "DocumentAPI", "Invite", invite, &ret)
			if err != nil {
				self.logger.Debug("Could not invite peer", "peer", evt.Source, "doc", invite)
			}
		}
		self.mutex.RUnlock()
	}
}
