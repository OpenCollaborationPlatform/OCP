package document

import (
	"context"
	"fmt"
	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
	"sync"

	nxclient "github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/wamp"
	uuid "github.com/satori/go.uuid"
)

//A p2p RPC API that allows querying some document details
type DocumentAPI struct {
	handler *DocumentHandler
}

func (self DocumentAPI) DocumentDML(ctx context.Context, val string, ret *p2p.Cid) error {

	//find the correct document
	self.handler.mutex.RLock()
	defer self.handler.mutex.RUnlock()

	for _, doc := range self.handler.documents {
		if doc.ID == val {
			*ret = doc.cid
			return nil
		}
	}
	return fmt.Errorf("No document with given ID available")
}

type DocumentHandler struct {

	//connection handling
	client *nxclient.Client
	router *connection.Router
	host   *p2p.Host

	//document handling
	documents []Document
	mutex     *sync.RWMutex
}

func NewDocumentHandler(router *connection.Router, host *p2p.Host) (*DocumentHandler, error) {

	mutex := &sync.RWMutex{}
	client, err := router.GetLocalClient("document")
	if err != nil {
		return nil, utils.StackError(err, "Could not setup document handler")
	}

	dh := &DocumentHandler{
		client:    client,
		router:    router,
		host:      host,
		documents: make([]Document, 0),
		mutex:     mutex,
	}

	//here we create all general document related RPCs and Topic
	client.Register("ocp.documents.create", dh.createDoc, wamp.Dict{})
	client.Register("ocp.documents.open", dh.openDoc, wamp.Dict{})
	client.Register("ocp.documents.list", dh.listDocs, wamp.Dict{})
	client.Register("ocp.documents.close", dh.closeDoc, wamp.Dict{})

	//register the RPC api
	host.Rpc.Register(DocumentAPI{dh})

	return dh, nil
}

func (self *DocumentHandler) Close(ctx context.Context) {

	//go over all documents nd close them!
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for _, doc := range self.documents {
		doc.Close(ctx)
	}

	self.documents = make([]Document, 0)
}

func (self *DocumentHandler) CreateDocument(ctx context.Context, path string) (Document, error) {

	//add the dml folder to the data exchange!
	cid, err := self.host.Data.Add(ctx, path)
	if err != nil {
		return Document{}, utils.StackError(err, "Unable to share DML data during document creation")
	}

	//create the document
	id := uuid.NewV4().String()
	doc, err := NewDocument(ctx, self.router, self.host, cid, id, false)
	if err != nil {
		return Document{}, utils.StackError(err, "Unable to create document")
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.documents = append(self.documents, doc)

	//inform everyone about the new doc... p2p and locally!
	self.client.Publish("ocp.documents.created", wamp.Dict{}, wamp.List{doc.ID}, wamp.Dict{})
	self.host.Event.Publish("ocp.documents.created", []byte(doc.ID))

	return doc, nil
}

func (self *DocumentHandler) createDoc(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//the user needs to provide the dml folder!
	if len(inv.Arguments) != 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be path to dml folder"}, Err: wamp.URI("ocp.error")}
	}
	dmlpath, ok := inv.Arguments[0].(string)
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be path to dml folder"}, Err: wamp.URI("ocp.error")}
	}
	if dmlpath[(len(dmlpath)-3):] != "Dml" {
		return nxclient.InvokeResult{Args: wamp.List{"Path is not valid Dml folder"}, Err: wamp.URI("ocp.error")}
	}

	doc, err := self.CreateDocument(ctx, dmlpath)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{Args: wamp.List{doc.ID}}
}

func (self *DocumentHandler) OpenDocument(ctx context.Context, docID string) error {

	//check if already open /unlocka afterwards to not lock during potenially long
	//swarm operation)
	self.mutex.RLock()
	for _, doc := range self.documents {
		if doc.ID == docID {
			return fmt.Errorf("Document already open")
		}
	}
	self.mutex.RUnlock()

	//we know doc id == swarm id... hence use it to find an active peer!
	swarmID := p2p.SwarmID(docID)
	peer, err := self.host.FindSwarmMember(ctx, swarmID)
	if err != nil {
		return fmt.Errorf("No document with id found: " + err.Error())
	}

	//ask the peer what the correct dml cid for this document is!
	var cid p2p.Cid
	err = self.host.Rpc.Call(peer, "DocumentAPI", "DocumentDML", docID, &cid)
	if err != nil {
		return fmt.Errorf("No dml description for document found: " + err.Error())
	}

	//create the document by joining it
	doc, err := NewDocument(ctx, self.router, self.host, cid, docID, true)
	if err != nil {
		return err
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.documents = append(self.documents, doc)

	//inform everyone about the newly opened doc... p2p and locally!
	self.client.Publish("ocp.documents.opened", wamp.Dict{}, wamp.List{doc.ID}, wamp.Dict{})
	self.host.Event.Publish("ocp.documents.opened", []byte(doc.ID))

	return nil
}

func (self *DocumentHandler) openDoc(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//the user needs to provide the doc id!
	if len(inv.Arguments) != 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be document id"}, Err: wamp.URI("ocp.error")}
	}
	docID, ok := inv.Arguments[0].(string)
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be document id"}, Err: wamp.URI("ocp.error")}
	}

	err := self.OpenDocument(ctx, docID)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
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

			//inform everyone about the closed doc... p2p and locally!
			self.client.Publish("ocp.documents.closed", wamp.Dict{}, wamp.List{docID}, wamp.Dict{})
			self.host.Event.Publish("ocp.documents.closed", []byte(docID))

			return nil
		}
	}

	return fmt.Errorf("No document for given ID found")
}

func (self *DocumentHandler) closeDoc(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

	//the user needs to provide the doc id!
	if len(inv.Arguments) != 1 {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be document id"}, Err: wamp.URI("ocp.error")}
	}
	docID, ok := inv.Arguments[0].(string)
	if !ok {
		return nxclient.InvokeResult{Args: wamp.List{"Argument must be document id"}, Err: wamp.URI("ocp.error")}
	}

	err := self.CloseDocument(ctx, docID)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
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
		return nxclient.InvokeResult{Args: wamp.List{"Function does not take arguments"}, Err: wamp.URI("ocp.error")}
	}

	self.mutex.RLock()
	defer self.mutex.RUnlock()

	res := make(wamp.List, len(self.documents))
	for i, doc := range self.documents {
		res[i] = doc.ID
	}

	return nxclient.InvokeResult{Args: res}
}
