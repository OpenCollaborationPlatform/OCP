package document

import (
	"CollaborationNode/connection"
	"context"
	"fmt"
	"sync"

	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/satori/go.uuid"
)

var (
	docClient *connection.Client
	server    *connection.Server
	router    *connection.Router
	documents []*Document
	mutex     *sync.RWMutex
)

func Setup(s *connection.Server, r *connection.Router) {

	mutex = &sync.RWMutex{}
	server = s
	router = r
	client, err := r.GetLocalClient("document")
	if err != nil {
		panic(fmt.Sprintf("Could not setup document handler: %s", err))
	}
	docClient = client

	//here we create all general document related RPCs and Topic
	docClient.Register("ocp.documents.create", createDoc, wamp.Dict{"disclose_caller": true})
}

func createDoc(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *client.InvokeResult {

	caller, ok := details["caller"]
	if !ok {
		return &client.InvokeResult{Err: wamp.URI("ocp.document.no_caller_disclosed")}
	}
	id, ok := caller.(wamp.ID)
	if !ok {
		return &client.InvokeResult{Err: wamp.URI("ocp.document.wrong_argument_type")}
	}

	doc, err := NewDocument(id)
	if err != nil {
		return &client.InvokeResult{Err: wamp.URI(fmt.Sprintf("%s", err))}
	}

	mutex.Lock()
	defer mutex.Unlock()
	documents = append(documents, doc)

	return &client.InvokeResult{Args: wamp.List{doc.ID}}
}

type Document struct {
	owner         *connection.Client
	serverClients []*connection.Client
	routerClient  *connection.Client
	mutex         *sync.RWMutex

	ID uuid.UUID
}

func NewDocument(ownerID wamp.ID) (*Document, error) {

	owner, err := server.GetClientByRouterSession(ownerID)
	if err != nil {
		return nil, err
	}

	docID := uuid.NewV4()
	docClient, err := router.GetLocalClient(docID.String())
	if err != nil {
		return nil, err
	}

	return &Document{
		ID:            docID,
		owner:         owner,
		mutex:         &sync.RWMutex{},
		serverClients: make([]*connection.Client, 0),
		routerClient:  docClient}, nil
}

func (doc *Document) open(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *client.InvokeResult {

	doc.mutex.Lock()
	defer doc.mutex.Unlock()

	caller, ok := details["caller"]
	if !ok {
		return &client.InvokeResult{Err: wamp.URI("ocp.document.no_caller_disclosed")}
	}

	fmt.Printf("open document from %s\n", caller)
	fmt.Printf("args: %s\n", args)
	fmt.Printf("kwargs: %s\n", kwargs)
	fmt.Printf("details: %v\n", details)
	return &client.InvokeResult{}
}
