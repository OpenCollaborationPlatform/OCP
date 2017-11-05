package document

import (
	"CollaborationNode/connection"
	"context"
	"fmt"

	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/satori/go.uuid"
)

var (
	docClient *connection.Client
	server    *connection.Server
	router    *connection.Router
	documents []*Document
)

type Document struct {
	owner         *connection.Client
	serverClients []*connection.Client
	routerClient  *connection.Client

	ID uuid.UUID
}

func NewDocument(owner wamp.ID) *Document {

	doc := Document{}
	doc.ID = uuid.NewV4()

	return &doc
}

func (doc *Document) open(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *client.InvokeResult {

	caller, ok := details["caller"]
	if !ok {
		return &client.InvokeResult{Err: wamp.URI("ocp.document.no_caller_disclosed")}
	}

	fmt.Printf("open document from %s\n", caller)
	fmt.Printf("args: %s\n", args)
	fmt.Printf("kwargs: %s\n", kwargs)
	fmt.Printf("details: %v\n", details)
	return &client.InvokeResult{}

	sync
}

func Setup(s *connection.Server, r *connection.Router) {

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

	fmt.Printf("Create document from %s\n", caller)
	fmt.Printf("args: %s\n", args)
	fmt.Printf("kwargs: %s\n", kwargs)
	fmt.Printf("details: %v\n", details)
	return &client.InvokeResult{}
}
