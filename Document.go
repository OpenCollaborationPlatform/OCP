package main

import (
	"context"
	"fmt"

	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
)

var (
	docClient *client.Client
)

type Document struct {
	serverClients []string
	routerClient  *client.Client
}

func NewDocument(name string) *Document {

	doc := Document{}
	doc.serverClients = make([]string, 0)
	//routerClient =
	return &doc
}

func setupDocumentHandler(documentClient *connection.Client) {

	//here we create all general document related RPCs and Topics
	docClient = documentClient
	docClient.Register("ocp.documents.create", createDoc, wamp.Dict{"disclose_caller": true})
}

func createDoc(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *client.InvokeResult {

	caller, ok := details["caller"]
	if !ok {
		return &client.InvokeResult{Err: wamp.URI("ocp.document.no_caller_disclosed")}
	}

	fmt.Println("Create document:")
	fmt.Printf("args: %s\n", args)
	fmt.Printf("kwargs: %s\n", kwargs)
	fmt.Printf("details: %v\n", details)
	return &client.InvokeResult{}
}
