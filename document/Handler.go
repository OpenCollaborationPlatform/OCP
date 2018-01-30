package document

import (
	"CollaborationNode/connection"
	"CollaborationNode/p2p"
	"context"
	"fmt"
	"log"
	"sync"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
)

var (
	docClient *nxclient.Client
	server    *connection.Server
	router    *connection.Router
	documents []*Document
	nodeID    p2p.PeerID
	mutex     *sync.RWMutex
)

func Setup(s *connection.Server, r *connection.Router, node p2p.PeerID) {

	mutex = &sync.RWMutex{}
	server = s
	router = r
	nodeID = node
	client, err := r.GetLocalClient("document")
	if err != nil {
		panic(fmt.Sprintf("Could not setup document handler: %s", err))
	}
	docClient = client

	//here we create all general document related RPCs and Topic
	docClient.Register("ocp.documents.create", createDoc, wamp.Dict{"disclose_caller": true})
}

func createDoc(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

	log.Print("Adding document")

	caller, ok := details["caller"]
	if !ok {
		return &nxclient.InvokeResult{Err: wamp.URI("No caller disclosed in call")}
	}
	id, ok := caller.(wamp.ID)
	if !ok {
		return &nxclient.InvokeResult{Err: wamp.URI("No caller disclosed in call")}
	}

	client, err := server.GetClientByRouterSession(id)
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("No caller available: %s", err))}
	}

	log.Print("Adding document: Data collected")

	//see if we can register that document
	result, err := server.Call("ocp.documents.create", wamp.Dict{}, wamp.List{client.AuthID}, wamp.Dict{})
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Creation failed: %s", err))}
	}

	doc, err := NewDocument(id, result.Arguments[0].(string))
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("%s", err))}
	}

	log.Print("Adding document: call made")
	mutex.Lock()
	defer mutex.Unlock()
	documents = append(documents, doc)

	log.Print("Adding document: done")
	return &nxclient.InvokeResult{Args: wamp.List{doc.ID}}
}

func removeDoc(doc *Document) {

	mutex.Lock()
	defer mutex.Unlock()
	for i, value := range documents {
		if value == doc {
			documents = append(documents[:i], documents[i+1:]...)
		}
	}
}
