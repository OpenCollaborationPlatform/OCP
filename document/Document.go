package document

import (
	"CollaborationNode/connection"
	"context"
	"fmt"
	"sync"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/satori/go.uuid"
)

type Document struct {
	owner        *connection.Client               //the document owner
	clients      map[*connection.Client][]wamp.ID //client can have multiple sessions, but not all must have the doc open
	routerClient *nxclient.Client                 //the client with which this doc is represented on the router
	mutex        *sync.RWMutex                    //slices must be protected

	ID string
}

func NewDocument(ownerID wamp.ID, docID string) (*Document, error) {

	owner, err := server.GetClientByRouterSession(ownerID)
	if err != nil {
		return nil, err
	}

	if docID == "" {
		docID = uuid.NewV4().String()
	}

	docClient, err := router.GetLocalClient(docID)
	if err != nil {
		return nil, err
	}

	doc := Document{
		ID:           docID,
		owner:        owner,
		mutex:        &sync.RWMutex{},
		clients:      make(map[*connection.Client][]wamp.ID),
		routerClient: docClient}

	//register all docclient user functions
	errS := append([]error{}, docClient.Register(fmt.Sprintf("ocp.documents.%s.open", docID), doc.open, wamp.Dict{"disclose_caller": true}))
	errS = append(errS, docClient.Register(fmt.Sprintf("ocp.documents.%s.close", docID), doc.closeDoc, wamp.Dict{"disclose_caller": true}))

	//subscribe all needed meta events
	errS = append(errS, docClient.Subscribe("wamp.session.on_leave", doc.onSessionLeave, wamp.Dict{}))

	for _, err := range errS {
		if err != nil {
			return nil, err
		}
	}

	return &doc, nil
}

//****************************
//normal methods
//****************************

func (doc *Document) removeSession(session wamp.ID) error {

	doc.mutex.Lock()
	defer doc.mutex.Unlock()

	//remove this session, and client if needed
	for key, value := range doc.clients {
		for i, s := range value {
			if s == session {
				doc.clients[key] = append(value[:i], value[i+1:]...)
				//we may need to remove the client itself from this document
				if len(doc.clients[key]) == 0 {
					delete(doc.clients, key)

					//and even the doc itself?
					if len(doc.clients) == 0 {
						removeDoc(doc)
					}
				}
				return nil
			}
		}
	}
	return fmt.Errorf("No session with ID %s registered in document %s", session, doc.ID)
}

func (doc *Document) removeClient(client *connection.Client) {

	doc.mutex.Lock()
	defer doc.mutex.Unlock()

	//remove this client, and doc if needed
	delete(doc.clients, client)
	//and even the doc itself?
	if len(doc.clients) == 0 {
		removeDoc(doc)
	}
}

//****************************
//internal callbacks
//****************************
func (doc *Document) onSessionLeave(args wamp.List, kwargs, details wamp.Dict) {

	session, ok := args[0].(wamp.ID)
	if !ok {
		return
	}

	//maybe the session has not correclty closed the document
	doc.removeSession(session)

	return
}

//****************************
//external callbacks
//****************************
func (doc *Document) open(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

	caller := wamp.OptionID(details, "caller")
	client, err := server.GetClientByRouterSession(caller)
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Opening document failed: %s", err))}
	}

	//no double openings
	doc.mutex.RLock()
	for key, value := range doc.clients {
		if key == client {
			for _, session := range value {
				if session == caller {
					return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Document already opened"))}
				}
			}
		}
	}
	doc.mutex.RUnlock()

	//see if we are able to open the document
	_, err = server.Call(fmt.Sprintf("ocp.documents.%s.open", doc.ID), wamp.Dict{}, wamp.List{nodeID}, wamp.Dict{})
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Opening document failed: %s", err))}
	}

	doc.mutex.Lock()
	defer doc.mutex.Unlock()

	//check if we have this client already, and add the session if so
	added := false
	for key, value := range doc.clients {
		if key == client {
			doc.clients[key] = append(value, caller)
			added = true
			break
		}
	}

	if !added {
		//we  don't have the client added yet to this document
		doc.clients[client] = []wamp.ID{caller}
	}

	//register all server client functions
	//client.Register(fmt.Sprintf("ocp.documents.%s.%s.addOperation", doc.ID, nodeID))
	//client.Register(fmt.Sprintf("ocp.documents.%s.%s.removeOperation", doc.ID, nodeID))
	//client.Subscribe(fmt.Sprintf(ocp.documents.%.applyOperation", doc.ID, nodeID))

	//register all server client events

	return &nxclient.InvokeResult{}
}

func (doc *Document) closeDoc(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

	caller := wamp.OptionID(details, "caller")
	client, err := server.GetClientByRouterSession(caller)
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Opening document failed: %s", err))}
	}

	//check we if it is really open
	doc.mutex.RLock()
	isOpen := false
	for key, value := range doc.clients {
		if key == client {
			for _, session := range value {
				if session == caller {
					isOpen = true
				}
			}
		}
	}
	doc.mutex.RUnlock()

	if !isOpen {
		return &nxclient.InvokeResult{Err: wamp.URI("Document is not opened by this session")}
	}

	//see if we are able to close the document
	_, err = server.Call(fmt.Sprintf("ocp.documents.%s.close", doc.ID), wamp.Dict{}, wamp.List{nodeID}, wamp.Dict{})
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Closing document failed: %s", err))}
	}

	err = doc.removeSession(caller)
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(fmt.Sprintf("Closing document failed: %s", err))}
	}

	return &nxclient.InvokeResult{}
}
