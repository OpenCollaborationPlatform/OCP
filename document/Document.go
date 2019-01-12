package document

import (
	"CollaborationNode/connection"
	"context"
	"fmt"
	"sync"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	uuid "github.com/satori/go.uuid"
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
		ID := uuid.NewV4()
		docID = ID.String()
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

	//register all required server functions

	//subscribe all server document events
	errS = append(errS, doc.forwardServerEvent("opened"))
	errS = append(errS, doc.forwardServerEvent("closed"))

	for _, err := range errS {
		if err != nil {
			docClient.Close()
			server.GroupRemove(doc.ID)
			return nil, err
		}
	}

	return &doc, nil
}

//****************************
//normal methods
//****************************

//this function forwards server events to the router
func (doc *Document) forwardServerEvent(event string) error {

	handler := func(args wamp.List, kwargs, details wamp.Dict) {
		doc.routerClient.Publish(fmt.Sprintf("ocp.documents.%s.%s", doc.ID, event), wamp.Dict{}, args, kwargs)
	}
	return server.GroupSubscribe(doc.ID, fmt.Sprintf("ocp.documents.%s.%s", doc.ID, event), handler, wamp.Dict{})
}

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
						doc.routerClient.Close()
						removeDoc(doc)
					}
				}
				return nil
			}
		}
	}
	return fmt.Errorf("No session with ID %v registered in document %v", session, doc.ID)
}

func (doc *Document) removeClient(client *connection.Client) {

	doc.mutex.Lock()
	defer doc.mutex.Unlock()

	//remove this client, and doc if needed
	delete(doc.clients, client)
	//and even the doc itself?
	if len(doc.clients) == 0 {
		doc.routerClient.Close()
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

//close the document, no matter if we have anyone conencted anymore
func (doc *Document) onRemoved(args wamp.List, kwargs, details wamp.Dict) {

	doc.routerClient.Publish(fmt.Sprintf("ocp.documents.%s.removed", doc.ID), wamp.Dict{}, wamp.List{}, wamp.Dict{})
	doc.routerClient.Close()
	removeDoc(doc)

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
