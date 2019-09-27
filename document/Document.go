package document

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"

	nxclient "github.com/gammazero/nexus/client"
	wamp "github.com/gammazero/nexus/wamp"
)

type Document struct {

	//wamp connection
	client *nxclient.Client //the client with which this doc is represented on the router
	swarm  *p2p.Swarm

	//DML
	cid           p2p.Cid
	datastructure Datastructure
	ID            string
}

func NewDocument(ctx context.Context, router *connection.Router, host *p2p.Host, dml p2p.Cid, id string, join bool) (Document, error) {

	//lets create the folder for the document
	path := filepath.Join(host.GetPath(), id)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return Document{}, utils.StackError(err, "Unable to create folder for document")
	}

	//add the dml file
	_, err = host.Data.Write(ctx, dml, path)
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
	prefix := "ocp.documents.edit." + id
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
	doc := Document{
		client:        client,
		swarm:         swarm,
		datastructure: ds,
		ID:            id,
		cid: 		   dml,
	}

	//register all docclient user functions
	errS := []error{}
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.addPeer", doc.ID), doc.addPeer, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.setPeerAuth", doc.ID), doc.setPeerAuth, wamp.Dict{}))
	errS = append(errS, client.Register(fmt.Sprintf("ocp.documents.%s.removePeer", doc.ID), doc.removePeer, wamp.Dict{}))

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

func (self Document) addPeer(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

	return &nxclient.InvokeResult{}
}

func (self Document) setPeerAuth(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

	return &nxclient.InvokeResult{}
}

func (self Document) removePeer(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

	return &nxclient.InvokeResult{}
}
