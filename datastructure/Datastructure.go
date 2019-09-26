package datastructure

import (
	nxclient "github.com/gammazero/nexus/client"
	wamp "github.com/gammazero/nexus/wamp"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
)

//Async datastructure whcih encapsulates synchronous DML runtime and a datastore.
//It works operation based: All operations are carried out ordered
type Datastructure struct {

	//general
	path   string
	prefix string

	//dml state handling
	dml      *dml.Runtime
	dmlState dmlState

	//connectvity: p2p and client
	swarm  *p2p.Swarm
	client *nxclient.Client
}

/* Creates a new shared dml datastructure and exposes it to wamp. 
 * - The dml folder needs to be found in "path", and the datastore will be 
 * 	 created there too.
 * - Prefix is the wamp URI prefix the datastructure uses. After this prefix all
 *   datastrucutre events are created in /events/... and all methods in /methods/...
 */
func NewDatastructure(path string, prefix string, client *nxclient.Client) (Datastructure, error) {

	//create the state
	state, err := newState(path)
	if err != nil {
		return Datastructure{}, utils.StackError(err, "Unable to create state for datastructure")
	}
	
	//make sure the prefix has a "/" as last charachter
	if prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}

	//return the datastructure
	return Datastructure{
		path:     path,
		prefix:   prefix,
		dml:      state.dml,
		dmlState: state,
		swarm:    nil,
		client:   client,
	}, nil
}

// 							Bookepping functions
// *****************************************************************************
func (self Datastructure) Start(s *p2p.Swarm) {
	self.swarm = s

	//initiate the client connections for events
	wh := wampHelper{self.client, s, self.prefix}
	rntm := self.dml
	rntm.SetupAllObjects(func(objpath string, obj dml.Data) error {

		//build the full path
		fpath := objpath + obj.Id().Name

		//go over all events and set them up
		wh.SetupDmlEvents(obj, fpath)
		return nil
	})

	//general options
	options := wamp.SetOption(wamp.Dict{}, wamp.OptMatch, wamp.MatchPrefix)
	options =  wamp.SetOption(options, wamp.OptDiscloseCaller, true)

	//register the function handler
	uri := self.prefix + "methods/"
	self.client.Register(uri, wh.createWampInvokeFunction(), options)

	//register javascript handler
	uri = self.prefix + "execute"
	self.client.Register(uri, wh.createWampJSFunction(), options)
}

func (self Datastructure) Close() {
	self.dmlState.Close()
	self.client.Unregister(self.prefix + "/methods/")
}

func (self Datastructure) GetState() p2p.State {
	return self.dmlState
}
