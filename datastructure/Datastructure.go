package datastructure

import (
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	nxclient "github.com/gammazero/nexus/client"
)

//Async datastructure whcih encapsulates synchronous DML runtime and a datastore.
//It works operation based: All operations are carried out ordered
type Datastructure struct {

	//dml state handling
	dml      *dml.Runtime
	dmlState *dmlState
	
	//swarm to be used fo all relevant datastructure operations
	swarm *p2p.Swarm
	
	//the wamp client used for communication
	client *nxclient.Client
}

func NewDatastructure(ctx context.Context, dmlFiles p2p.Cid, host *p2p.Host, client *nxclient.Client) (Datastructure, error) {
	
	//load the dml file. those are not swarm specific, but global host datas
	_, err := host.Data.Write(ctx, dmlFiles, swarm.GetPath())
	if err != nil {
		return Datastructure{}, utils.StackError(err, "Unable to find the dml file") 
	}
	
	//create the state
	self.state, err = newState(swarm.GetPath())
	if err != nil {
		return Datastructure{}, utils.StackError(err, "Unable to create state for datastructure")
	}
	
	//initiate the client connections for functions and events
	wamp := wampHelper{client}
	rntm := state.dml
	rntm.SetupAllObjects(func(path string, obj dml.Data) error {
		
		//build the full path
		fpath := path + obj.Id().Name
		
		//go over all events and set them up
		wamp.SetupDmlEvents(obj, fpath)
	})
	
	
	//return the datastructure
	return Datastructure{
		dml: rntm,
		dmlState: state,
		swarm: nil,
		client: client,
	}
}

// 							Bookepping functions
// *****************************************************************************
func (self Datastructure) Close() {
	self.dmlState.Close()
}

func (self Datastructure) GetState() p2p.State {
	return self.dmlState
}

func (self Datastructure) SetSwarm(s *p2p.Swarm) {
	self.swarm = s
}


// 							Handling functions
// *****************************************************************************

func (self Datastructure) RegisterEvent() {
	
}

func (self Datastructure) UnregisterEvent() {
	
}

func (self Datastructure) CallFunction() {
	
}

func (self Datastructure) ReadProperty() {
	
}

func (self Datastructure) ExecuteCode() {
	
}