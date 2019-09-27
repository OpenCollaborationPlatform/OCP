package document

import (
	"context"
	nxclient "github.com/gammazero/nexus/client"
	wamp "github.com/gammazero/nexus/wamp"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
	"strings"
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
	if prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
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
	rntm := self.dml
	rntm.SetupAllObjects(func(objpath string, obj dml.Data) error {

		//build the full path
		fpath := objpath + obj.Id().Name

		//go over all events and set them up
		self.setupDmlEvents(obj, fpath)
		return nil
	})

	//general options
	options := wamp.SetOption(wamp.Dict{}, wamp.OptMatch, wamp.MatchPrefix)
	options = wamp.SetOption(options, wamp.OptDiscloseCaller, true)

	//register the function handler
	uri := self.prefix + "methods"
	self.client.Register(uri, self.createWampInvokeFunction(), options)

	//register property handler
	uri = self.prefix + "properties"
	self.client.Register(uri, self.createWampPropertyFunction(), options)

	//register javascript handler
	options = wamp.SetOption(options, wamp.OptMatch, wamp.MatchExact)
	uri = self.prefix + "execute"
	self.client.Register(uri, self.createWampJSFunction(), options)
}

func (self Datastructure) Close() {
	self.dmlState.Close()
	self.client.Unregister(self.prefix + "methods")
	self.client.Unregister(self.prefix + "properties")
}

func (self Datastructure) GetState() p2p.State {
	return self.dmlState
}

//							helper functions
//******************************************************************************

//setup events so that they publish in the wamp client!
func (self Datastructure) setupDmlEvents(obj dml.EventHandler, path string) {

	//go over all events and set them up
	for _, evtName := range obj.Events() {

		evt := obj.GetEvent(evtName)
		evt.RegisterCallback(self.createWampPublishFunction(path, evtName))
	}
}

func (self Datastructure) createWampPublishFunction(path string, event string) dml.EventCallback {

	return func(vals ...interface{}) error {

		//convert the arguments into wamp style
		args := make(wamp.List, len(vals))
		for i, val := range vals {
			args[i] = val
		}

		//connvert the path into wamp style
		path = self.prefix + "events." + path + "." + event

		//other arguments we do not need
		kwargs := make(wamp.Dict, 0)
		opts := make(wamp.Dict, 0)

		//publish!
		return self.client.Publish(path, opts, args, kwargs)
	}
}

func (self Datastructure) executeOperation(ctx context.Context, op Operation) *nxclient.InvokeResult {

	//execute the operation!
	res, err := self.swarm.State.AddCommand(ctx, "dmlState", op.ToData())
	if err != nil {
		return &nxclient.InvokeResult{Err: wamp.URI(err.Error())}
	}

	//check if the returned value is an error
	if err, ok := res.(error); ok {
		return &nxclient.InvokeResult{Err: wamp.URI(err.Error())}
	}

	return &nxclient.InvokeResult{Args: wamp.List{res}}

}

func (self Datastructure) createWampInvokeFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

		//get the details: user and path of the call
		auth := wamp.OptionString(details, "caller_authid") //authid, for session id use "caller"
		procedure := wamp.OptionURI(details, "procedure")

		if auth == "" || procedure == "" {
			return &nxclient.InvokeResult{Err: wamp.URI("No authid disclosed")}
		}

		//build dml path and function
		idx := strings.LastIndex(string(procedure), ".")
		path := procedure[(len(self.prefix) + 8):idx] // 8 for methods.
		fnc := procedure[(idx + 1):]

		//build and excecute the operation arguments
		op := newFunctionOperation(dml.User(auth), string(path), string(fnc), args)
		return self.executeOperation(ctx, op)
	}

	return res
}

func (self Datastructure) createWampJSFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

		//get the user id
		auth := wamp.OptionString(details, "caller_authid") //authid, for session id use "caller"
		if auth == "" {
			return &nxclient.InvokeResult{Err: wamp.URI("No authid disclosed")}
		}

		//get the js code
		if len(args) != 1 {
			return &nxclient.InvokeResult{Err: wamp.URI("JS code needs to be provided as single argument")}
		}
		code, ok := args[0].(string)
		if !ok {
			return &nxclient.InvokeResult{Err: wamp.URI("Argument is not valid JS code")}
		}

		//build and execute the operation arguments
		op := newJsOperation(dml.User(auth), code)
		return self.executeOperation(ctx, op)
	}

	return res
}

func (self Datastructure) createWampPropertyFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

		//get the user id
		auth := wamp.OptionString(details, "caller_authid") //authid, for session id use "caller"
		if auth == "" {
			return &nxclient.InvokeResult{Err: wamp.URI("No authid disclosed")}
		}

		//get the paths
		procedure := wamp.OptionURI(details, "procedure")
		idx := strings.LastIndex(string(procedure), ".")
		path := procedure[(len(self.prefix) + 11):idx] //11 for .properties
		prop := procedure[(idx + 1):]

		//get the arguments: if provided it is a write, otherwise read
		if len(args) == 0 {
			val, err := self.dml.ReadProperty(dml.User(auth), string(path), string(prop))
			if err != nil {
				return &nxclient.InvokeResult{Err: wamp.URI(err.Error())}
			}
			return &nxclient.InvokeResult{Args: wamp.List{val}}

		} else if len(args) == 1 {
			//build and execute the operation arguments
			op := newPropertyOperation(dml.User(auth), string(path), string(prop), args[0])
			return self.executeOperation(ctx, op)
		}

		return &nxclient.InvokeResult{Err: wamp.URI("Only non or one argument supportet")}
	}

	return res
}