package document

import (
	"context"
	"encoding/gob"
	"fmt"
	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
	cid "github.com/ipfs/go-cid"
	"strings"
)

func init() {
	//a few types that may be a valid wamp arguments
	gob.Register(new([]interface{}))
	gob.Register(new(map[string]interface{}))
	gob.Register(new(map[int64]interface{}))
}

//Async datastructure which encapsulates synchronous DML runtime and a datastore.
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

	//make sure the prefix has a "." as last charachter
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
		if objpath != "" {
			objpath += "."
		}
		objpath += obj.Id().Name

		//go over all events and set them up
		self.setupDmlEvents(obj, objpath)
		return nil
	})

	//general options
	options := wamp.SetOption(wamp.Dict{}, wamp.OptMatch, wamp.MatchPrefix)
	options = wamp.SetOption(options, wamp.OptDiscloseCaller, true)

	//register the function handler
	uri := self.prefix + "call"
	self.client.Register(uri, self.createWampInvokeFunction(), options)

	//register raw data handler
	uri = self.prefix + "rawdata"
	self.client.Register(uri, self.createWampRawFunction(), options)

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
		uri := self.prefix + "events." + path + "." + event

		//other arguments we do not need
		kwargs := make(wamp.Dict, 0)
		opts := make(wamp.Dict, 0)

		//check if we should omit a session!
		session := self.dmlState.GetOperationSession()
		if session.IsSet() && session.Node == self.swarm.GetHost().ID() {
			opts["exclude"] = []wamp.ID{session.Session}
		}

		//publish!
		//fmt.Printf("Publish event %v with args %v\n", uri, args)
		return self.client.Publish(uri, opts, args, kwargs)
	}
}

func (self Datastructure) executeOperation(ctx context.Context, op Operation) nxclient.InvokeResult {

	//execute the operation!
	data, err := op.ToData()
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{utils.StackError(err, "Unable to parse operation for shared state").Error()}, Err: wamp.URI("ocp.error")}
	}

	res, err := self.swarm.State.AddCommand(ctx, "dmlState", data)
	if err != nil {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	//check if the returned value is an error
	if err, ok := res.(error); ok {
		return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
	}

	return nxclient.InvokeResult{Args: wamp.List{res}}
}

func (self Datastructure) createWampInvokeFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

		//get session info
		node := self.swarm.GetHost().ID()
		session := wamp.OptionID(inv.Details, "caller")

		//get the user and path of the call
		auth := wamp.OptionString(inv.Details, "caller_authid") //authid, for session id use "caller"
		procedure := wamp.OptionURI(inv.Details, "procedure")

		if auth == "" || procedure == "" {
			return nxclient.InvokeResult{Args: wamp.List{"No authid disclosed"}, Err: wamp.URI("ocp.error")}
		}

		//build dml path
		path := procedure[(len(self.prefix) + 5):] // 5 for call.

		//check if we execute local or globally
		local, err := self.dmlState.CanCallLocal(string(path))
		if err != nil {
			return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
		}
		if local {
			res, err := self.dmlState.CallLocal(dml.User(auth), string(path), inv.Arguments...)
			if err != nil {
				return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
			}
			return nxclient.InvokeResult{Args: wamp.List{res}}
		}

		//build and excecute the operation arguments
		op := newCallOperation(dml.User(auth), string(path), inv.Arguments, node, session)
		return self.executeOperation(ctx, op)
	}

	return res
}

func (self Datastructure) createWampJSFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

		node := self.swarm.GetHost().ID()
		session := wamp.OptionID(inv.Details, "caller")

		//get the user id
		auth := wamp.OptionString(inv.Details, "caller_authid") //authid, for session id use "caller"
		if auth == "" {
			return nxclient.InvokeResult{Args: wamp.List{"No authid disclosed"}, Err: wamp.URI("ocp.error")}
		}

		//get the js code
		if len(inv.Arguments) != 1 {
			return nxclient.InvokeResult{Args: wamp.List{"JS code needs to be provided as single argument"}, Err: wamp.URI("ocp.error")}
		}
		code, ok := inv.Arguments[0].(string)
		if !ok {
			return nxclient.InvokeResult{Args: wamp.List{"Argument is not valid JS code"}, Err: wamp.URI("ocp.error")}
		}

		//build and execute the operation arguments
		op := newJsOperation(dml.User(auth), code, node, session)
		return self.executeOperation(ctx, op)
	}

	return res
}

func (self Datastructure) cidByBinary(ctx context.Context, inv *wamp.Invocation) (p2p.Cid, error) {

	//as progressive results are only possible when calling, not when
	//get called, we call back and get the data progressivly
	//the arguments is the uri to call, and the arg to identify the data!
	if len(inv.Arguments) != 2 {
		return p2p.Cid{}, fmt.Errorf("Arguments must be URI to call for progressive data and the argsument for that uri")
	}
	uri, ok := inv.Arguments[0].(string)
	if !ok {
		return p2p.Cid{}, fmt.Errorf("First argument must be URI")
	}
	arg := inv.Arguments[1] //Arg is allowed to be everything

	//make the progressive call!
	block := p2p.NewStreamBlockifyer()
	channel := block.Start()
	callback := func(result *wamp.Result) {
		//get the data block
		if len(result.Arguments) != 1 {
			fmt.Printf("Progress called but no data in %v\n", result)
			return
		}

		res := result.Arguments[0]
		switch res.(type) {
		case []byte:
			channel <- res.([]byte)
		case string:
			channel <- []byte(res.(string))
		}
	}
	res, err := self.client.Call(ctx, uri, wamp.Dict{}, wamp.List{arg}, wamp.Dict{}, callback)
	if err != nil {
		return p2p.Cid{}, err
	}
	if len(res.Arguments) > 0 && res.Arguments[0] != nil {

		data := res.Arguments[0]
		switch data.(type) {
		case []byte:
			channel <- data.([]byte)
		case string:
			channel <- []byte(data.(string))
		default:
			msg := fmt.Sprintf("Binary data not received as binary but %T", res.Arguments[0])
			return p2p.Cid{}, fmt.Errorf(msg)
		}
	}
	block.Stop()
	if err != nil {
		msg := utils.StackError(err, "Cannot call provided URI").Error()
		return p2p.Cid{}, fmt.Errorf(msg)
	}

	//add the data we received
	cid, err := self.swarm.Data.AddBlocks(ctx, block)
	if err != nil {
		msg := utils.StackError(err, "Unable to distribute data").Error()
		return p2p.Cid{}, fmt.Errorf(msg)
	}
	
	return cid, nil
}

func (self Datastructure) createWampRawFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {

		//get session info
		node := self.swarm.GetHost().ID()
		session := wamp.OptionID(inv.Details, "caller")

		//get the user id
		auth := wamp.OptionString(inv.Details, "caller_authid") //authid, for session id use "caller"
		if auth == "" {
			return nxclient.InvokeResult{Args: wamp.List{"No authid disclosed"}, Err: wamp.URI("ocp.error")}
		}

		//get the paths
		procedure := string(wamp.OptionURI(inv.Details, "procedure"))
		procedure = procedure[(len(self.prefix)+8):]

		if !strings.Contains(procedure, ".") {
			//generall, non-object raw methods

			switch procedure {
				
			case "CidByBinary":
				//get the cid
				cid, err := self.cidByBinary(ctx, inv)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				//return the cid (no operation started, this is just to setup the data!
				return nxclient.InvokeResult{Args: wamp.List{cid.String()}}
				
			case "BinaryByCid":
				
				if len(inv.Arguments) != 1 {
					return nxclient.InvokeResult{Args: wamp.List{"Argument must be Cid"}, Err: wamp.URI("ocp.error")}
				}
				strcid, ok := inv.Arguments[0].(string)
				if !ok {
					return nxclient.InvokeResult{Args: wamp.List{"Argument must be Cid"}, Err: wamp.URI("ocp.error")}
				}
				
				id, err := cid.Decode(strcid)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{"Argument seems not to be a Cid"}, Err: wamp.URI("ocp.error")}
				}

				// Read and send chunks of data
				channel, err := self.swarm.Data.ReadChannel(ctx, id)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				for data := range channel {
					// Send a chunk of data.
					err := self.client.SendProgress(ctx, wamp.List{data}, nil)
					if err != nil {
						close(channel)
						return nxclient.InvokeResult{Err: wamp.ErrCanceled}
					}
				}
				return nxclient.InvokeResult{}
			}

		} else {
			//object raw methods
			
			idx := strings.LastIndex(string(procedure), ".")
			path := procedure[:idx]
			fnc  := procedure[(idx + 1):]

			switch fnc {
			case "SetByPath":
				if len(inv.Arguments) != 1 {
					return nxclient.InvokeResult{Args: wamp.List{"Argument must be path to file or directory"}, Err: wamp.URI("ocp.error")}
				}
				filepath, ok := inv.Arguments[0].(string)
				if !ok {
					return nxclient.InvokeResult{Args: wamp.List{"Argument must be path to file or directory"}, Err: wamp.URI("ocp.error")}
				}
				//add the file!
				cid, err := self.swarm.Data.Add(ctx, filepath)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				//set the cid to the dml object
				op := newCallOperation(dml.User(auth), string(path)+".Set", wamp.List{cid.String()}, node, session)
				return self.executeOperation(ctx, op)

			case "WriteIntoPath":
				if len(inv.Arguments) != 1 {
					return nxclient.InvokeResult{Args: wamp.List{"Argument must be path to directory"}, Err: wamp.URI("ocp.error")}
				}
				dirpath, ok := inv.Arguments[0].(string)
				if !ok {
					return nxclient.InvokeResult{Args: wamp.List{"Argument must be path to directory"}, Err: wamp.URI("ocp.error")}
				}

				//get the cid from the data object!
				val, err := self.dml.Call(dml.User(auth), string(path)+".Get")
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				sval, ok := val.(string)
				if !ok {
					return nxclient.InvokeResult{Args: wamp.List{"Raw object does not contain any data"}, Err: wamp.URI("ocp.error")}
				}
				id, err := cid.Decode(sval)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{"Raw object does not contain any data"}, Err: wamp.URI("ocp.error")}
				}

				//write the data
				filepath, err := self.swarm.Data.Write(ctx, id, dirpath)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}

				//return the exact path of new file
				return nxclient.InvokeResult{Args: wamp.List{filepath}}

			case "SetByBinary":

				//get the cid
				cid, err := self.cidByBinary(ctx, inv)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}

				//set the cid to the dml object
				op := newCallOperation(dml.User(auth), string(path)+".Set", wamp.List{cid.String()}, node, session)
				return self.executeOperation(ctx, op)

			case "ReadBinary":

				//get the cid from the data object!
				val, err := self.dml.Call(dml.User(auth), string(path)+".Get")
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				sval, ok := val.(string)
				if !ok {
					return nxclient.InvokeResult{Args: wamp.List{"Raw object does not contain any data"}, Err: wamp.URI("ocp.error")}
				}
				id, err := cid.Decode(sval)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{"Raw object does not contain any data"}, Err: wamp.URI("ocp.error")}
				}

				// Read and send chunks of data
				channel, err := self.swarm.Data.ReadChannel(ctx, id)
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				for data := range channel {
					// Send a chunk of data.
					err := self.client.SendProgress(ctx, wamp.List{data}, nil)
					if err != nil {
						close(channel)
						return nxclient.InvokeResult{Err: wamp.ErrCanceled}
					}
				}

				return nxclient.InvokeResult{}
			}
		}
		return nxclient.InvokeResult{Args: wamp.List{"Unknown function"}, Err: wamp.URI("ocp.error")}
	}

	return res
}
