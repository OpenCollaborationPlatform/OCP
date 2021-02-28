package document

import (
	"context"
	"strings"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
)

//Async datastructure which encapsulates synchronous DML runtime and a datastore.
//It works operation based: All operations are carried out ordered
type Datastructure struct {

	//general
	path   string
	prefix string

	//dml state handling
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
	rntm := self.dmlState.dml

	//general options
	options := wamp.SetOption(wamp.Dict{}, wamp.OptMatch, wamp.MatchPrefix)
	options = wamp.SetOption(options, wamp.OptDiscloseCaller, true)

	//register the event handler
	rntm.RegisterEventCallback("events", self.createWampPublishFunction(defaultEventOtions()))

	//register the function handler
	uri := self.prefix + "content"
	self.client.Register(uri, self.createWampInvokeFunction(), options)

	//register raw data handler
	uri = self.prefix + "raw"
	self.client.Register(uri, self.createWampRawFunction(), options)

	//register javascript handler
	options = wamp.SetOption(options, wamp.OptMatch, wamp.MatchExact)
	uri = self.prefix + "execute"
	self.client.Register(uri, self.createWampJSFunction(), options)

	//register debug print handler
	uri = self.prefix + "prints"
	fnc := func(ctx context.Context, inv *wamp.Invocation) nxclient.InvokeResult {
		return nxclient.InvokeResult{Args: wamp.List{rntm.GetMessages()}}
	}
	self.client.Register(uri, fnc, options)
}

func (self Datastructure) Close() {
	self.dmlState.Close()
	self.client.Unregister(self.prefix + "content")
	self.client.Unregister(self.prefix + "raw")
	self.client.Unregister(self.prefix + "execute")
	self.client.Unregister(self.prefix + "prints")
}

func (self Datastructure) GetState() p2p.State {
	return self.dmlState
}

func (self Datastructure) OpenView(session wamp.ID) error {
	return self.dmlState.OpenView(session)
}

func (self Datastructure) CloseView(session wamp.ID) error {
	return self.dmlState.CloseView(session, "events", self.createWampPublishFunction(whitelistEventOptions(session)))
}

func (self Datastructure) HasView(session wamp.ID) bool {
	return self.dmlState.HasView(session)
}

//							helper functions
//******************************************************************************

type optFnc func(*Datastructure) wamp.Dict

func defaultEventOtions() optFnc {
	return func(ds *Datastructure) wamp.Dict {

		//check if we should omit a session!
		opts := make(wamp.Dict, 0)
		exclude := ds.dmlState._sessionsWithView()
		session := ds.dmlState._getOperationSession()
		if session.IsSet() && session.Node == ds.swarm.GetHost().ID() {
			exclude = append(exclude, session.Session)
		}
		opts["exclude"] = exclude
		return opts
	}
}

func whitelistEventOptions(whitelist wamp.ID) optFnc {
	return func(ds *Datastructure) wamp.Dict {
		opts := make(wamp.Dict, 0)
		opts["eligible"] = wamp.List{whitelist}
		return opts
	}
}

func (self Datastructure) createWampPublishFunction(optionFnc optFnc) dml.EventCallbackFunc {

	return func(path string, args ...interface{}) {

		//convert the arguments into wamp style
		//this includes encoding all custom types
		wampArgs := make(wamp.List, len(args))
		for i, arg := range args {

			if enc, ok := arg.(utils.Encotable); ok {
				wampArgs[i] = enc.Encode()

			} else {
				wampArgs[i] = arg
			}
		}

		//connvert the path into wamp style uri
		uri := self.prefix + "content." + path

		//other arguments we do not need
		kwargs := make(wamp.Dict, 0)
		opts := optionFnc(&self)

		//publish!
		self.client.Publish(uri, opts, wampArgs, kwargs)
	}
}

func (self Datastructure) executeOperation(ctx context.Context, op Operation) nxclient.InvokeResult {

	//execute the operation!
	data, err := op.ToData()
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	res, err := self.swarm.State.AddCommand(ctx, "dmlState", data)
	if err != nil {
		return utils.ErrorToWampResult(err)
	}

	//check if the returned value is an error
	if err, ok := res.(error); ok {
		return utils.ErrorToWampResult(err)
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
			err := newUserError(Error_Operation_Invalid, "No authid disclosed in WAMP call")
			return utils.ErrorToWampResult(err)
		}

		//build dml path
		path := procedure[(len(self.prefix) + 8):] // 8 content.

		//check if we execute local or globally
		local, err := self.dmlState.CanCallLocal(session, string(path), inv.Arguments...)
		if err != nil {
			return utils.ErrorToWampResult(err)
		}
		if local {
			res, err := self.dmlState.CallLocal(session, dml.User(auth), string(path), inv.Arguments...)
			if err != nil {
				return utils.ErrorToWampResult(err)
			}
			return nxclient.InvokeResult{Args: wamp.List{res}}
		}

		if self.dmlState.HasView(session) {
			err := newUserError(Error_Operation_Invalid, "View is open, cannot make change")
			return utils.ErrorToWampResult(err)
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

		if self.dmlState.HasView(session) {
			err := newUserError(Error_Operation_Invalid, "View is open, cannot make change")
			return utils.ErrorToWampResult(err)
		}

		//get the user id
		auth := wamp.OptionString(inv.Details, "caller_authid") //authid, for session id use "caller"
		if auth == "" {
			err := newUserError(Error_Operation_Invalid, "No authid disclosed in WAMP call")
			return utils.ErrorToWampResult(err)
		}

		//get the js code
		if len(inv.Arguments) != 1 {
			err := newUserError(Error_Arguments, "JS code needs to be provided as single argument")
			return utils.ErrorToWampResult(err)
		}
		code, ok := inv.Arguments[0].(string)
		if !ok {
			err := newUserError(Error_Arguments, "Argument not valid JS code")
			return utils.ErrorToWampResult(err)
		}

		//build and execute the operation arguments
		op := newJsOperation(dml.User(auth), code, node, session)
		return self.executeOperation(ctx, op)
	}

	return res
}

func (self Datastructure) cidByBinary(ctx context.Context, inv *wamp.Invocation) (utils.Cid, error) {

	//as progressive results are only possible when calling, not when
	//get called, we call back and get the data progressivly
	//the arguments is the uri to call, and the arg to identify the data!
	if len(inv.Arguments) != 2 {
		return utils.Cid{}, newUserError(Error_Arguments, "Arguments must be URI to call for progressive data and the argsument for that uri")
	}
	uri, ok := inv.Arguments[0].(string)
	if !ok {
		return utils.Cid{}, newUserError(Error_Arguments, "First argument must be URI")
	}
	arg := inv.Arguments[1] //Arg is allowed to be everything

	//make the progressive call!
	block := make([]byte, 0)
	callback := func(result *wamp.Result) {
		//get the data block
		if len(result.Arguments) != 1 {
			//fmt.Printf("Progress called but no data in %v\n", result)
			return
		}

		res := result.Arguments[0]
		switch res.(type) {
		case []byte:
			block = append(block, res.([]byte)...)
		case string:
			block = append(block, []byte(res.(string))...)
		}
	}
	res, err := self.client.Call(ctx, uri, wamp.Dict{}, wamp.List{arg}, wamp.Dict{}, callback)
	if err != nil {
		return utils.Cid{}, wrapConnectionError(err, Error_Process)
	}
	if len(res.Arguments) > 0 && res.Arguments[0] != nil {

		data := res.Arguments[0]
		switch data.(type) {
		case []byte:
			block = append(block, data.([]byte)...)
		case string:
			block = append(block, []byte(data.(string))...)
		default:
			return utils.Cid{}, newInternalError(Error_Invalid_Data, "Binary data not received as binary")
		}
	}

	//add the data we received
	cid, err := self.swarm.Data.AddData(ctx, block)
	if err != nil {
		return utils.Cid{}, utils.StackError(err, "Unable to distribute data")
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
			err := newUserError(Error_Operation_Invalid, "No authid disclosed in WAMP call")
			return utils.ErrorToWampResult(err)
		}

		//get the paths
		procedure := string(wamp.OptionURI(inv.Details, "procedure"))
		procedure = procedure[(len(self.prefix) + 4):] // 4 for raw.

		if !strings.Contains(procedure, ".") {
			//generall, non-object raw methods

			switch procedure {

			case "CidByBinary":

				if self.dmlState.HasView(session) {
					err := newUserError(Error_Operation_Invalid, "View is open, cannot make change")
					return utils.ErrorToWampResult(err)
				}

				//get the cid
				cid, err := self.cidByBinary(ctx, inv)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}
				//return the cid (no operation started, this is just to setup the data!
				return nxclient.InvokeResult{Args: wamp.List{cid.Encode()}}

			case "BinaryByCid":

				if len(inv.Arguments) != 1 {
					err := newUserError(Error_Arguments, "Argument must be Cid")
					return utils.ErrorToWampResult(err)
				}
				strcid, ok := inv.Arguments[0].(string)
				if !ok {
					err := newUserError(Error_Arguments, "Argument must be Cid")
					return utils.ErrorToWampResult(err)
				}

				id, err := utils.CidDecode(strcid)
				if err != nil {
					err := newUserError(Error_Arguments, "Argument must be Cid")
					return utils.ErrorToWampResult(err)
				}

				// Read and send chunks of data
				channel, err := self.swarm.Data.ReadChannel(ctx, id)
				if err != nil {
					//channel gets closed on error
					return utils.ErrorToWampResult(err)
				}
				for data := range channel {
					// Send a chunk of data.
					err := self.client.SendProgress(ctx, wamp.List{data}, nil)
					if err != nil {
						err := wrapConnectionError(err, Error_Process)
						return utils.ErrorToWampResult(err)
					}
				}
				return nxclient.InvokeResult{}

			case "CidByPath":

				if self.dmlState.HasView(session) {
					err := newUserError(Error_Operation_Invalid, "View is open, cannot make change")
					return utils.ErrorToWampResult(err)
				}

				if len(inv.Arguments) != 1 {
					err := newUserError(Error_Arguments, "Argument must be path to file or directory")
					return utils.ErrorToWampResult(err)
				}
				filepath, ok := inv.Arguments[0].(string)
				if !ok {
					err := newUserError(Error_Arguments, "Argument must be path to file or directory")
					return utils.ErrorToWampResult(err)
				}
				//add the file!
				cid, err := self.swarm.Data.Add(ctx, filepath)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}
				return nxclient.InvokeResult{Args: wamp.List{cid.Encode()}}

			case "PathByCid":
				if len(inv.Arguments) != 2 {
					err := newUserError(Error_Arguments, "Argument must be cid and path to directory")
					return utils.ErrorToWampResult(err)
				}

				strcid, ok := inv.Arguments[0].(string)
				if !ok {
					err := newUserError(Error_Arguments, "First argument must be cid")
					return utils.ErrorToWampResult(err)
				}

				id, err := utils.CidDecode(strcid)
				if err != nil {
					err := newUserError(Error_Arguments, "First argument must be cid")
					return utils.ErrorToWampResult(err)
				}

				dirpath, ok := inv.Arguments[1].(string)
				if !ok {
					err := newUserError(Error_Arguments, "Second argument must be path to directory")
					return utils.ErrorToWampResult(err)
				}

				//write the data
				filepath, err := self.swarm.Data.Write(ctx, id, dirpath)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}

				//return the exact path of new file
				return nxclient.InvokeResult{Args: wamp.List{filepath}}
			}

		} else {
			//property raw methods

			idx := strings.LastIndex(string(procedure), ".")
			path := procedure[:idx]
			fnc := procedure[(idx + 1):]

			switch fnc {
			case "SetByPath":

				if self.dmlState.HasView(session) {
					err := newUserError(Error_Operation_Invalid, "View is open, cannot make change")
					return utils.ErrorToWampResult(err)
				}

				if len(inv.Arguments) != 1 {
					err := newUserError(Error_Arguments, "Argument must be path to file or directory")
					return utils.ErrorToWampResult(err)
				}
				filepath, ok := inv.Arguments[0].(string)
				if !ok {
					err := newUserError(Error_Arguments, "Argument must be path to file or directory")
					return utils.ErrorToWampResult(err)
				}
				//add the file!
				cid, err := self.swarm.Data.Add(ctx, filepath)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}
				//set the cid to the dml property
				op := newCallOperation(dml.User(auth), string(path), wamp.List{cid.Encode()}, node, session)
				return self.executeOperation(ctx, op)

			case "WriteIntoPath":
				if len(inv.Arguments) != 1 {
					err := newUserError(Error_Arguments, "Argument must be path to directory")
					return utils.ErrorToWampResult(err)
				}
				dirpath, ok := inv.Arguments[0].(string)
				if !ok {
					err := newUserError(Error_Arguments, "Argument must be path to directory")
					return utils.ErrorToWampResult(err)
				}

				//get the cid from the data object!
				val, err := self.dmlState.dml.Call(self.dmlState.store, dml.User(auth), string(path))
				if err != nil {
					return utils.ErrorToWampResult(err)
				}
				id, ok := val.(utils.Cid)
				if !ok {
					err := newUserError(Error_Operation_Invalid, "Raw property faulty, does not contain Cid")
					return utils.ErrorToWampResult(err)
				}
				if !id.Defined() {
					err := newUserError(Error_Operation_Invalid, "Raw property does not contain any data")
					return utils.ErrorToWampResult(err)
				}

				//write the data
				filepath, err := self.swarm.Data.Write(ctx, id, dirpath)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}

				//return the exact path of new file
				return nxclient.InvokeResult{Args: wamp.List{filepath}}

			case "SetByBinary":

				if self.dmlState.HasView(session) {
					err := newUserError(Error_Operation_Invalid, "View is open, cannot make change")
					return utils.ErrorToWampResult(err)
				}

				//get the cid
				cid, err := self.cidByBinary(ctx, inv)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}

				//set the cid to the dml object
				op := newCallOperation(dml.User(auth), string(path), wamp.List{cid.Encode()}, node, session)
				return self.executeOperation(ctx, op)

			case "ReadBinary":

				//get the cid from the data object!
				val, err := self.dmlState.dml.Call(self.dmlState.store, dml.User(auth), string(path))
				if err != nil {
					return nxclient.InvokeResult{Args: wamp.List{err.Error()}, Err: wamp.URI("ocp.error")}
				}
				id, ok := val.(utils.Cid)
				if !ok {
					err := newUserError(Error_Operation_Invalid, "Raw property faulty, does not contain Cid")
					return utils.ErrorToWampResult(err)
				}
				if !id.Defined() {
					err := newUserError(Error_Operation_Invalid, "Raw property does not contain any data")
					return utils.ErrorToWampResult(err)
				}

				// Read and send chunks of data
				channel, err := self.swarm.Data.ReadChannel(ctx, id)
				if err != nil {
					return utils.ErrorToWampResult(err)
				}
				for data := range channel {
					// Send a chunk of data.
					err := self.client.SendProgress(ctx, wamp.List{data}, nil)
					if err != nil {
						close(channel)
						return utils.ErrorToWampResult(wrapConnectionError(err, Error_Process))
					}
				}

				return nxclient.InvokeResult{}
			}
		}
		return nxclient.InvokeResult{Args: wamp.List{"Unknown function"}, Err: wamp.URI("ocp.error.application.documents.uri_wrong")}
	}

	return res
}
