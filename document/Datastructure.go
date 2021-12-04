package document

import (
	"context"
	"strings"

	"github.com/OpenCollaborationPlatform/OCP/dml"
	"github.com/OpenCollaborationPlatform/OCP/p2p"
	"github.com/OpenCollaborationPlatform/OCP/utils"
	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
)

//Async datastructure which encapsulates synchronous DML runtime and a datastore.
//It works operation based: All operations are carried out ordered
type Datastructure struct {

	//general
	path   string
	prefix string

	//dml state handling
	dmlState *dmlState

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

	//make sure the prefix has a "." as last charachter
	if prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
	}

	//create the state
	state, err := newState(path)
	if err != nil {
		return Datastructure{}, utils.StackError(err, "Unable to create state for datastructure")
	}

	//return the datastructure
	ds := Datastructure{
		path:     path,
		prefix:   prefix,
		dmlState: state,
		swarm:    nil,
		client:   client,
	}

	return ds, nil
}

// 							Bookepping functions
// *****************************************************************************
func (self Datastructure) Start(s *p2p.Swarm) {
	self.swarm = s
	self.dmlState.publish = self.publishCallback

	//initiate the client connections for events
	rntm := self.dmlState.dml

	//general options
	options := wamp.SetOption(wamp.Dict{}, wamp.OptMatch, wamp.MatchPrefix)
	options = wamp.SetOption(options, wamp.OptDiscloseCaller, true)

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

	// first emit all events
	eventChan, err := self.dmlState.ViewEventChannel(session)
	if err != nil {
		return wrapInternalError(err, "Cannot get event channel for view")
	}

	for evt := range eventChan {
		self.publish(evt, whitelistEventOptions(session))
	}

	return self.dmlState.CloseView(session)
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

func (self Datastructure) publishCallback(evts []dml.EmmitedEvent) {

	self.multiPublish(evts, defaultEventOtions())
}

func (self Datastructure) multiPublish(events []dml.EmmitedEvent, optionFnc optFnc) {

	for _, event := range events {

		//convert the arguments into wamp style
		//this includes encoding all custom types
		self.publish(event, optionFnc)
	}
}

func (self Datastructure) publish(event dml.EmmitedEvent, optionFnc optFnc) {

	//convert the arguments into wamp style
	//this includes encoding all custom types
	wampArgs := make(wamp.List, len(event.Args))
	for i, arg := range event.Args {

		if enc, ok := arg.(utils.Encotable); ok {
			wampArgs[i] = enc.Encode()

		} else {
			wampArgs[i] = arg
		}
	}

	//connvert the path into wamp style uri
	uri := self.prefix + "content." + event.Path

	//other arguments we do not need
	kwargs := make(wamp.Dict, 0)
	opts := optionFnc(&self)

	//publish!
	self.client.Publish(uri, opts, wampArgs, kwargs)
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

/* +extract prio:2
.. wamp:uri:: ocp.documents.<docid>.content.<dmlpath>

	Access anything within the document that is defined by the DML code. As the
	dml code builds up a hirarchical datastructure where each object has its own
	name, it can be easily be transformed into a WAMP uri. This is then used to
	access individual objects, properties or events. The end result of where the URI
	points to defines than the behaviour and how it should be used from the client:

	* Object: Calling the procedure returns the ObjID
	* Property: Calling the procedure without arguments returns the value, with an argument sets the property value
	* Event: Subscribing to the topic receives all event emits
	* Function: Calling the procedure calls the function and returns its results

	:path DocID docid: The document whichs content should be accessed
	:path str dmlpath: Mutli state procedure to the dml object
*/
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

/* +extract prio:2
.. wamp:procedure:: ocp.documents.<docid>.execute(code)

	Execute the provided javascript code in the document.

	:path DocID docid: The document in which the code shall be executed
	:param str code: JavaScript code that will be executed within the document
	:return: Returns the outcome of the code
*/
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

/* +extract prio:4

Raw data handling
^^^^^^^^^^^^^^^^^
OCP documents support raw binary data as mass storage for complex and custom
data. It can be added directly from the filesystem or as binary datastream with
the provided procedures. If used the caller is responsible for storing the
content identifiers in the document, it is not done automatically.
*/
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
				/* +extract prio:4
				.. wamp:procedure:: ocp.documents.<docid>.raw.CidByBinary(uri, arguments)

					Adds raw binary data to the document. To support unlimited size data the binary stream is broken up in packages
					and a progressive WAMP procedure is used to deliver the packages. However, as only progressive read is supported,
					and not write, the user needs to provide a progressive wamp procedure the node can call. The arguments to
					CidByBinary are the URI of this procedure as well as any argument you want it to receive. The node than calls
					your procedure with those arguments and collects the binary data.

					:path DocID docid: ID of the document the data shall be added to
					:param Uri uri: The uri of the progressive WAMP procedure that delivers the binary data stream
					:param any arguments: All arguments that shall be provided to the progressive procedure to identify the correct data
					:result cid: Content identifier for the binary datastream
					:resulttype cid: Cid
				*/

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
				/* +extract prio:4
				.. wamp:procedure:: ocp.documents.<docid>.raw.BinaryByCid(uri, arguments)

					Reads raw binary data from the document. To support unlimited size data the binary stream is broken up in packages,
					hence this is a progressive WAMP procedure. You need to collect all datapackages sent and combine them into the
					final binary data.

					:path DocID docid: ID of the document the data shall be added to
					:param Uri uri: The uri of the progressive WAMP procedure that delivers the binary data stream
					:param any arguments: All arguments that shall be provided to the progressive procedure to identify the correct data
					:result Cid cid: Content identifier for the binary datastream
				*/

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
				/* +extract prio:4
				.. wamp:procedure:: ocp.documents.<docid>.raw.CidByPath(path)

					Reads raw data from the filesystem. It adds all the content in path to the document,
					including all subdirectories recursively. The returned cid will be valid for the
					full structure, all files and directories, and it is not possible to get a cid for
					individual files or subdirs. The structure stays intact, and when extracting the data
					again into a filesystem will be reproduced.

					.. note:: The content in path is copied without any restrictions or filtering

					:path DocID docid: ID of the document the data in path shall be added to
					:param str path: Absolute filesystem path to file or directory
					:result Cid cid: Content identifier for the data in path
				*/
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
				/* +extract prio:4
				.. wamp:procedure:: ocp.documents.<docid>.raw.PathByCid(cid, path)

					Write data stored in the document into the given path. If the cid describes a binary stream or file,
					a file will be created, if it is a directory it will be recreated with the original structure.
					The name of files or toplevel directories are not stored and hence not recreated. The name will
					be the Cid. Use the return value to get the newly created path with full name.

					:path DocID docid: ID of the document the data in path shall be added to
					:param Cid cid: The content you want to store in the filesystem
					:param str path: Absolute filesystem to directory to create the content in
					:result str path: Path of the newly created file or directory
				*/
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
				val, _, err := self.dmlState.dml.Call(self.dmlState.store, dml.User(auth), string(path))
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
				val, _, err := self.dmlState.dml.Call(self.dmlState.store, dml.User(auth), string(path))
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
