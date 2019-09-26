package datastructure

import (
	"context"
	"strings"

	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"

	nxclient "github.com/gammazero/nexus/client"
	wamp "github.com/gammazero/nexus/wamp"
)

type wampHelper struct {
	client *nxclient.Client
	swarm  *p2p.Swarm
	rntm   *dml.Runtime
	prefix string
}

//setup events so that they publish in the wamp client!
func (self wampHelper) SetupDmlEvents(obj dml.EventHandler, path string) {

	//go over all events and set them up
	for _, evtName := range obj.Events() {

		evt := obj.GetEvent(evtName)
		evt.RegisterCallback(self.createWampPublishFunction(path, evtName))
	}
}

//							helper functions
//******************************************************************************

func (self wampHelper) createWampPublishFunction(path string, event string) dml.EventCallback {

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

func (self wampHelper) executeOperation(ctx context.Context, op Operation) *nxclient.InvokeResult {

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

func (self wampHelper) createWampInvokeFunction() nxclient.InvocationHandler {

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

func (self wampHelper) createWampJSFunction() nxclient.InvocationHandler {

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

func (self wampHelper) createWampPropertyFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

		//get the user id
		auth := wamp.OptionString(details, "caller_authid") //authid, for session id use "caller"
		if auth == "" {
			return &nxclient.InvokeResult{Err: wamp.URI("No authid disclosed")}
		}

		//get the paths
		procedure := wamp.OptionURI(details, "procedure")
		idx := strings.LastIndex(string(procedure), ".")
		path := procedure[(len(self.prefix) + 11):idx] //11 for properties.
		prop := procedure[(idx + 1):]

		//get the arguments: if provided it is a write, otherwise read
		if len(args) == 0 {
			val, err := self.rntm.ReadProperty(dml.User(auth), string(path), string(prop))
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
