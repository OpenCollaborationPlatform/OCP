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
		path = self.prefix + "events/" + path + "/" + event
		wamppath := strings.Replace(path, ".", "/", -1)

		//other arguments we do not need
		kwargs := make(wamp.Dict, 0)
		opts := make(wamp.Dict, 0)

		//publish!
		return self.client.Publish(wamppath, opts, args, kwargs)
	}
}

func (self wampHelper) createWampInvokeFunction() nxclient.InvocationHandler {

	res := func(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *nxclient.InvokeResult {

		//get the details: user and path of the call
		auth := wamp.OptionString(details, "caller_authid") //authid, for session id use "caller"
		procedure := wamp.OptionURI(details, "procedure")

		//build dml path and function
		idx := strings.LastIndex(string(procedure), "/")
		path := procedure[(len(self.prefix)+8):idx]
		fnc := procedure[(idx + 1):]

		//build the operation arguments
		op := newFunctionOperation(dml.User(auth), string(path), string(fnc), args)

		//execute the operation!
		res, err := self.swarm.State.AddCommand(ctx, "dmlState", op.ToData())
		if err != nil {
			return &nxclient.InvokeResult{Args: wamp.List{err}}
		}

		return &nxclient.InvokeResult{Args: wamp.List{res}}
	}

	return res
}
