package datastructure

import (
	"strings"
	
	"github.com/ickby/CollaborationNode/dml"
	
	nxclient "github.com/gammazero/nexus/client"
	wamp "github.com/gammazero/nexus/wamp"
)

type wampHelper struct {
	client *nxclient.Client
}

//setup events so that they publish in the wamp client!
func (self wampHelper) SetupDmlEvents(obj dml.EventHandler, path string) {
	
	//go over all events and set them up
	for _, evtName := range obj.AllEvents() {
		
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
		path = path + "/" + event
		wamppath := strings.Replace(path, ".", "/", -1)
		
		//other arguments we do not need
		kwargs := make(wamp.Dict, 0)
		opts := make(wamp.Dict, 0)
		
		//publish!
		return self.client.Publish(wamppath, opts, args, kwargs)
	}
}