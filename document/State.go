package document

import (
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/gammazero/nexus/v3/wamp"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"
)

//sharing information about the session, that is currently manipulating the state
type sessionInfo struct {
	Node    p2p.PeerID
	Session wamp.ID
}

func (self *sessionInfo) Set(n p2p.PeerID, s wamp.ID) {
	self.Node = n
	self.Session = s
}

func (self *sessionInfo) Unset() {
	self.Node = p2p.PeerID("")
	self.Session = wamp.ID(0)
}

func (self *sessionInfo) IsSet() bool {
	return self.Node != p2p.PeerID("") && self.Session != wamp.ID(0)
}

//Shared dmlState data structure
//Note: The p2p replicated state ensures that the state is never accessed concurrently, hence
//		does not need a lock. However, we also access the state from the datastructure
//		directly, hence a lock is needed
type dmlState struct {
	//path which holds the datastores and dml files
	path string

	//runtime data
	lock             *sync.Mutex
	dml              *dml.Runtime
	store            *datastore.Datastore
	operationSession *sessionInfo
	views            *viewManager
}

func newState(path string) (dmlState, error) {

	//create the datastore (autocreates the folder)
	//path/Datastore
	store, err := datastore.NewDatastore(path)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Cannot create datastore for datastructure")
	}

	//create the runtime
	rntm := dml.NewRuntime()

	//parse the dm file in path/Dml/main.dml
	dmlpath := filepath.Join(path, "Dml")
	err = rntm.ParseFolder(dmlpath)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Unable to parse dml file")
	}

	//init DB
	err = rntm.InitializeDatastore(store)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Unable to initialize datastore for state")
	}

	//create the view manager
	viewMngr := newViewManager(filepath.Join(path, "Views"))

	return dmlState{path, &sync.Mutex{}, rntm, store, &sessionInfo{}, viewMngr}, nil
}

func (self dmlState) Apply(data []byte) interface{} {

	self.lock.Lock()
	defer self.lock.Unlock()

	//get the operation from the log entry
	op, err := operationFromData(data)
	if err != nil {
		return utils.StackError(err, "Provided data is not of Operation type")
	}

	//ensure the correct session is set
	self.operationSession.Set(op.GetSession())
	defer self.operationSession.Unset()

	//append to all available views
	self.views.appendOperation(op)

	//apply to runtime
	return op.ApplyTo(self.dml, self.store)
}

func (self dmlState) Snapshot() ([]byte, error) {

	self.lock.Lock()
	defer self.lock.Unlock()

	//prepare the datastore for backup
	err := self.store.PrepareFileBackup()
	defer self.store.FinishFileBackup()
	if err != nil {
		return nil, utils.StackError(err, "Unable to prepare the datastore for snapshotting")
	}

	//zip the folder to get a a nice byte slice
	data := make([]byte, 0)
	buf := bytes.NewBuffer(data)
	writer := zip.NewWriter(buf)

	files, err := ioutil.ReadDir(self.store.Path())
	if err != nil {
		return data, utils.StackError(err, "Unable to open datastore directory for snapshotting")
	}

	for _, file := range files {
		if !file.IsDir() {
			path := filepath.Join(self.store.Path(), file.Name())
			dat, err := ioutil.ReadFile(path)
			if err != nil {
				return data, utils.StackError(err, "Unable to open file in datastore directory")
			}

			// Add some files to the archive.
			f, err := writer.Create(file.Name())
			if err != nil {
				return data, utils.StackError(err, "Unable to create file in zip archive")
			}
			_, err = f.Write(dat)
			if err != nil {
				return data, utils.StackError(err, "Unable to add file data to zip archive")
			}
		}
	}

	err = writer.Close()
	if err != nil {
		return data, utils.StackError(err, "Unable to close zip writer for data generation")
	}

	return buf.Bytes(), nil
}

func (self dmlState) LoadSnapshot(data []byte) error {

	self.lock.Lock()
	defer self.lock.Unlock()

	//prepare the datastore for backup
	err := self.store.PrepareFileBackup()
	defer self.store.FinishFileBackup()
	if err != nil {
		return utils.StackError(err, "Unable to prepare the datastore for snapshot restore")
	}

	//clear the datastore directory
	files, err := ioutil.ReadDir(self.store.Path())
	if err != nil {
		return utils.StackError(err, "Unable to open datastore directory for snapshot restore")
	}

	for _, file := range files {
		path := filepath.Join(self.store.Path(), file.Name())
		err := os.Remove(path)
		if err != nil {
			return utils.StackError(err, "Unable to delete old datastore files")
		}
	}

	//load the new files
	buf := bytes.NewReader(data)
	reader, err := zip.NewReader(buf, int64(len(data)))
	if err != nil {
		return utils.StackError(err, "Unable to load zip archive from snapshot data")
	}

	for _, f := range reader.File {

		rc, err := f.Open()
		if err != nil {
			return utils.StackError(err, "Unable to read file in zip archive for snapshot restore")
		}
		path := filepath.Join(self.store.Path(), f.Name)
		file, err := os.Create(path)
		if err != nil {
			return utils.StackError(err, "Unable to create file in datastore for snapshot loading")
		}
		_, err = io.Copy(file, rc)
		if err != nil {
			return utils.StackError(err, "Unable to store snapshot data in datastore file")
		}
		rc.Close()
	}

	return nil
}

//Carefull: Not locking, do not use outside of Apply callbacks!
func (self dmlState) _getOperationSession() *sessionInfo {
	//not locking here, as this function is used for event publishing and hence during
	//apply. This would deadlock
	return self.operationSession
}

func (self dmlState) CanCallLocal(session wamp.ID, path string, args ...interface{}) (bool, error) {

	self.lock.Lock()
	defer self.lock.Unlock()

	if self.views.hasView(session) {
		view, err := self.views.getOrCreateView(session, self.store)
		if err != nil {
			return false, utils.StackError(err, "Unable to access opened view")
		}
		return self.dml.IsReadOnly(view.store, path, args...)
	}

	return self.dml.IsReadOnly(self.store, path, args...)
}

func (self dmlState) CallLocal(session wamp.ID, user dml.User, path string, args ...interface{}) (interface{}, error) {

	self.lock.Lock()
	defer self.lock.Unlock()

	//convert all encoded arguments
	for i, arg := range args {
		if utils.Decoder.InterfaceIsEncoded(arg) {
			val, err := utils.Decoder.DecodeInterface(arg)
			if err == nil {
				args[i] = val
			}
		}
	}

	var val interface{}
	var err error
	if self.views.hasView(session) {
		view, err := self.views.getOrCreateView(session, self.store)
		if err != nil {
			return false, utils.StackError(err, "Unable to access opened view")
		}
		val, err = self.dml.Call(view.store, user, path, args...)

	} else {
		val, err = self.dml.Call(self.store, user, path, args...)
	}
	if err != nil {
		return nil, err
	}

	//check if it is a Object, if so we only return the encoded identifier!
	if enc, ok := val.(utils.Encotable); ok {
		val = enc.Encode()
	}

	return val, nil
}

func (self dmlState) HasView(session wamp.ID) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.views.hasView(session)
}

//Carefull: Not locking, do not use outside of Apply callbacks!
func (self dmlState) _sessionsWithView() []wamp.ID {
	//not locking here, as this function is used for event publishing and hence during
	//apply. This would deadlock
	return self.views.getSessionsWithView()
}

func (self dmlState) OpenView(session wamp.ID) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	_, err := self.views.getOrCreateView(session, self.store)
	return err
}

func (self dmlState) CloseView(session wamp.ID, pauseEvent string, eventCB dml.EventCallbackFunc) error {

	//We lock the state, hence noone else will  access the dml runtime and also not emit events.
	//we therefore can remove the event handler and not miss any events

	//TODO:  A few could be open long, and hence extreme amounts of events could need
	//		 processing. As implemented now it blocks all state updates, which could
	//		 be then a long block for everybdy in the document which is not nice.
	//		 Better to release the lock after certain amount of events and reopen for the rest.

	self.lock.Lock()
	defer self.lock.Unlock()

	if pauseEvent != "" {
		cb, err := self.dml.UnregisterEventCallback(pauseEvent)
		defer self.dml.RegisterEventCallback(pauseEvent, cb)
		if err != nil {
			return utils.StackError(err, "Unable to pause event handling")
		}
	}

	if eventCB != nil {
		err := self.dml.RegisterEventCallback("tmp", eventCB)
		defer self.dml.UnregisterEventCallback("tmp")
		if err != nil {
			return utils.StackError(err, "Unable to setup temporary event  handler")
		}
	}

	//close the view, which emits all the events that occured after the view was created
	return self.views.removeAndCloseView(session, self.dml)
}

func (self dmlState) Close() {

	self.lock.Lock()
	defer self.lock.Unlock()

	self.store.Close()
}
