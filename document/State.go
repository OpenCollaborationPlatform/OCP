package document

import (
	"archive/zip"
	"bytes"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/utils"
	"github.com/ickby/CollaborationNode/p2p"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"github.com/gammazero/nexus/v3/wamp"
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

//shared dmlState data structure
type dmlState struct {
	//path which holds the datastores and dml files
	path string

	//runtime data
	dml              *dml.Runtime
	store            *datastore.Datastore
	operationSession *sessionInfo
}

func newState(path string) (dmlState, error) {

	//create the datastore (autocreates the folder)
	//path/Datastore
	store, err := datastore.NewDatastore(path)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Cannot create datastore for datastructure")
	}

	//create the runtime
	rntm := dml.NewRuntime(store)
	rntm.RegisterObjectCreator("Raw", NewRawDmlObject)

	//parse the dm file in path/Dml/main.dml
	dmlpath := filepath.Join(path, "Dml")
	err = rntm.ParseFolder(dmlpath)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Unable to parse dml file")
	}

	return dmlState{path, rntm, store, &sessionInfo{}}, nil
}

func (self dmlState) Apply(data []byte) interface{} {

	//get the operation from the log entry
	op, err := operationFromData(data)
	if err != nil {
		return utils.StackError(err, "Provided data is not of Operation type")
	}

	//ensure the correct session is set
	self.operationSession.Set(op.GetSession())
	defer self.operationSession.Unset()

	//apply to runtime
	return op.ApplyTo(self.dml)
}

func (self dmlState) Snapshot() ([]byte, error) {

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

func (self dmlState) GetOperationSession() *sessionInfo {
	return self.operationSession
}

func (self dmlState) CanCallLocal(path string) (bool, error) {
	return self.dml.IsConstant(path)
}

func (self dmlState) CallLocal(user dml.User, path string, args ...interface{}) (interface{}, error) {
	val, err := self.dml.Call(user, path, args...)
	if err != nil {
		return nil, err
	}	
	
	//check if it is a Object, if so we only return the encoded identifier!
	obj, ok := val.(dml.Object)
	if ok {
		return obj.Id().Encode(), nil
	}
	return val, nil
}

func (self dmlState) Close() {

	self.store.Close()
}
