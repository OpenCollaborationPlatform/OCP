package datastructure

import (
	"archive/zip"
	"bytes"
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/utils"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

//shared dmlState data structure
type dmlState struct {
	//path which holds the datastores and dml files
	path string

	//runtime data
	dml   *dml.Runtime
	store *datastore.Datastore
}

func newState(path string) (dmlState, error) {

	//create the datastore (autocreates the folder)
	//path/Datastore
	store, err := datastore.NewDatastore(path)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Cannot create datastore for datastructure")
	}

	//read in the file and create the runtime
	//path/Dml/main.dml
	rntm := dml.NewRuntime(store)
	file := filepath.Join(path, "Dml", "main.dml")
	filereader, err := os.Open(file)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Unable to load dml file")
	}
	err = rntm.Parse(filereader)
	if err != nil {
		return dmlState{}, utils.StackError(err, "Unable to parse dml file")
	}

	return dmlState{path, rntm, store}, nil
}

func (self dmlState) Apply(data []byte) interface{} {

	//get the operation from the log entry
	op := operationFromData(data)

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

func (self dmlState) Close() {
	
	self.store.Close()
}