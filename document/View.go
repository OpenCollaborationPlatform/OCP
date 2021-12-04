package document

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"strings"
	"time"

	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/dml"
	"github.com/OpenCollaborationPlatform/OCP/utils"

	"io"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/gammazero/nexus/v3/wamp"
	uuid "github.com/satori/go.uuid"
)

/* A View on a Document

A view is a way to capture the current state of the document and preventing any updates to it.
It is intended for the users to have enough time to process whatever they see without the risk
of running into synchronisation issues due to running changes

1. 	The view needs to be able to be opened and closed. When closing all the missed events need to
	be published
2. 	The view is a user property: It should not happen for everyone that connects to the document,
	but only the requresting user. This holds also for a single node: multiple users/apps could connect
	to the node docuent, but only one of them should see the view
3. 	The view cannot block the normal state operation including updates. Not allowing to update
	the state would block everyone in the document, which is not in line with point 2. If we cache
	the operations before applying to state the correct return values could not be retreived,
	which would be a problem if we are the replica leader

Hence the implementation creates a copy of the current datastore to capture the state and
creates an additional store for all operations that are applied after the capture. Then
on closing the view all stored operations are applied to get all events correctly. When done
the user switches back to the live state.
*/

var eventKey = []byte("events")

type view struct {
	store    *datastore.Datastore
	eventLog *bolt.DB
	path     string
}

func newView(path string, captureDS *datastore.Datastore) (view, error) {

	//create the folder for this view
	id := uuid.NewV4().String()
	viewPath := filepath.Join(path, id)
	err := os.MkdirAll(viewPath, os.ModePerm)
	if err != nil {
		return view{}, wrapInternalError(err, Error_Filesytem)
	}

	//make a copy of the datastore
	err = captureDS.PrepareFileBackup()
	defer captureDS.FinishFileBackup()
	if err != nil {
		return view{}, utils.StackError(err, "Unable to prepare datastore for view creation")
	}

	dsPath := filepath.Join(viewPath, "Datastore")
	err = os.MkdirAll(dsPath, os.ModePerm)
	if err != nil {
		return view{}, wrapInternalError(err, Error_Filesytem)
	}
	err = filepath.Walk(captureDS.Path(), func(path string, info os.FileInfo, err error) error {
		var relPath string = strings.Replace(path, captureDS.Path(), "", 1)
		if relPath == "" {
			return nil
		}
		if info.IsDir() {
			err := os.MkdirAll(filepath.Join(dsPath, relPath), os.ModePerm)
			return wrapInternalError(err, Error_Filesytem)
		} else {
			return copyFileContents(path, filepath.Join(dsPath, relPath))
		}
		return nil
	})

	if err != nil {
		return view{}, utils.StackError(err, "Unable to copy datastore folder")
	}

	//create the new datastore with the copied data
	ds, err := datastore.NewDatastore(viewPath)
	if err != nil {
		return view{}, utils.StackError(err, "Unable to create Datastore from copied folder")
	}

	//create the event store
	olPath := filepath.Join(viewPath, "evtlog.db")
	db, err := bolt.Open(olPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return view{}, wrapInternalError(err, Error_Setup)
	}
	err = db.Update(func(tx *bolt.Tx) error {

		_, err := tx.CreateBucketIfNotExists(eventKey)
		return err
	})

	return view{ds, db, viewPath}, wrapInternalError(err, Error_Process)
}

func (self view) appendEvents(evts []dml.EmmitedEvent) error {

	//single update is sufficient
	return self.eventLog.Update(func(tx *bolt.Tx) error {

		for _, evt := range evts {
			bucket := tx.Bucket(eventKey)
			id, err := bucket.NextSequence()
			if err != nil {
				return wrapInternalError(err, Error_Process)
			}

			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			if err := enc.Encode(evt); err != nil {
				return wrapInternalError(err, Error_Invalid_Data)
			}

			err = wrapInternalError(bucket.Put(itob(id), buf.Bytes()), Error_Process)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (self view) getEventChan() chan dml.EmmitedEvent {

	returnChan := make(chan dml.EmmitedEvent)

	go func() {
		self.eventLog.View(func(tx *bolt.Tx) error {

			bucket := tx.Bucket(eventKey)
			err := bucket.ForEach(func(k, v []byte) error {
				var event dml.EmmitedEvent
				buf := bytes.NewBuffer(v)
				dec := gob.NewDecoder(buf)
				err := dec.Decode(&event)
				if err != nil {
					return err
				}
				returnChan <- event
				return nil
			})

			close(returnChan)
			return err
		})
	}()

	return returnChan
}

func (self view) close(rntm *dml.Runtime) error {

	//close the datastore and log
	err := self.store.Close()
	if err != nil {
		return err
	}
	err = self.eventLog.Close()
	if err != nil {
		return err
	}

	//remove the folder!
	err = os.RemoveAll(self.path)
	return wrapInternalError(err, Error_Filesytem)
}

func (self view) getStore() *datastore.Datastore {
	return self.store
}

type viewManager struct {
	views map[wamp.ID]view
	path  string
}

/* Managing views per session

As a view is a user centric feature, there can be one per session. Hence they
need to be managed. Additionally the state views are used in is copied, hence
all views need to be collected in a struct used as pointer.

Note: ViewManager is not safe for concurrent access
*/

func newViewManager(path string) *viewManager {
	return &viewManager{make(map[wamp.ID]view), path}
}

func (self *viewManager) hasView(id wamp.ID) bool {
	_, ok := self.views[id]
	return ok
}

func (self *viewManager) getOrCreateView(id wamp.ID, store *datastore.Datastore) (view, error) {

	if self.hasView(id) {
		return self.views[id], nil
	}

	v, err := newView(self.path, store)
	if err != nil {
		return v, err
	}

	self.views[id] = v
	return v, nil
}

func (self *viewManager) removeAndCloseView(id wamp.ID, rntm *dml.Runtime) error {

	v, ok := self.views[id]
	if !ok {
		return newUserError(Error_Operation_Invalid, "No view available for ID")
	}
	delete(self.views, id)
	return v.close(rntm)
}

func (self *viewManager) getSessionsWithView() []wamp.ID {

	result := make([]wamp.ID, 0)
	for id, _ := range self.views {
		result = append(result, id)
	}
	return result
}

func (self *viewManager) appendEvents(events []dml.EmmitedEvent) {

	for _, view := range self.views {
		view.appendEvents(events)
	}
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return wrapInternalError(err, Error_Filesytem)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return wrapInternalError(err, Error_Filesytem)
	}
	defer func() {
		out.Close()
	}()
	if _, err = io.Copy(out, in); err != nil {
		return wrapInternalError(err, Error_Filesytem)
	}
	err = out.Sync()
	return wrapInternalError(err, Error_Filesytem)
}

//helper functions  for uint to byte and return transformation
func itob(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

func btoi(b []byte) uint64 {
	value, _ := binary.Uvarint(b)
	return value
}
