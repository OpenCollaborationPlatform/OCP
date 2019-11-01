//Special behaviour that describes the transaction handling

package dml

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"crypto/sha256"
	datastore "github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"

	"github.com/dop251/goja"
	uuid "github.com/satori/go.uuid"
)

func init() {
	gob.Register(new([32]byte))
}

/*********************************************************************************
								Object
*********************************************************************************/
//convinience object to abstracct transaction database io. Not accassible by user
type transaction struct {

	//static data
	identification [32]byte
	rntm           *Runtime

	//dynamic state. We need to be able to commit and reset, hence versiond db entries.
	//actually it is no state that needs to be stored over multiple versions, but as
	//transaction gets deleted after closing the storage overhead is minimal and the
	//easy solution accaptable
	objects datastore.List
	user    datastore.Value
}

func loadTransaction(key [32]byte, rntm *Runtime) (transaction, error) {

	set, err := rntm.datastore.GetOrCreateSet(datastore.ListType, false, key)
	if err != nil {
		return transaction{}, err
	}
	listSet := set.(*datastore.ListSet)

	//load the participants
	objects, err := listSet.GetOrCreateList([]byte("participants"))
	if err != nil {
		return transaction{}, err
	}

	//and the user
	set, err = rntm.datastore.GetOrCreateSet(datastore.ValueType, false, key)
	if err != nil {
		return transaction{}, err
	}
	valueSet := set.(*datastore.ValueSet)
	user, err := valueSet.GetOrCreateValue([]byte("user"))
	if err != nil {
		return transaction{}, err
	}

	return transaction{key, rntm, *objects, *user}, nil
}

func (self transaction) Remove() error {

	//remove participants list
	ldb, err := self.rntm.datastore.GetDatabase(datastore.ListType, true)
	if err != nil {
		return utils.StackError(err, "Unable to access list database")
	}
	err = ldb.RemoveSet(self.identification)
	if err != nil {
		return utils.StackError(err, "Unable to remove database entry of transaction participants")
	}

	//remove the user and transaction entries
	vdb, err := self.rntm.datastore.GetDatabase(datastore.ValueType, true)
	if err != nil {
		return utils.StackError(err, "Unable to access value database")
	}
	err = vdb.RemoveSet(self.identification)
	if err != nil {
		return utils.StackError(err, "Unable to remove database entry of transaction user")
	}
	return nil
}

func (self transaction) Equal(trans transaction) bool {

	return bytes.Equal(self.identification[:], trans.identification[:])
}

func (self transaction) User() (User, error) {

	user, err := self.user.Read()
	if err != nil {
		return User(""), utils.StackError(err, "Unable to read transaction user from database")
	}

	return *(user.(*User)), nil
}

func (self transaction) SetUser(user User) error {

	return self.user.Write(user)
}

func (self transaction) Objects() []Data {

	entries, err := self.objects.GetEntries()
	if err != nil {
		return make([]Data, 0)
	}
	result := make([]Data, len(entries))

	//note: must panic on error as otherwise slice has more nil objects in it
	for i, entry := range entries {

		id, err := entry.Read()
		if err != nil {
			panic("Cannot read identifier\n")
		}

		obj, ok := self.rntm.objects[*(id.(*Identifier))]
		if !ok {
			panic("Id not available")
		}
		result[i] = obj
	}
	return result
}

func (self transaction) HasObject(id Identifier) bool {

	entries, err := self.objects.GetEntries()
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {

		obj_id, err := entry.Read()
		if err != nil {
			continue
		}

		if obj_id.(*Identifier).Equal(id) {
			return true
		}
	}

	return false
}

//Adds object to the transaction. Note: Already having it is not a error to avoid
//the need of excessive checking
func (self transaction) AddObject(id Identifier) error {

	if self.HasObject(id) {
		return nil
	}

	_, err := self.objects.Add(id)
	if err != nil {
		return utils.StackError(err, "Cannot store object identifier in datastore list")
	}

	return nil
}

/*********************************************************************************
								Manager
*********************************************************************************/

type TransactionManager struct {
	methodHandler

	rntm *Runtime

	//we do not need the versioning, but the commit/rollback possibility
	mapset       *datastore.MapSet
	transactions *datastore.Map

	jsobj *goja.Object
}

func NewTransactionManager(rntm *Runtime) (*TransactionManager, error) {

	var setKey [32]byte
	copy(setKey[:], []byte("internal"))
	set, err := rntm.datastore.GetOrCreateSet(datastore.MapType, false, setKey)
	if err != nil {
		return &TransactionManager{}, utils.StackError(err, "Cannot acccess internal list datastore")
	}
	mapSet := set.(*datastore.MapSet)
	map_, err := mapSet.GetOrCreateMap([]byte("transactions"))
	if err != nil {
		return &TransactionManager{}, utils.StackError(err, "Cannot access internal transaction list store")
	}

	mngr := &TransactionManager{NewMethodHandler(), rntm, mapSet, map_, nil}

	//setup default methods
	mngr.AddMethod("IsOpen", MustNewMethod(mngr.IsOpen, true))
	mngr.AddMethod("Open", MustNewMethod(mngr.Open, false))
	mngr.AddMethod("Close", MustNewMethod(mngr.Close, false))

	//build js object
	mngr.jsobj = rntm.jsvm.NewObject()
	err = mngr.SetupJSMethods(mngr.rntm, mngr.jsobj)
	if err != nil {
		return &TransactionManager{}, utils.StackError(err, "Unable to expose TransactionMAnager methods to javascript")
	}

	return mngr, nil
}

//returns if currently a transaction is open
func (self *TransactionManager) IsOpen() bool {
	return self.transactions.HasKey(self.rntm.currentUser)
}

//Opens a transaction, and if one is already open it will be closed first
func (self *TransactionManager) Open() error {

	//close if a transaction is open
	if self.IsOpen() {
		err := self.Close()
		if err != nil {
			return utils.StackError(err, "Unable to close current transaction bevore opening a new one")
		}
	}

	_, err := self.newTransaction()
	if err != nil {
		err = utils.StackError(err, "Unable to open transaction")
	}

	//call the relevant events
	for _, obj := range self.rntm.objects {
		bhvr := getTransactionBehaviour(obj)
		if bhvr != nil {
			err := bhvr.GetEvent("onOpen").Emit()
			if err != nil {
				return utils.StackError(err, "Unable to open transaction due to failed event emitting")
			}
		}
	}

	return err
}

//Close a transaction
func (self *TransactionManager) Close() error {

	trans, err := self.getTransaction()
	if err != nil {
		return utils.StackError(err, "No transaction available to be closed")
	}

	//iterate over all objects and call the close event
	objs := trans.Objects()
	for _, obj := range objs {

		//the object is not part of a transaction anymode
		bhvr := getTransactionBehaviour(obj)

		if bhvr != nil {
			err = bhvr.GetEvent("onClosing").Emit()
			if err != nil {
				return utils.StackError(err, "Unable to close transaction due to failed close event emitting")
			}
			//the object is not part of a transaction anymode
			bhvr.inTransaction.Write(false)
		}
	}

	//remove from db
	err = trans.Remove()
	if err != nil {
		return utils.StackError(err, "Removing transaction from database failed")
	}
	keys, _ := self.transactions.GetKeys()
	for _, key := range keys {
		data, _ := self.transactions.Read(key)
		transkey := *(data.(*[32]byte))
		if bytes.Equal(transkey[:], trans.identification[:]) {
			self.transactions.Remove(key)
			break
		}
	}

	//remove from map
	self.transactions.Remove(self.rntm.currentUser)

	return nil
}

//Adds a new object to the current transaction. Fails if object is already part of
//annother transaction
//Note: If transaction has Object already no error is returned
func (self *TransactionManager) Add(obj Data) error {

	if !obj.HasBehaviour("Transaction") {
		return fmt.Errorf("Object has no transaction behaviour: cannot be added to transaction")
	}
	bhvr, ok := obj.GetBehaviour("Transaction").(*transactionBehaviour)
	if !ok {
		return fmt.Errorf("wrong behaviour type used in object")
	}

	trans, err := self.getTransaction()
	if err != nil {
		err = utils.StackError(err, "Unable to add object to transaction: No transaction open")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}

	//check if object is not already in annother transaction
	if bhvr.InTransaction() && !bhvr.GetTransaction().Equal(trans) {
		err = fmt.Errorf("Object already part of different transaction")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}

	//check if we are already in this transaction
	if trans.HasObject(obj.Id()) {
		return nil
	}

	//check if it is allowed by gthe object
	res, err := bhvr.GetMethod("CanBeAdded").CallBoolReturn()
	if err != nil {
		err = utils.StackError(err, "Invalid \"CanBeAdded\" function")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}
	if res != true {
		err = fmt.Errorf("Object cannot be added to transaction according to \"CanBeAdded\"")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}

	err = trans.AddObject(obj.Id())
	if err != nil {
		err = utils.StackError(err, "Unable to add object to transaction")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}

	//store in the behaviour
	bhvr.current.Write(trans.identification)
	bhvr.inTransaction.Write(true)

	//throw relevant event
	err = bhvr.GetEvent("onParticipation").Emit()
	if err != nil {
		err = utils.StackError(err, "Unable to emit participation event for transaction")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}

	//add the requried additional objects to the transaction
	list := bhvr.GetMethod("DependendObjects").Call()
	objs, ok := list.([]Object)
	if !ok {
		err = fmt.Errorf("Invalid \"DependendObjects\" function: return value must be list of objects")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
	}
	for _, obj := range objs {
		dat, ok := obj.(Data)
		if !ok {
			err = fmt.Errorf("Only objects are allowed to be added to transactions, not %t", obj)
			bhvr.GetEvent("onFailure").Emit(err.Error())
			return err
		}
		self.Add(dat)
	}

	return nil
}

func (self *TransactionManager) getOrCreateTransaction() (transaction, error) {

	//check if we have one already.
	//TODO: distuingish between failed load and not available
	trans, err := self.getTransaction()
	if err == nil {
		return trans, err
	}

	//if not available we need to create one
	trans, err = self.newTransaction()
	if err != nil {
		return transaction{}, utils.StackError(err, "Cannot create new transaction")
	}

	return trans, nil
}

//gets the transaction for the currently active user
func (self *TransactionManager) getTransaction() (transaction, error) {

	if !self.transactions.HasKey(self.rntm.currentUser) {
		return transaction{}, fmt.Errorf("No transaction available for user")
	}

	key, err := self.transactions.Read(self.rntm.currentUser)
	if err != nil {
		return transaction{}, utils.StackError(err, "Faied to access user transaction")
	}

	return loadTransaction(*(key.(*[32]byte)), self.rntm)
}

//opens a new transaction for the current user (without handling the old one if any)
func (self *TransactionManager) newTransaction() (transaction, error) {

	id := uuid.NewV4()
	key := sha256.Sum256(id.Bytes())
	err := self.transactions.Write(self.rntm.currentUser, key)
	if err != nil {
		return transaction{}, utils.StackError(err, "Cannot add transaction to datastore list")
	}

	trans, err := loadTransaction(key, self.rntm)
	if err != nil {
		return transaction{}, utils.StackError(err, "Loading new transaction failed")
	}
	err = trans.SetUser(self.rntm.currentUser)
	if err != nil {
		return transaction{}, utils.StackError(err, "Setting user for new transaction failed")
	}

	return trans, err
}

func getTransactionBehaviour(obj Data) *transactionBehaviour {
	//must have the transaction behaviour
	if !obj.HasBehaviour("Transaction") {
		return nil
	}
	return obj.GetBehaviour("Transaction").(*transactionBehaviour)
}

/*********************************************************************************
								Behaviour
*********************************************************************************/
type transactionBehaviour struct {
	*behaviour

	//transient state (hence db storage)
	inTransaction datastore.Value
	current       datastore.Value
}

func NewTransactionBehaviour(id Identifier, parent Identifier, rntm *Runtime) (Object, error) {

	behaviour, _ := NewBehaviour(id, parent, rntm)

	//get the datastores
	set, err := rntm.datastore.GetOrCreateSet(datastore.ValueType, false, behaviour.Id().Hash())
	if err != nil {
		return nil, err
	}
	vset := set.(*datastore.ValueSet)
	inTrans, _ := vset.GetOrCreateValue([]byte("__inTransaction"))
	curTrans, _ := vset.GetOrCreateValue([]byte("__currentTransaction"))

	tbhvr := &transactionBehaviour{behaviour, *inTrans, *curTrans}

	//add default methods for overriding by the user
	tbhvr.AddMethod("CanBeAdded", MustNewMethod(tbhvr.defaultAddable, true))                //return true/false if object can be used in current transaction
	tbhvr.AddMethod("CanBeClosed", MustNewMethod(tbhvr.defaultCloseable, true))             //return true/false if transaction containing the object can be closed
	tbhvr.AddMethod("DependendObjects", MustNewMethod(tbhvr.defaultDependendObjects, true)) //return array of objects that need also to be added to transaction

	//add default events
	tbhvr.AddEvent(`onOpen`, NewEvent(behaviour.GetJSObject(), rntm))          //called when a new transaction was opened
	tbhvr.AddEvent(`onParticipation`, NewEvent(behaviour.GetJSObject(), rntm)) //called when added to a transaction
	tbhvr.AddEvent(`onClosing`, NewEvent(behaviour.GetJSObject(), rntm))       //called when transaction, to which the parent was added, is closed (means finished)
	tbhvr.AddEvent(`onFailure`, NewEvent(behaviour.GetJSObject(), rntm))       //called when adding to transaction failed, e.g. because already in annother transaction

	//add the user usable methods
	tbhvr.AddMethod("InTransaction", MustNewMethod(tbhvr.InTransaction, true))

	return tbhvr, nil
}

func (self *transactionBehaviour) InTransaction() bool {

	if holds, _ := self.inTransaction.HoldsValue(); !holds {
		return false
	}

	res, _ := self.inTransaction.Read()
	if res == nil {
		return false
	}
	return res.(bool)
}

func (self *transactionBehaviour) GetTransaction() transaction {

	key, _ := self.current.Read()
	trans, err := loadTransaction(*(key.(*[32]byte)), self.rntm)
	if err != nil {
		panic(err.Error())
	}
	return trans
}

func (self *transactionBehaviour) Copy() Behaviour {

	return nil
}

func (self *transactionBehaviour) defaultAddable() bool {
	return true
}

func (self *transactionBehaviour) defaultCloseable() bool {
	return true
}

func (self *transactionBehaviour) defaultDependendObjects() []Object {
	return make([]Object, 0)
}
