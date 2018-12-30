//Special behaviour that describes the transaction handling

package dml

import (
	"bytes"
	"fmt"

	"CollaborationNode/datastores"
	"CollaborationNode/utils"
	"crypto/sha256"

	"github.com/dop251/goja"
	"github.com/satori/go.uuid"
)

/*********************************************************************************
								Object
*********************************************************************************/
//convinience object to abstracct transaction database io. Not accassible by user
type transaction struct {
	identification datastore.ListEntry
	objects        datastore.List
	user           datastore.Value
	rntm           *Runtime
}

func (self *transaction) Equal(trans transaction) bool {

	data, err := self.identification.Read()
	if err != nil {
		panic(err)
	}
	id, ok := data.([32]byte)
	if !ok {
		panic("cannot read transaction key")
	}

	data, err = trans.identification.Read()
	if err != nil {
		panic(err)
	}
	otherid, ok := data.([32]byte)

	return bytes.Equal(id[:], otherid[:])
}

func (self *transaction) User() (User, error) {

	data, err := self.user.Read()
	if err != nil {
		return User(""), err
	}

	return User(data.(string)), nil
}

func (self *transaction) SetUser(user User) error {

	return self.user.Write(user)
}

func (self *transaction) Objects() []Object {

	entries, err := self.objects.GetEntries()
	if err != nil {
		return make([]Object, 0)
	}
	result := make([]Object, len(entries))

	for i, entry := range entries {

		data, err := entry.Read()
		if err != nil {
			continue
		}
		id, ok := data.(identifier)
		if !ok {
			panic("unable to load id")
		}

		result[i] = self.rntm.objects[id]
	}
	return result
}

func (self *transaction) HasObject(id identifier) bool {

	entries, err := self.objects.GetEntries()
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {

		data, err := entry.Read()
		if err != nil {
			continue
		}
		obj_id, ok := data.(identifier)
		if !ok {
			panic("unable to load id")
		}

		if obj_id.equal(id) {
			return true
		}
	}

	return false
}

//Adds object to the transaction. Note: Already having it is not a error to avoid
//the need of excessive checking
func (self *transaction) AddObject(id identifier) error {

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

	rntm         *Runtime
	list         *datastore.List
	transactions map[User]transaction
	jsobj        *goja.Object
}

func NewTransactionManager(rntm *Runtime) (TransactionManager, error) {

	var setKey [32]byte
	copy(setKey[:], []byte("internal"))
	set, err := rntm.datastore.GetOrCreateSet(datastore.ListType, false, setKey)
	if err != nil {
		return TransactionManager{}, utils.StackError(err, "Cannot acccess internal list datastore")
	}
	listSet := set.(*datastore.ListSet)
	list, err := listSet.GetOrCreateList([]byte("transactions"))
	if err != nil {
		return TransactionManager{}, utils.StackError(err, "Cannot access internal transaction list store")
	}

	mngr := TransactionManager{NewMethodHandler(), rntm, list, make(map[User]transaction, 0), nil}

	//setup default methods
	mngr.AddMethod("IsOpen", MustNewMethod(mngr.IsOpen))
	mngr.AddMethod("Open", MustNewMethod(mngr.Open))
	mngr.AddMethod("Close", MustNewMethod(mngr.Close))

	//build js object
	mngr.jsobj = rntm.jsvm.NewObject()
	err = mngr.SetupJSMethods(mngr.rntm.jsvm, mngr.jsobj)
	if err != nil {
		return TransactionManager{}, utils.StackError(err, "Unable to expose TransactionMAnager methods to javascript")
	}

	return mngr, nil
}

//returns if currently a transaction is open
func (self *TransactionManager) IsOpen() bool {
	_, ok := self.transactions[self.rntm.currentUser]
	return ok
}

//Opens a transaction, and if one is already open it will be closed first
func (self *TransactionManager) Open() error {

	//close if a transaction is open
	if self.IsOpen() {
		self.Close()
	}

	_, err := self.newTransaction()
	if err != nil {
		err = utils.StackError(err, "Unable to open transaction")
	}
	return err
}

//Close a transaction
func (self *TransactionManager) Close() error {

	if !self.IsOpen() {
		return fmt.Errorf("No transaction open to be closed")
	}

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
	if bhvr.inTransaction && !bhvr.current.Equal(trans) {
		err = fmt.Errorf("Object already part of different transaction")
		bhvr.GetEvent("onFailure").Emit(err.Error())
		return err
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

	bhvr.current = trans
	bhvr.inTransaction = true

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

//run this on runtime initialisation to setup all transactions correctly
func (self *TransactionManager) loadTransactions() error {

	entries, err := self.list.GetEntries()
	if err != nil {
		return utils.StackError(err, "Cannot load transactions")
	}
	for _, entry := range entries {

		//get the transaction
		transaction, err := self.loadTransaction(entry)
		if err != nil {
			return utils.StackError(err, "Failed to load transactions from datastore")
		}
		usr, err := transaction.User()
		if err != nil {
			return utils.StackError(err, "Cannot access transactions user")
		}
		self.transactions[usr] = transaction
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

	trans, ok := self.transactions[self.rntm.currentUser]
	if !ok {
		return transaction{}, fmt.Errorf("No transaction available for user")
	}

	return trans, nil
}

//opens a new transaction for the current user (without handling the old one if any)
func (self *TransactionManager) newTransaction() (transaction, error) {

	id, err := uuid.NewV4()
	if err != nil {
		return transaction{}, utils.StackError(err, "Cannot create transaction id")
	}
	key := sha256.Sum256(id.Bytes())
	entry, err := self.list.Add(key)
	if err != nil {
		return transaction{}, utils.StackError(err, "Cannot add transaction to datastore list")
	}

	trans, err := self.loadTransaction(entry)
	if err != nil {
		return transaction{}, utils.StackError(err, "Loading new transaction failed")
	}
	err = trans.SetUser(self.rntm.currentUser)
	if err != nil {
		return transaction{}, utils.StackError(err, "Setting user for new transaction failed")
	}

	self.transactions[self.rntm.currentUser] = trans

	return trans, nil
}

func (self *TransactionManager) loadTransaction(id datastore.ListEntry) (transaction, error) {

	var key [32]byte
	err := id.ReadType(&key)
	if err != nil {
		return transaction{}, utils.StackError(err, "Unable to load transaction id from store")
	}
	set, err := self.rntm.datastore.GetOrCreateSet(datastore.ListType, false, key)
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
	set, err = self.rntm.datastore.GetOrCreateSet(datastore.ValueType, false, key)
	if err != nil {
		return transaction{}, err
	}
	valueSet := set.(*datastore.ValueSet)
	user, err := valueSet.GetOrCreateValue([]byte("user"))
	if err != nil {
		return transaction{}, err
	}

	return transaction{id, *objects, *user, self.rntm}, nil
}

/*********************************************************************************
								Behaviour
*********************************************************************************/
type transactionBehaviour struct {
	*behaviour

	inTransaction bool
	current       transaction
}

func NewTransactionBehaviour(name string, parent identifier, rntm *Runtime) Object {

	behaviour, _ := NewBehaviour(parent, name, `TransactionBehaviour`, rntm)

	//add default events
	behaviour.AddEvent(`onParticipation`, NewEvent(rntm.jsvm)) //called when added to a transaction
	behaviour.AddEvent(`onClosing`, NewEvent(rntm.jsvm))       //called when transaction, to which the parent was added, is closed (means finished)
	behaviour.AddEvent(`onFailure`, NewEvent(rntm.jsvm))       //called when adding to transaction failed, e.g. because already in annother transaction

	tbhvr := &transactionBehaviour{behaviour, false, transaction{}}

	//add default methods for overriding by the user
	tbhvr.defaults.AddMethod("CanBeAdded", MustNewMethod(tbhvr.defaultAddable))                //return true/false if object can be used in current transaction
	tbhvr.defaults.AddMethod("CanBeClosed", MustNewMethod(tbhvr.defaultCloseable))             //return true/false if transaction containing the object can be closed
	tbhvr.defaults.AddMethod("DependendObjects", MustNewMethod(tbhvr.defaultDependendObjects)) //return array of objects that need also to be added to transaction

	//add the user usable methods
	tbhvr.AddMethod("InTransaction", MustNewMethod(tbhvr.InTransaction))

	return tbhvr
}

func (self *transactionBehaviour) InTransaction() bool {
	return self.inTransaction
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
