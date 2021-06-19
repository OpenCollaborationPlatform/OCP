//Special behaviour that describes the transaction handling

package dml

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"crypto/sha256"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/dop251/goja"
	uuid "github.com/satori/go.uuid"
)

func init() {
	gob.Register(new([32]byte))
}

/*********************************************************************************
								Object
*********************************************************************************/
//convinience object to abstract transaction database io. Not accassible by user
type transaction struct {

	//static data
	identification [32]byte
	rntm           *Runtime

	//dynamic state
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

func (self transaction) Behaviours() ([]dmlSet, error) {

	entries, err := self.objects.GetValues()
	if err != nil {
		return make([]dmlSet, 0), err
	}
	result := make([]dmlSet, 0)

	for _, entry := range entries {

		id, err := entry.Read()
		if err != nil {
			return result, err
		}

		obj, err := self.rntm.getObjectSet(*id.(*Identifier))
		if err != nil {
			return result, utils.StackError(err, "Unable to get Object for stored ID")
		}
		result = append(result, obj)
	}
	return result, nil
}

func (self transaction) HasBehaviour(id Identifier) bool {

	entries, err := self.objects.GetValues()
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {

		obj_id, err := entry.Read()
		if err != nil {
			continue
		}

		if obj_id.(*Identifier).Equals(id) {
			return true
		}
	}

	return false
}

//Adds object to the transaction. Note: Already having it is not a error to avoid
//the need of excessive checking
func (self transaction) AddBehaviour(id Identifier) error {

	if self.HasBehaviour(id) {
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

//implements BehaviourManager
type TransactionManager struct {
	methodHandler

	rntm  *Runtime
	jsobj *goja.Object
}

func NewTransactionManager(rntm *Runtime) (*TransactionManager, error) {

	mngr := &TransactionManager{NewMethodHandler(), rntm, nil}

	//setup default methods
	mngr.AddMethod("IsOpen", MustNewMethod(mngr.IsOpen, true))
	mngr.AddMethod("Open", MustNewMethod(mngr.Open, false))
	mngr.AddMethod("Close", MustNewMethod(mngr.Close, false))
	mngr.AddMethod("Abort", MustNewMethod(mngr.Abort, false))

	//build js object
	mngr.jsobj = rntm.jsvm.NewObject()
	err := mngr.SetupJSMethods(mngr.rntm, mngr.jsobj)
	if err != nil {
		return &TransactionManager{}, utils.StackError(err, "Unable to expose TransactionManager methods to javascript")
	}

	return mngr, nil
}

func (self *TransactionManager) transactionMap() (*datastore.Map, error) {

	set, err := self.rntm.datastore.GetOrCreateSet(datastore.MapType, false, internalKey)
	if err != nil {
		return nil, utils.StackError(err, "Cannot acccess internal map datastore")
	}
	mapSet := set.(*datastore.MapSet)
	map_, err := mapSet.GetOrCreateMap([]byte("transactions"))
	if err != nil {
		return nil, utils.StackError(err, "Cannot access internal transaction map")
	}
	return map_, nil
}

func (self *TransactionManager) GetJSObject() *goja.Object {
	return self.jsobj
}

func (self *TransactionManager) GetJSRuntime() *goja.Runtime {
	return self.rntm.jsvm
}

func (self *TransactionManager) CanHandleEvent(event string) bool {

	switch event {
	case "onBeforePropertyChange", "onBeforeChange":
		return true
	}

	return false
}

//returns if currently a transaction is open
func (self *TransactionManager) IsOpen() bool {
	transactions, err := self.transactionMap()
	if err != nil {
		return false
	}
	return transactions.HasKey(self.rntm.currentUser)
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

	return err
}

//Close a transaction
func (self *TransactionManager) Close() error {

	trans, err := self.getTransaction()
	if err != nil {
		return utils.StackError(err, "No transaction available to be closed")
	}

	//iterate over all objects and call the close event
	bhvrs, err := trans.Behaviours()
	if err != nil {
		return err
	}
	for _, set := range bhvrs {

		//the object is not part of a transaction anymore
		bhvr := set.obj.(transactionBehaviour)
		err := bhvr.closeTransaction(set.id, trans.identification)
		if err != nil {
			return utils.StackError(err, "Unable to close transaction for object", "Object", set.id)
		}
	}

	//remove from db
	err = trans.Remove()
	if err != nil {
		return utils.StackError(err, "Removing transaction from database failed")
	}
	transactions, err := self.transactionMap()
	if err != nil {
		return utils.StackError(err, "Unable to access transactions in DB")
	}
	keys, _ := transactions.GetKeys()
	for _, key := range keys {
		data, _ := transactions.Read(key)
		transkey := *(data.(*[32]byte))
		if bytes.Equal(transkey[:], trans.identification[:]) {
			transactions.Remove(key)
			break
		}
	}

	//remove from map
	transactions.Remove(self.rntm.currentUser)

	return nil
}

//Aborts the current transaction and reverts all objects to the state they had when adding to the transaction
func (self *TransactionManager) Abort() error {

	trans, err := self.getTransaction()
	if err != nil {
		return utils.StackError(err, "No transaction available to be closed")
	}

	//iterate over all objects
	bhvrs, err := trans.Behaviours()
	if err != nil {
		return err
	}
	for _, set := range bhvrs {

		//the object is not part of a transaction anymode
		bhvr := set.obj.(transactionBehaviour)
		err := bhvr.abortTransaction(set.id, trans.identification)
		if err != nil {
			return utils.StackError(err, "Unable to abort transaction for object", "Object", set.id)
		}
	}

	//remove from db
	err = trans.Remove()
	if err != nil {
		return utils.StackError(err, "Removing transaction from database failed")
	}
	transactions, err := self.transactionMap()
	if err != nil {
		return utils.StackError(err, "Unable to access transactions in DB")
	}
	keys, _ := transactions.GetKeys()
	for _, key := range keys {
		data, _ := transactions.Read(key)
		transkey := *(data.(*[32]byte))
		if bytes.Equal(transkey[:], trans.identification[:]) {
			transactions.Remove(key)
			break
		}
	}

	//remove from map
	transactions.Remove(self.rntm.currentUser)

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

	transactions, err := self.transactionMap()
	if err != nil {
		return transaction{}, utils.StackError(err, "Unable to access transactions in DB")
	}

	if !transactions.HasKey(self.rntm.currentUser) {
		return transaction{}, fmt.Errorf("No transaction available for user")
	}

	key, err := transactions.Read(self.rntm.currentUser)
	if err != nil {
		return transaction{}, utils.StackError(err, "Faied to access user transaction")
	}

	return loadTransaction(*(key.(*[32]byte)), self.rntm)
}

//opens a new transaction for the current user (without handling the old one if any)
func (self *TransactionManager) newTransaction() (transaction, error) {

	transactions, err := self.transactionMap()
	if err != nil {
		return transaction{}, utils.StackError(err, "Unable to access transactions in DB")
	}

	id := uuid.NewV4()
	key := sha256.Sum256(id.Bytes())
	err = transactions.Write(self.rntm.currentUser, key)
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

	return trans, nil
}

func getTransactionBehaviour(set dmlSet) (transactionBehaviour, Identifier) {

	data, ok := set.obj.(Data)
	if !ok {
		return nil, Identifier{}
	}

	//must have the transaction behaviour
	if !data.HasBehaviour("Transaction") {
		return nil, Identifier{}
	}
	set, _ = data.GetBehaviour(set.id, "Transaction")
	return set.obj.(transactionBehaviour), set.id
}

/*********************************************************************************
								Behaviour
*********************************************************************************/
var inTransKey []byte = []byte("__inTransaction")
var curTransKey []byte = []byte("__currentTransaction")

type transactionBehaviour interface {
	closeTransaction(Identifier, [32]byte) error //called when a transaction, which holds the behaviour, is closed
	abortTransaction(Identifier, [32]byte) error //called when a transaction, which holds the behaviour, is abortet
}

type objectTransaction struct {
	*behaviour

	mngr *TransactionManager
}

func NewObjectTransactionBehaviour(rntm *Runtime) (Object, error) {

	behaviour, _ := NewBaseBehaviour(rntm)

	//get the datastores
	/*set, err := rntm.datastore.GetOrCreateSet(datastore.ValueType, false, behaviour.Id().Hash())
	if err != nil {
		return nil, err
	}
	vset := set.(*datastore.ValueSet)
	inTrans, _ := vset.GetOrCreateValue(transKey)
	curTrans, _ := vset.GetOrCreateValue([]byte("__currentTransaction"))
	*/
	mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
	tbhvr := &objectTransaction{behaviour, mngr}

	//add default properties
	tbhvr.AddProperty(`automatic`, MustNewDataType("bool"), false, false) //open transaction automatically on change

	//add default methods for overriding by the user
	tbhvr.AddMethod("CanBeAdded", MustNewIdMethod(tbhvr.defaultAddable, true))                //return true/false if object can be used in current transaction
	tbhvr.AddMethod("CanBeClosed", MustNewIdMethod(tbhvr.defaultCloseable, true))             //return true/false if transaction containing the object can be closed
	tbhvr.AddMethod("DependentObjects", MustNewIdMethod(tbhvr.defaultDependentObjects, true)) //return array of objects that need also to be added to transaction

	//add default events
	tbhvr.AddEvent(NewEvent(`onParticipation`, behaviour)) //called when added to a transaction
	tbhvr.AddEvent(NewEvent(`onClosing`, behaviour))       //called when transaction, to which the parent was added, is closed (means finished)
	tbhvr.AddEvent(NewEvent(`onAborting`, behaviour))      //Caled when the transaction, the object is part of. is aborted (means reverted)
	tbhvr.AddEvent(NewEvent(`onFailure`, behaviour))       //called when adding to transaction failed, e.g. because already in annother transaction

	//add the user usable methods
	tbhvr.AddMethod("Add", MustNewIdMethod(tbhvr.add, false))                                      //Adds the object to the current transaction
	tbhvr.AddMethod("InTransaction", MustNewIdMethod(tbhvr.InTransaction, true))                   //behaviour is in any transaction, also other users?
	tbhvr.AddMethod("InCurrentTransaction", MustNewIdMethod(tbhvr.InCurrentTransaction, true))     //behaviour is in currently open transaction for user?
	tbhvr.AddMethod("InDifferentTransaction", MustNewIdMethod(tbhvr.InDifferentTransaction, true)) //behaviour is in currently open transaction for user?

	return tbhvr, nil
}

func (self *objectTransaction) GetBehaviourType() string {
	return "Transaction"
}

func (self *objectTransaction) HandleEvent(id Identifier, event string) error {

	//whenever a property or the object itself changed, we add ourself to the current transaction
	switch event {
	case "onBeforePropertyChange", "onBeforeChange":
		err := self.add(id)
		if err != nil {
			return err
		}
	}
	return nil
}

//Adds a new object to the current transaction. Fails if object is already part of
//annother transaction
//Note: If transaction has Object already no error is returned
func (self *objectTransaction) add(id Identifier) error {

	parent, err := self.GetParent(id)
	if err != nil {
		return utils.StackError(err, "Unable to get parent objecct of transaction behaviour")
	}

	trans, err := self.mngr.getTransaction()
	if err != nil {
		//seems we do not have a transaction open. Let's check if we shall open one
		if self.GetProperty("automatic").GetValue(id).(bool) {
			err = self.mngr.Open()
			if err == nil {
				trans, err = self.mngr.getTransaction()
			}
		}

		if err != nil {
			err = utils.StackError(err, "Unable to add object to transaction: No transaction open")
			self.GetEvent("onFailure").Emit(id, err.Error())
			return err
		}
	}

	//check if object is not already in annother transaction
	different, err := self.InDifferentTransaction(id)
	if err != nil {
		return utils.StackError(err, "Unable to check if object is in transaction")
	}
	if different {
		err = newUserError(Error_Operation_Invalid, "Object already part of different transaction")
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
	}

	//check if we are already in this transaction
	if trans.HasBehaviour(id) {
		return nil
	}

	//check if it is allowed by the object
	res, err := self.GetMethod("CanBeAdded").CallBoolReturn(id)
	if err != nil {
		err = utils.StackError(err, "Calling CanBeAdded failed")
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
	}
	if !res {
		err = newUserError(Error_Operation_Invalid, "Object cannot be added to transaction according to \"CanBeAdded\"")
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
	}

	err = trans.AddBehaviour(id)
	if err != nil {
		err = utils.StackError(err, "Unable to add object to transaction")
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
	}

	//store in the behaviour
	err = self.setCurrent(id, trans.identification)
	if err != nil {
		return utils.StackError(err, "Unable to write transaction to behaviour")
	}
	err = self.setInTransaction(id, true)
	if err != nil {
		return err
	}

	//make sure we have a fixed state at the beginning of the transaction (required for revert later)
	if has, _ := parent.obj.HasUpdates(parent.id); has {
		_, err = parent.obj.FixStateAsVersion(parent.id)
		if err != nil {
			return utils.StackError(err, "Unable to fix current state as version")
		}
	}
	if self.GetProperty("recursive").GetValue(id).(bool) {
		err = self.recursiveFixVersionTransaction(parent)
		if err != nil {
			return utils.StackError(err, "Unable to fix current state as version")
		}
	}

	//throw relevant event
	err = self.GetEvent("onParticipation").Emit(id)
	if err != nil {
		return err
	}

	//add the requried additional objects to the transaction
	list, err := self.GetMethod("DependentObjects").Call(id)
	if err != nil {
		return utils.StackError(err, "Error in \"DependentObjects\" function")
	}
	objs, ok := list.([]interface{})
	if !ok {
		err = newUserError(Error_Operation_Invalid, "Invalid \"DependentObjects\" function: return value must be list of objects, not %T", list)
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
	}
	for _, obj := range objs {
		dat, ok := obj.(Identifier)
		if !ok {
			err = newUserError(Error_Operation_Invalid, "Only objects are allowed to be added to transactions, not %T", obj)
			self.GetEvent("onFailure").Emit(id, err.Error())
			return err
		}
		obj, err := self.rntm.getObjectSet(dat)
		if err != nil {
			return err
		}
		var bhvr dmlSet
		data, ok := obj.obj.(Data)
		if ok {
			bhvr, err = data.GetBehaviour(obj.id, "Transaction")
			if err != nil {
				return newUserError(Error_Operation_Invalid, "Provided object does not have Transaction behaviour, cannot be added", "Object", obj.id)
			}
		} else {
			return newUserError(Error_Operation_Invalid, "List of dependent objects contain behaviours ")
		}
		//for now only allow object transactions
		_, ok = bhvr.obj.(*objectTransaction)
		if !ok {
			return newUserError(Error_Operation_Invalid, "Partial transaction behaviour cannot be dependent object")
		}
		err = self.add(bhvr.id)
		if err != nil {
			return utils.StackError(err, "Unable to add dependend object")
		}
	}

	return nil
}

func (self *objectTransaction) InTransaction(id Identifier) (bool, error) {

	inTransaction, err := self.GetDBValue(id, inTransKey)
	if err != nil {
		return false, utils.StackError(err, "Unable to read transaction status from DB")
	}

	if !inTransaction.IsValid() {
		return false, nil
	}

	res, err := inTransaction.Read()
	if err != nil {
		return false, utils.StackError(err, "Cannot check if in transaction")
	}
	if res == nil {
		return false, nil
	}
	return res.(bool), nil
}

func (self *objectTransaction) setInTransaction(id Identifier, value bool) error {

	inTransaction, err := self.GetDBValue(id, inTransKey)
	if err != nil {
		return utils.StackError(err, "Unable to read transaction status from DB")
	}
	return utils.StackError(inTransaction.Write(value), "Unable to write transaction status")
}

func (self *objectTransaction) InCurrentTransaction(id Identifier) (bool, error) {

	if in, err := self.InTransaction(id); err != nil || !in {
		return false, nil
	}

	trans, err := self.GetTransaction(id)
	if err != nil {
		return false, err
	}

	current, err := self.mngr.getTransaction()
	if err != nil {
		return false, err
	}

	return current.Equal(trans), nil
}

func (self *objectTransaction) InDifferentTransaction(id Identifier) (bool, error) {

	if in, err := self.InTransaction(id); err != nil || !in {
		return false, nil
	}

	trans, err := self.GetTransaction(id)
	if err != nil {
		return false, err
	}

	current, err := self.mngr.getTransaction()
	if err != nil {
		return false, err
	}

	return !current.Equal(trans), nil
}

func (self *objectTransaction) GetTransaction(id Identifier) (transaction, error) {

	current, err := self.GetDBValue(id, curTransKey)
	if err != nil {
		return transaction{}, utils.StackError(err, "Unable to read transaction from DB")
	}

	key, _ := current.Read()
	trans, err := loadTransaction(*(key.(*[32]byte)), self.rntm)
	if err != nil {
		return transaction{}, err
	}
	return trans, nil
}

func (self *objectTransaction) setCurrent(id Identifier, transIdent [32]byte) error {

	trans, err := self.GetDBValue(id, curTransKey)
	if err != nil {
		return utils.StackError(err, "Unable to get transaction from DB")
	}
	return utils.StackError(trans.Write(transIdent), "Unable to write transaction status")
}

func (self *objectTransaction) InitializeDB(id Identifier) error {

	err := self.object.InitializeDB(id)
	if err != nil {
		return err
	}

	trans, err := self.GetDBValue(id, curTransKey)
	if err != nil {
		return err
	}
	if ok, _ := trans.WasWrittenOnce(); !ok {
		trans.Write([32]byte{})
	}

	inTransaction, err := self.GetDBValue(id, inTransKey)
	if err != nil {
		return err
	}
	if ok, _ := inTransaction.WasWrittenOnce(); !ok {
		inTransaction.Write(false)
	}

	return nil
}

func (self *objectTransaction) defaultAddable(id Identifier) bool {
	return true
}

func (self *objectTransaction) defaultCloseable(id Identifier) bool {
	return true
}

func (self *objectTransaction) defaultDependentObjects(id Identifier) []interface{} {
	return make([]interface{}, 0)
}

func (self *objectTransaction) closeTransaction(id Identifier, transIdent [32]byte) error {

	err := self.GetEvent("onClosing").Emit(id)
	if err != nil {
		return utils.StackError(err, "Unable to close transaction due to failed close event emitting")
	}
	//the object is not part of a transaction anymode
	self.setInTransaction(id, false)

	//fix the new state as version
	set, err := self.GetParent(id)
	if err != nil {
		return err
	}
	if updates, _ := set.obj.HasUpdates(set.id); updates {
		set.obj.FixStateAsVersion(set.id)
	}
	if self.GetProperty("recursive").GetValue(id).(bool) {
		self.recursiveFixVersionTransaction(set)
	}
	return nil
}

// calls FixStateAsVersion for all childs of the provided dmlSet, and recursively for their
// childs too. Notes:
// - Calls not for the provided object itself
// - Stops recursion on objects having a Transaction behaviour (and does not call fix for those)
// - Does call on Behaviours
func (self *objectTransaction) recursiveFixVersionTransaction(set dmlSet) error {

	data, ok := set.obj.(Data)
	if ok {
		sets, err := data.GetSubobjects(set.id)
		if err != nil {
			return utils.StackError(err, "Unable to access children of dataobject")
		}
		for _, set := range sets {
			data, ok := set.obj.(Data)
			if ok && data.HasBehaviour("Transaction") {
				continue
			}
			if updates, _ := set.obj.HasUpdates(set.id); updates {
				_, err := set.obj.FixStateAsVersion(set.id)
				if err != nil {
					return err
				}
			}

			err := self.recursiveFixVersionTransaction(set)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *objectTransaction) abortTransaction(id Identifier, transIdent [32]byte) error {

	//infrm abort. We do not care about errors, aborts are not cancable
	self.GetEvent("onAborting").Emit(id)

	//the object is not part of a transaction anymore
	self.setInTransaction(id, false)

	//revert to the old state
	set, err := self.GetParent(id)
	if err != nil {
		return err
	}
	set.obj.ResetHead(set.id)
	if self.GetProperty("recursive").GetValue(id).(bool) {
		self.recursiveResetTransaction(set)
	}
	return nil
}

// calls RevertHead for all childs of the provided dmlSet, and recursively for their
// childs too. Notes:
// - Calls not for the provided object itself
// - Stops recursion on objects having a Transaction behaviour (and does not call revert for those)
// - Does call on Behaviours
func (self *objectTransaction) recursiveResetTransaction(set dmlSet) error {

	data, ok := set.obj.(Data)
	if ok {
		sets, err := data.GetSubobjects(set.id)
		if err != nil {
			return utils.StackError(err, "Unable to access children of dataobject")
		}
		for _, set := range sets {
			data, ok := set.obj.(Data)
			if ok && data.HasBehaviour("Transaction") {
				continue
			}
			if updates, _ := set.obj.HasUpdates(set.id); updates {
				err := set.obj.ResetHead(set.id)
				if err != nil {
					return err
				}
			}

			err := self.recursiveResetTransaction(set)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
