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

func (self transaction) Objects() ([]dmlSet, error) {

	entries, err := self.objects.GetEntries()
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

		if obj_id.(*Identifier).Equals(id) {
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
	mngr.AddMethod("Add", MustNewMethod(mngr.Add, false))

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
	case "onPropertyChanged", "onChanged":
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
	objs, err := trans.Objects()
	if err != nil {
		return err
	}
	for _, set := range objs {

		//the object is not part of a transaction anymode
		bhvr, bhvrId := getTransactionBehaviour(set)

		if bhvr != nil {
			err = bhvr.GetEvent("onClosing").Emit(bhvrId)
			if err != nil {
				return utils.StackError(err, "Unable to close transaction due to failed close event emitting")
			}
			//the object is not part of a transaction anymode
			bhvr.setInTransaction(bhvrId, false)
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

//Adds a new object to the current transaction. Fails if object is already part of
//annother transaction
//Note: If transaction has Object already no error is returned
func (self *TransactionManager) Add(id Identifier) error {

	set, err := self.rntm.getObjectSet(id)
	if err != nil {
		return utils.StackError(err, "Unable to get object for id")
	}
	data, ok := set.obj.(Data)
	if !ok {
		return newUserError(Error_Operation_Invalid, "Cannot add behaviour to transaction, only data objects")
	}

	if !data.HasBehaviour("Transaction") {
		return newUserError(Error_Operation_Invalid, "Object has no transaction behaviour: cannot be added to transaction")
	}
	bhvrSet, _ := data.GetBehaviour(id, "Transaction")
	transBhvr, ok := bhvrSet.obj.(*transactionBehaviour)
	if !ok {
		return newInternalError(Error_Setup_Invalid, "Wrong behaviour type used in object")
	}

	trans, err := self.getTransaction()
	if err != nil {
		//seems we do not have a transaction open. Let's check if we shall open one
		if bhvrSet.obj.GetProperty("automatic").GetValue(bhvrSet.id).(bool) {
			err = self.Open()
			if err == nil {
				trans, err = self.getTransaction()
			}
		}

		if err != nil {
			err = utils.StackError(err, "Unable to add object to transaction: No transaction open")
			bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
			return err
		}
	}

	//check if object is not already in annother transaction
	ok, err = transBhvr.InTransaction(bhvrSet.id)
	if err != nil {
		return utils.StackError(err, "Unable to check if object is in transaction")
	}
	if ok {
		objTrans, err := transBhvr.GetTransaction(bhvrSet.id)
		if err != nil {
			return utils.StackError(err, "Unable to retreive transaction from behaviour")
		}
		if !objTrans.Equal(trans) {
			err = newUserError(Error_Operation_Invalid, "Object already part of different transaction")
			bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
			return err
		}
	}

	//check if we are already in this transaction
	if trans.HasObject(id) {
		return nil
	}

	//check if it is allowed by gthe object
	res, err := bhvrSet.obj.GetMethod("CanBeAdded").CallBoolReturn(bhvrSet.id)
	if err != nil {
		err = utils.StackError(err, "Calling CanBeAdded failed")
		bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
		return err
	}
	if res != true {
		err = newUserError(Error_Operation_Invalid, "Object cannot be added to transaction according to \"CanBeAdded\"")
		bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
		return err
	}

	err = trans.AddObject(id)
	if err != nil {
		err = utils.StackError(err, "Unable to add object to transaction")
		bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
		return err
	}

	//store in the behaviour
	err = transBhvr.setCurrent(bhvrSet.id, trans.identification)
	if err != nil {
		return utils.StackError(err, "Unable to write transaction to behaviour")
	}
	err = transBhvr.setInTransaction(bhvrSet.id, true)
	if err != nil {
		return err
	}

	//store state (required for revert)
	set.obj.FixStateAsVersion(set.id)

	//throw relevant event
	bhvrSet.obj.GetEvent("onParticipation").Emit(bhvrSet.id)

	//add the requried additional objects to the transaction
	list, err := bhvrSet.obj.GetMethod("DependentObjects").Call(bhvrSet.id)
	if err != nil {
		return utils.StackError(err, "Error in \"DependentObjects\" function")
	}
	objs, ok := list.([]interface{})
	if !ok {
		err = newUserError(Error_Operation_Invalid, "Invalid \"DependentObjects\" function: return value must be list of objects, not %T", list)
		bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
		return err
	}
	for _, obj := range objs {
		dat, ok := obj.(Identifier)
		if !ok {
			err = newUserError(Error_Operation_Invalid, "Only objects are allowed to be added to transactions, not %T", obj)
			bhvrSet.obj.GetEvent("onFailure").Emit(bhvrSet.id, err.Error())
			return err
		}
		err := self.Add(dat)
		if err != nil {
			return utils.StackError(err, "Unable to add dependend object")
		}
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

func getTransactionBehaviour(set dmlSet) (*transactionBehaviour, Identifier) {

	data, ok := set.obj.(Data)
	if !ok {
		return nil, Identifier{}
	}

	//must have the transaction behaviour
	if !data.HasBehaviour("Transaction") {
		return nil, Identifier{}
	}
	set, _ = data.GetBehaviour(set.id, "Transaction")
	return set.obj.(*transactionBehaviour), set.id
}

/*********************************************************************************
								Behaviour
*********************************************************************************/
var inTransKey []byte = []byte("__inTransaction")
var curTransKey []byte = []byte("__currentTransaction")

type transactionBehaviour struct {
	*behaviour

	mngr *TransactionManager
}

func NewTransactionBehaviour(rntm *Runtime) (Object, error) {

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
	tbhvr := &transactionBehaviour{behaviour, mngr}

	//add default properties
	tbhvr.AddProperty(`automatic`, MustNewDataType("bool"), false, false) //open transaction automatically on change

	//add default methods for overriding by the user
	tbhvr.AddMethod("CanBeAdded", MustNewIdMethod(tbhvr.defaultAddable, true))                //return true/false if object can be used in current transaction
	tbhvr.AddMethod("CanBeClosed", MustNewIdMethod(tbhvr.defaultCloseable, true))             //return true/false if transaction containing the object can be closed
	tbhvr.AddMethod("DependentObjects", MustNewIdMethod(tbhvr.defaultDependentObjects, true)) //return array of objects that need also to be added to transaction

	//add default events
	tbhvr.AddEvent(NewEvent(`onParticipation`, behaviour)) //called when added to a transaction
	tbhvr.AddEvent(NewEvent(`onClosing`, behaviour))       //called when transaction, to which the parent was added, is closed (means finished)
	tbhvr.AddEvent(NewEvent(`onFailure`, behaviour))       //called when adding to transaction failed, e.g. because already in annother transaction

	//add the user usable methods
	tbhvr.AddMethod("InTransaction", MustNewIdMethod(tbhvr.InTransaction, true))               //behaviour is in any transaction, also other users?
	tbhvr.AddMethod("InCurrentTransaction", MustNewIdMethod(tbhvr.InCurrentTransaction, true)) //behaviour is in currently open transaction for user?

	return tbhvr, nil
}

func (self *transactionBehaviour) HandleEvent(id Identifier, event string) {

	//whenever a property or the object itself changed, we add ourself to the current transaction
	switch event {
	case "onPropertyChanged", "onChanged":
		set, err := self.GetParent(id)
		if err == nil {
			self.mngr.Add(set.id)
		}
	}
}

func (self *transactionBehaviour) InTransaction(id Identifier) (bool, error) {

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

func (self *transactionBehaviour) setInTransaction(id Identifier, value bool) error {

	inTransaction, err := self.GetDBValue(id, inTransKey)
	if err != nil {
		return utils.StackError(err, "Unable to read transaction status from DB")
	}
	return utils.StackError(inTransaction.Write(value), "Unable to write transaction status")
}

func (self *transactionBehaviour) InCurrentTransaction(id Identifier) (bool, error) {

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

func (self *transactionBehaviour) GetTransaction(id Identifier) (transaction, error) {

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

func (self *transactionBehaviour) setCurrent(id Identifier, transIdent [32]byte) error {

	trans, err := self.GetDBValue(id, curTransKey)
	if err != nil {
		return utils.StackError(err, "Unable to get transaction from DB")
	}
	return utils.StackError(trans.Write(transIdent), "Unable to write transaction status")
}

func (self *transactionBehaviour) defaultAddable(id Identifier) bool {
	return true
}

func (self *transactionBehaviour) defaultCloseable(id Identifier) bool {
	return true
}

func (self *transactionBehaviour) defaultDependentObjects(id Identifier) []interface{} {
	return make([]interface{}, 0)
}
