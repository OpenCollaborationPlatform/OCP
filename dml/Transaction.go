//Special behaviour that describes the transaction handling

package dml

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"

	"crypto/sha256"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"

	"github.com/dop251/goja"
	uuid "github.com/satori/go.uuid"
)

func init() {
	gob.Register(new([32]byte))
	gob.Register(new(transSet))
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

/* +extract target:systems

.. dml:system:: Transaction

	The transaction system provides access control for data objects. A user can open
	a transaction and make data objects part of it, and other users are then prevented
	from chaning whatever is part of the transaction.

	The general idea is to give the user a way to make multiple successive manipulations
	on the datastructure, possibly over long time with many calls, in an atomic way.
	For this he needs the gurantee, that only his calls change certain data, and
	noone else can make any unforseen changes he needs to react to. To achieve this
	the manipulated data can be made part of a transaction, which prevents anyone else
	to manipulate it.

	To enable data objects for transaction use it needs to have one of the transaction
	behaviours added. In those behaviours the detailed handling of the Object withing the
	transaction can be specified.

	.. note:: The dml transactions are different to SQL transactions as they are open
			  for successive user calls, without a time limit. This does however also
			  mean, that the user needs to take care of closing it correctly.

*/

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
	case "onBeforePropertyChange", "onBeforeChange", "onNewSubobject":
		return true
	}

	return false
}

/* +extract target:systems indent:1

.. dml:function:: IsOpen()

	Checks if the user has currently a transaction open

	:return bool open: True if a transaction is open

*/
func (self *TransactionManager) IsOpen() bool {
	transactions, err := self.transactionMap()
	if err != nil {
		return false
	}
	return transactions.HasKey(self.rntm.currentUser)
}

/* +extract target:systems indent:1

.. dml:function:: Open()

	Opens a transaction. If one is already open, it will be closed first.
*/
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

/* +extract target:systems indent:1

.. dml:function:: Close()

	Closes the currently open transaction.
*/
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

/* +extract target:systems indent:1

.. dml:function:: Abort()

	Aborts the current transaction and reverts all objects to the state they had when adding to the transaction
*/
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

type transactionBehaviour interface {
	closeTransaction(Identifier, [32]byte) error //called when a transaction, which holds the behaviour, is closed
	abortTransaction(Identifier, [32]byte) error //called when a transaction, which holds the behaviour, is abortet
}

//Both transaction types, object and partial, need to handle new subobjects and reverting those. Hence we abstract that in baseTransaction
type baseTransaction struct {
	*behaviour
	mngr *TransactionManager
}

var subKey []byte = []byte("__transactionSubObjects_")

func newBaseTransaction(rntm *Runtime) (*baseTransaction, error) {
	behaviour, err := NewBaseBehaviour(rntm)
	if err != nil {
		return nil, err
	}
	mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)

	tbhvr := &baseTransaction{behaviour, mngr}
	tbhvr.AddProperty(`automatic`, MustNewDataType("bool"), false, true) //open transaction automatically on change
	return tbhvr, nil
}

//add a new Identifier to the transaction
//id: the behaviour id wo which to add
//source: the source object which gets the new subobject (could be parent, or any child of it if recursive)
//sub: the subobject added to source
func (self *baseTransaction) addNewSubobject(id, source, sub Identifier, trans [32]byte) error {

	subMap, err := self.GetDBMap(id, bytes.Join([][]byte{subKey, trans[:]}, []byte{}))
	if err != nil {
		return err
	}

	err = subMap.Write(sub, source)
	if err != nil {
		utils.StackError(err, "Unable to add new subobject to transaction")
	}

	subSet, err := self.rntm.getObjectSet(sub)
	if err != nil {
		return err
	}
	return recursiveSubFix(subSet)
}

func recursiveSubFix(set dmlSet) error {

	_, err := set.obj.FixStateAsVersion(set.id)
	if err != nil {
		return err
	}

	if data, ok := set.obj.(Data); ok {
		subs, err := data.GetSubobjects(set.id)
		if err != nil {
			return err
		}
		for _, sub := range subs {
			if err = recursiveSubFix(sub); err != nil {
				return err
			}

		}
	}
	return nil
}

//clears all new sub objects stored
func (self *baseTransaction) clear(id Identifier, trans [32]byte) error {

	//remove all entries from the list (simply erase, is the fastes way of doing it
	subMap, err := self.GetDBMap(id, bytes.Join([][]byte{subKey, trans[:]}, []byte{}))
	if err != nil {
		return err
	}
	subMap.Erase()

	return nil
}

func recursiveSubErase(set dmlSet) error {

	//delete subs first, so that we do not need to access the db after erasure
	if data, ok := set.obj.(Data); ok {
		subs, err := data.GetSubobjects(set.id)
		if err != nil {
			return err
		}
		for _, sub := range subs {
			err := recursiveSubErase(sub)
			if err != nil {
				return err
			}
		}
	}

	return set.obj.EraseFromDB(set.id)
}

//erases all new subobjects from the datstore
func (self *baseTransaction) eraseSubobjects(id Identifier, trans [32]byte) error {

	subMap, err := self.GetDBMap(id, bytes.Join([][]byte{subKey, trans[:]}, []byte{}))
	if err != nil {
		return err
	}

	keys, err := subMap.GetKeys()
	if err != nil {
		return err
	}
	for _, key := range keys {

		sub, ok := key.(*Identifier)
		if !ok {
			return newInternalError(Error_Fatal, "Transaction Identifier stored in wrong format")
		}

		sset, err := self.rntm.getObjectSet(*sub)
		if err != nil {
			return err
		}
		if err := recursiveSubErase(sset); err != nil {
			return err
		}

	}

	return nil
}

func (self *baseTransaction) getNewSubobjects(id Identifier, trans [32]byte) ([]Identifier, error) {

	result := make([]Identifier, 0)
	subMap, err := self.GetDBMap(id, bytes.Join([][]byte{subKey, trans[:]}, []byte{}))
	if err != nil {
		return nil, err
	}

	keys, err := subMap.GetKeys()
	if err != nil {
		return nil, err
	}
	result = make([]Identifier, len(keys))
	for i, key := range keys {
		sub, ok := key.(*Identifier)
		if !ok {
			return nil, newInternalError(Error_Fatal, "Transaction data stored in wrong format")
		}
		result[i] = *sub
	}

	return result, nil
}

func (self *baseTransaction) eraseUnreferenced(id Identifier, trans [32]byte) error {

	sourceMap := make(map[Identifier]dmlSet, 0)

	subMap, err := self.GetDBMap(id, bytes.Join([][]byte{subKey, trans[:]}, []byte{}))
	if err != nil {
		return err
	}

	keys, err := subMap.GetKeys()
	if err != nil {
		return err
	}

	for _, key := range keys {

		sub, ok := key.(*Identifier)
		if !ok {
			return newInternalError(Error_Fatal, "Transaction data stored in wrong format")
		}

		data, err := subMap.Read(key)
		if err != nil {
			return err
		}
		source, ok := data.(*Identifier)
		if !ok {
			return newInternalError(Error_Fatal, "Transaction data stored in wrong format")
		}

		//get the source dmlSet
		sourceSet, ok := sourceMap[*source]
		if !ok {
			sourceSet, err = self.rntm.getObjectSet(*source)
			if err != nil {
				return err
			}
			sourceMap[*source] = sourceSet
		}

		//check if the subobject id is within all the subobjects of source
		if sdata, ok := sourceSet.obj.(Data); ok {

			allSubs, err := sdata.GetSubobjects(sourceSet.id)
			if err != nil {
				return err
			}
			referenced := false
			for _, allSub := range allSubs {
				if allSub.id.Equals(*sub) {
					referenced = true
					break
				}
			}
			if !referenced {
				//erase that object and all its subs!
				subSet, err := self.rntm.getObjectSet(*sub)
				if err != nil {
					return err
				}
				if err := recursiveSubErase(subSet); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

//little common helper
func (self *baseTransaction) getOrOpenTransaction() (transaction, error) {

	trans, err := self.mngr.getTransaction()
	if err != nil {
		//seems we do not have a transaction open. Let's check if we shall open one
		auto, _ := self.GetProperty("automatic").GetValue(Identifier{}) //const, ignore error, id irrelevant
		if auto.(bool) {
			err = self.mngr.Open()
			if err == nil {
				trans, err = self.mngr.getTransaction()
			}
		}
	}
	return trans, err
}

/* +extract prio:4

.. dml:behaviour:: Transaction
	:derived: Behaviour

	Defines how a Object behaves in transactions. With this behaviour defined in a Object
	it will become part of the current transaction when a change occurs. If any other user tries
	to edit it an error will be raised and the action fails.

	Any change within the object does trigger the transaction behaviour, be it a set property
	or any Object internal change, like a new entry in a Map. If recursive is true, the same
	holds for any change in a child- or subobject. Note that a change in a child will not add the
	child to the current transaction, but the Object which has the behaviour defined.
*/

//Object Transaction adds a whole object to the current transaction. This happens on every change within the object,
//property or key, and if recursive == True also for every change of any subobject.
type objectTransaction struct {
	*baseTransaction
}

var inTransKey []byte = []byte("__inTransaction")
var curTransKey []byte = []byte("__currentTransaction")

func NewObjectTransactionBehaviour(rntm *Runtime) (Object, error) {

	base, _ := newBaseTransaction(rntm)
	tbhvr := &objectTransaction{base}

	//add default methods for overriding by the user
	tbhvr.AddMethod("CanBeAdded", MustNewIdMethod(tbhvr.defaultAddable, true))                //return true/false if object can be used in current transaction
	tbhvr.AddMethod("CanBeClosed", MustNewIdMethod(tbhvr.defaultCloseable, true))             //return true/false if transaction containing the object can be closed
	tbhvr.AddMethod("DependentObjects", MustNewIdMethod(tbhvr.defaultDependentObjects, true)) //return array of objects that need also to be added to transaction

	//add default events
	tbhvr.AddEvent(NewEvent(`onParticipation`, tbhvr)) //called when added to a transaction
	tbhvr.AddEvent(NewEvent(`onClosing`, tbhvr))       //called when transaction, to which the parent was added, is closed (means finished)
	tbhvr.AddEvent(NewEvent(`onAborting`, tbhvr))      //Caled when the transaction, the object is part of. is aborted (means reverted)
	tbhvr.AddEvent(NewEvent(`onFailure`, tbhvr))       //called when adding to transaction failed, e.g. because already in annother transaction

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

func (self *objectTransaction) HandleEvent(id Identifier, source Identifier, event string, args []interface{}) error {

	//whenever a property or the object itself changed, we add ourself to the current transaction
	//note that event can be a fully qualified path for recursive events
	switch event {
	case "onBeforePropertyChange", "onBeforeChange":
		err := self.add(id)

		if err != nil {
			return err
		}
	case "onNewSubobject":
		sub, ok := args[0].(Identifier)
		if !ok {
			return newInternalError(Error_Fatal, "newSubobjectEvent does not provide object identifier as argument")
		}
		trans, err := self.getOrOpenTransaction()
		if err != nil {
			err = utils.StackError(err, "Unable to add object to transaction: No transaction open")
			self.GetEvent("onFailure").Emit(id, err.Error())
			return err
		}
		self.baseTransaction.addNewSubobject(id, source, sub, trans.identification)
	}
	return nil
}

/* +extract prio:4 indent:1

.. dml:function:: Add()

	Adds the Object to the current transaction. If the Object is already part of the
	current transaction nothing happens, and no error is thrown.

	:throws: If no transaction is open for current user
	:throws: If Object is already part of a transaction of annother user

*/
func (self *objectTransaction) add(id Identifier) error {

	parent, err := self.GetParent(id)
	if err != nil {
		return utils.StackError(err, "Unable to get parent objecct of transaction behaviour")
	}

	trans, err := self.getOrOpenTransaction()
	if err != nil {
		err = utils.StackError(err, "Unable to add object to transaction: No transaction open")
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
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
	recursive, _ := self.GetProperty("recursive").GetValue(id) //const, ignore error!
	if recursive.(bool) {
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

/* +extract prio:4 indent:1

.. dml:function:: InTransaction()

	Checks if the Object is currently part in any transaction

	:return bool IsPart: True if in a transaction, false otherwise

*/
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

/* +extract prio:4 indent:1

.. dml:function:: InCurrentTransaction()

	Checks if the Object is currently part in the users transaction

	:return bool IsPart: True if in users transaction, false otherwise

*/
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

/* +extract prio:4 indent:1

.. dml:function:: InDifferentTransaction()

	Checks if the Object is currently part in a transaction of annother user

	:return bool IsPart: True if in other users transaction, false otherwise

*/
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

/* +extract prio:4 indent:1

.. dml:function:: CanBeAdded()
	:virtual:

	Allows custom logic for checking if the Object is allowed to take part in the
	current transaction. To be overriden and implemented by the user. If false is returned,
	the action that lead to the Object being added to the transaction fails.
	Note that it is not needed to check if the Object is already part of annother
	transaction, this is still done internally. This function is to be used for custom logic
	only.

	:return bool Possible: True if it is allowed to add object to current transaction

*/
func (self *objectTransaction) defaultAddable(id Identifier) bool {
	return true
}

/* +extract prio:4 indent:1

.. dml:function:: CanBeClosed()
	:virtual:

	Allows custom logic for checking if the transaction, in which the Object takes part,
	is allowed to be closed. To be overriden and implemented by the user. If false is returned,
	closing the transaction fails and it stays open. The implementation should make sure
	that the user is informed about the reasons, so that he can correct it bevore trying
	to close the transaction again.

	.. note:: This function is not called for aborting transactions. Aborting cannot
			  be prevented.

	:return bool Possible: True if it is allowed to close the current transaction

*/
func (self *objectTransaction) defaultCloseable(id Identifier) bool {
	return true
}

/* +extract prio:4 indent:1

.. dml:function:: DependentObjects()
	:virtual:

	Allows custom logic for defining dependent Objeccts. To be overriden and implemented
	by the user. This function will be called when the Object is added to the transaction,
	and the list of returned Objects will then be added to the current transaction too.

	:return List[Objecct] Dependencies: All Objects that need to also be part of the transaction

*/
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
	recursive, _ := self.GetProperty("recursive").GetValue(id) //const, ignore error
	if recursive.(bool) {
		self.recursiveFixVersionTransaction(set)
	}

	//check if we reference all of the new subobjects, and delete those that we do not.
	if err := self.baseTransaction.eraseUnreferenced(id, transIdent); err != nil {
		return err
	}
	return self.baseTransaction.clear(id, transIdent)
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
	recursive, _ := self.GetProperty("recursive").GetValue(id) //const, ignore error
	if recursive.(bool) {
		if err := self.recursiveResetTransaction(set); err != nil {
			return err
		}
	}

	//check if we reference all of the new subobjects, and delete those that we do not.
	if err := self.baseTransaction.eraseSubobjects(id, transIdent); err != nil {
		return err
	}
	return self.baseTransaction.clear(id, transIdent)
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

/* +extract prio:4 indent:1

.. dml:event:: onParticipation

	Emitted when the Object becomes part of the users current transaction. If a JavaScript
	callback for this event throws an error the adding of the Object fails, as well as the
	user action triggering it. Hence this can be used the same way as overriding *CanBeAdded*

.. dml:event:: onClosing

	Emitted when current transaction, of which the Object is a part, is closed. If a JavaScript
	callback for this event throws an error the transaction closing fails, as well as the
	user action triggering it. Hence this can be used the same way as overriding *CanBeClosed*

.. dml:event:: onAborting

	Emitted when current transaction, of which the Object is a part, is aborted. Errors in
	callbacks ar ignored, as aborting cannot be stopped.

.. dml:event:: onFailure

	Emitted when adding the object to the current transaction fails, independent of
	the reason for the fail.

	:arg str error: The error message describing why it failed
*/

/* +extract prio:5
.. dml:behaviour:: PartialTransaction
	:derived: Behaviour

	Defines how the objects individual keys behaves in transactions. With this behaviour defined in a object
	its keys can become part of that transaction. A key is any identifier pointing to data, like a property
	name, a number for a Vector or a key for a map. If the behaviour is recursive keys can also be stacked,
	like childname.mapkey

	Any change within the object does trigger the transaction behaviour, be it a set property
	or any Object internal change, like a new entry in a Map. If recursive is true, the same
	holds for any change in a child- or subobject. Note that a change in a child will not add the
	child to the current transaction, but the Object which has the behaviour defined.

	.. note:: Keys are relative paths from the behaviours parent object, e.g. MyChild.myProperty
*/
type partialTransaction struct {
	*baseTransaction
}

//little helper to store source and key in datastore
type transSet struct {
	Id  Identifier
	Key Key
}

var transMap []byte = []byte("__transactionMap")

func NewPartialTransactionBehaviour(rntm *Runtime) (Object, error) {

	baseTrans, _ := newBaseTransaction(rntm)
	tbhvr := &partialTransaction{baseTrans}

	//add default methods for overriding by the user
	//tbhvr.AddMethod("CanBeAdded", MustNewIdMethod(tbhvr.defaultAddable, true))             //return true/false if the provided key can be used in current transaction
	//tbhvr.AddMethod("CanBeClosed", MustNewIdMethod(tbhvr.defaultCloseable, true))          //return true/false if the current transaction containing some object keys can be closed
	//tbhvr.AddMethod("DependentKeys", MustNewIdMethod(tbhvr.defaultDependentObjects, true)) //return array of keys that need to be added to the transaction together with the provided one

	//add default events
	tbhvr.AddEvent(NewEvent(`onParticipation`, tbhvr)) //called when the first key is added to the current transaction
	tbhvr.AddEvent(NewEvent(`onKeyAdded`, tbhvr))      //called when a new key is added to the transaction
	tbhvr.AddEvent(NewEvent(`onClosing`, tbhvr))       //called when transaction, to which tany key was added, is closed (means finished)
	tbhvr.AddEvent(NewEvent(`onAborting`, tbhvr))      //Caled when the transaction, any key is part of. is aborted (means reverted)
	tbhvr.AddEvent(NewEvent(`onFailure`, tbhvr))       //called when adding to transaction failed, e.g. because already in annother transaction

	//add the user usable methods
	tbhvr.AddMethod("Add", MustNewIdMethod(tbhvr.keyAdd, false)) //Adds a given key to the current trankey is in any transaction, also other users?
	//tbhvr.AddMethod("InCurrentTransaction", MustNewIdMethod(tbhvr.InCurrentTransaction, true))     //behaviour is in currently open transaction for user?
	//tbhvr.AddMethod("InDifferentTransaction", MustNewIdMethod(tbhvr.InDifferentTransaction, true)) //behaviour is in currently open transaction for user?
	tbhvr.AddMethod("CurrentTransactionKeys", MustNewIdMethod(tbhvr.transactionKeys, true)) //Returns the keys in current transaction

	return tbhvr, nil
}

func (self *partialTransaction) GetBehaviourType() string {
	return "Transaction"
}

func (self *partialTransaction) HandleEvent(id Identifier, source Identifier, event string, args []interface{}) error {

	//whenever a property or the object itself changed, we add ourself to the current transaction.
	//note that event coud be a fully qualified path
	switch event {
	case "onBeforePropertyChange", "onBeforeChange":

		if len(args) != 1 {
			return newInternalError(Error_Operation_Invalid, "Partial transaction cannot handle event due to missing key argument", "event", event)
		}

		key, err := NewKey(args[0])
		if err != nil {
			utils.StackError(err, "Unable to use event argument as key", "event", event, "argument", args)
		}

		//check if we need to construct the path
		path := key.AsString()
		if !id.Equals(source) {
			idP, _ := self.GetObjectPath(id)
			sourceP, _ := self.GetObjectPath(source)
			path = sourceP[len(idP):] + "." + key.AsString()
		}

		err = self.add(id, source, key, path)
		if err != nil {
			return err
		}

	case "onNewSubobject":
		sub, ok := args[0].(Identifier)
		if !ok {
			return newInternalError(Error_Fatal, "newSubobjectEvent does not provide object identifier as argument")
		}
		trans, err := self.getOrOpenTransaction()
		if err != nil {
			err = utils.StackError(err, "Unable to add object to transaction: No transaction open")
			self.GetEvent("onFailure").Emit(id, err.Error())
			return err
		}
		self.baseTransaction.addNewSubobject(id, source, sub, trans.identification)
	}
	return nil
}

func (self *partialTransaction) keyToSourceKey(id Identifier, key interface{}) (Identifier, Key, error) {

	source, err := self.GetParent(id)
	if err != nil {
		return Identifier{}, Key{}, err
	}

	str, isstr := key.(string)
	if isstr {
		//could be a complex path which needs to be resolved
		path := strings.Split(str, ".")
		for i := 1; i < len(path); i++ {
			data, ok := source.obj.(Data)
			if !ok {
				return Identifier{}, Key{}, newInternalError(Error_Setup_Invalid, "Behaviour parent is not Data object")
			}
			source, err = data.GetChildByName(source.id, path[1])
			if err != nil {
				return Identifier{}, Key{}, err
			}
		}

		key = path[len(path)-1]
	}

	key_, err := NewKey(key)
	return source.id, key_, err
}

//Allows to add keys in the dml path way, e.g. "mychild.subobject.property1" or simple "key". Key is always relative to parent data object.
func (self *partialTransaction) keyAdd(id Identifier, key interface{}) error {

	//add to transaction
	source, key_, err := self.keyToSourceKey(id, key)
	if err != nil {
		return err
	}
	return self.add(id, source, key_, fmt.Sprintf("%v", key))
}

//Adds a new object/key combo to the current transaction. Fails if key is already part of
//annother transaction
//Note: If transaction has Object already no error is returned
func (self *partialTransaction) add(id Identifier, source Identifier, key Key, path string) error {

	trans, err := self.getOrOpenTransaction()
	if err != nil {
		err = utils.StackError(err, "Unable to add key to transaction: No transaction open")
		self.GetEvent("onFailure").Emit(id, err.Error())
		return err
	}

	//store the source/key pair in the behaviour
	tMap, err := self.GetDBMap(id, transMap)
	if err != nil {
		return utils.StackError(err, "Unable to access datastore of transaction")
	}
	//first check if we have the entry already.
	set := transSet{source, key}
	if tMap.HasKey(set) {
		val, err := tMap.Read(set)
		if err != nil {
			return utils.StackError(err, "Unable to read transaction key entry")
		}
		transID, ok := val.(*[32]byte)
		if !ok {
			return newInternalError(Error_Setup_Invalid, "Stored trasaction data has wrong format")
		}
		if bytes.Equal(transID[:], trans.identification[:]) {
			//we already have this key added, we return without error
			return nil

		} else {
			//key belongs to different transaction
			err := newUserError(Error_Operation_Invalid, "Key already belongs to different transaction")
			self.GetEvent("onFailure").Emit(id, err.Error())
			return err
		}
	}

	//add the entry!
	if err := tMap.Write(set, trans.identification); err != nil {
		return utils.StackError(err, "Unable to store key for transaction")
	}

	//check if we are already written in the transaction
	if !trans.HasBehaviour(id) {
		err = trans.AddBehaviour(id)
		if err != nil {
			err = utils.StackError(err, "Unable to add object to transaction")
			self.GetEvent("onFailure").Emit(id, err.Error())
			return err
		}

		//throw relevant event that we are now part of the transaction
		err = self.GetEvent("onParticipation").Emit(id)
		if err != nil {
			return err
		}
	}

	//make sure we have a fixed state at the beginning of the transaction (required for revert later)
	dmlset, err := self.rntm.getObjectSet(source)
	if err != nil {
		return err
	}
	//note that it could happen, that the key is not available (e.g. a map with not yet created entry)
	if has, _ := dmlset.obj.HasKey(source, key); has {
		dskeys, err := dmlset.obj.keyToDS(source, key)
		if err != nil {
			return utils.StackError(err, "Unable to access key in source object of transaction")
		}
		for _, dskey := range dskeys {
			if dskey.Versioned {
				ventry, err := self.rntm.datastore.GetVersionedEntry(dskey)
				if err != nil {
					return utils.StackError(err, "Unable to access versioned entry based on ds key")
				}
				if upd, _ := ventry.HasUpdates(); upd {
					ventry.FixStateAsVersion()
				}
			}
		}
	}

	self.GetEvent("onKeyAdded").Emit(id, path)
	return nil
}

func (self *partialTransaction) InitializeDB(id Identifier) error {

	err := self.object.InitializeDB(id)
	if err != nil {
		return err
	}

	return nil
}

func (self *partialTransaction) transactionKeys(id Identifier) ([]string, error) {

	//collect all keys for current transaction
	if !self.mngr.IsOpen() {
		return nil, newUserError(Error_Operation_Invalid, "No transaction open, hence cannot inquery keys")
	}
	trans, err := self.mngr.getTransaction()
	if err != nil {
		return nil, utils.StackError(err, "Unable to query transaction for key search")
	}

	//iterate over all keys, and fix those that belong to the current transaction
	tMap, err := self.GetDBMap(id, transMap)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access datastore of transaction")
	}
	keys, err := tMap.GetKeys()
	if err != nil {
		return nil, err
	}

	//get othe parent path to allow string key construction
	parent, err := self.GetParent(id)
	if err != nil {
		return nil, err
	}
	parentpath, err := parent.obj.GetObjectPath(parent.id)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0)
	for _, key := range keys {
		data, err := tMap.Read(key)
		if err != nil {
			return nil, err
		}
		keyTrans, ok := data.(*[32]byte)
		if !ok {
			return nil, newInternalError(Error_Setup_Invalid, "Transaction data of wrong type", "type", fmt.Sprintf("%T", data))
		}
		if bytes.Equal(keyTrans[:], trans.identification[:]) {

			//this object/key set belongs to the currently closed transaction
			transset, ok := key.(*transSet)
			if !ok {
				return nil, newInternalError(Error_Setup_Invalid, "Transaction set data of wrong type", "type", fmt.Sprintf("%T", key))
			}
			dmlset, err := self.rntm.getObjectSet(transset.Id)
			if err != nil {
				return nil, utils.StackError(err, "Object for changed key not valid")
			}

			sourcepath, err := dmlset.obj.GetObjectPath(dmlset.id)
			if err != nil {
				return nil, err
			}
			key := ""
			if len(sourcepath) != len(parentpath) {
				key = sourcepath[len(parentpath)+1:] + "."
			}
			result = append(result, key+transset.Key.AsString())
		}
	}
	return result, nil
}

func (self *partialTransaction) closeTransaction(id Identifier, transIdent [32]byte) error {

	err := self.GetEvent("onClosing").Emit(id)
	if err != nil {
		return utils.StackError(err, "Unable to close transaction due to failed close event emitting")
	}

	//iterate over all keys, and fix those that belong to the current transaction
	tMap, err := self.GetDBMap(id, transMap)
	if err != nil {
		return utils.StackError(err, "Unable to access datastore of transaction")
	}
	keys, err := tMap.GetKeys()
	if err != nil {
		return err
	}

	for _, key := range keys {
		data, err := tMap.Read(key)
		if err != nil {
			return err
		}
		keyTrans, ok := data.(*[32]byte)
		if !ok {
			return newInternalError(Error_Setup_Invalid, "Transaction data of wrong type", "type", fmt.Sprintf("%T", data))
		}
		if bytes.Equal(keyTrans[:], transIdent[:]) {

			//this object/key set belongs to the currently closed transaction
			transset, ok := key.(*transSet)
			if !ok {
				return newInternalError(Error_Setup_Invalid, "Transaction set data of wrong type", "type", fmt.Sprintf("%T", key))
			}
			dmlset, err := self.rntm.getObjectSet(transset.Id)
			if err != nil {
				return utils.StackError(err, "Object for changed key not valid")
			}
			dskeys, err := dmlset.obj.keyToDS(dmlset.id, transset.Key)
			if err != nil {
				return utils.StackError(err, "Unable to get DS keys from transaction key")
			}

			for _, dskey := range dskeys {
				if dskey.Versioned {
					entry, err := self.rntm.datastore.GetVersionedEntry(dskey)
					if err != nil {
						return utils.StackError(err, "Unable to get DS entry for transaction keys")
					}
					if has, _ := entry.HasUpdates(); has {
						_, err := entry.FixStateAsVersion()
						if err != nil {
							return utils.StackError(err, "Unable to fix updates in transaction key")
						}
					}
				}
			}

			//remove from key map
			err = tMap.Remove(key)
			if err != nil {
				return utils.StackError(err, "Unable to remove key from transaction while closing")
			}
		}
	}

	//handle the newly created subobjects
	if err := self.eraseUnreferenced(id, transIdent); err != nil {
		return utils.StackError(err, "Unable to erase unreferenced objects")
	}
	return self.clear(id, transIdent)
}

func (self *partialTransaction) abortTransaction(id Identifier, transIdent [32]byte) error {

	//infrm abort. We do not care about errors, aborts are not cancable
	self.GetEvent("onAborting").Emit(id)

	//iterate over all keys, and fix those that belong to the current transaction
	tMap, err := self.GetDBMap(id, transMap)
	if err != nil {
		return utils.StackError(err, "Unable to access datastore of transaction")
	}
	keys, err := tMap.GetKeys()
	if err != nil {
		return err
	}

	for _, key := range keys {
		data, err := tMap.Read(key)
		if err != nil {
			return err
		}
		keyTrans, ok := data.(*[32]byte)
		if !ok {
			return newInternalError(Error_Setup_Invalid, "Transaction data of wrong type", "type", fmt.Sprintf("%T", data))
		}
		if bytes.Equal(keyTrans[:], transIdent[:]) {

			//this object/key set belongs to the currently closed transaction
			transset, ok := key.(*transSet)
			if !ok {
				return newInternalError(Error_Setup_Invalid, "Transaction set data of wrong type", "type", fmt.Sprintf("%T", key))
			}
			dmlset, err := self.rntm.getObjectSet(transset.Id)
			if err != nil {
				return utils.StackError(err, "Object for changed key not valid")
			}
			dskeys, err := dmlset.obj.keyToDS(dmlset.id, transset.Key)
			if err != nil {
				return utils.StackError(err, "Unable to get DS keys from transaction key")
			}
			if len(dskeys) == 0 {
				return newInternalError(Error_Setup_Invalid, "User key does not correspond to any ds keys")
			}

			//we need to check, if the key can be reset, or if we should delete him. For that the logic is as following:
			// - all DS keys that are used for the dml key are assumed to be either new or already versioned, no mixture
			// - Hence if any DS key is unversioned, it is assumed the dml key is unversioned, and vice very
			// - If unversioned, it gets removed instead of reset
			// - It is aso assumed that all ds keys are either versioned or unversioned
			var reset bool = false
			if dskeys[0].Versioned {
				entry, err := self.rntm.datastore.GetVersionedEntry(dskeys[0])
				if err != nil {
					return utils.StackError(err, "Unable to get DS entry for transaction keys")
				}
				reset, err = entry.HasVersions()
				if err != nil {
					return utils.StackError(err, "Unable to inquery version state for transaction key")
				}
			}

			if reset {
				for _, dskey := range dskeys {
					if !dskey.Versioned {
						return newInternalError(Error_Setup_Invalid, "Transaction tries to reset unversioned key, something went wrong!")
					}
					entry, err := self.rntm.datastore.GetVersionedEntry(dskey)
					if err != nil {
						return utils.StackError(err, "Unable to get DS entry for transaction keys")
					}

					if upd, _ := entry.HasUpdates(); upd {
						err := entry.ResetHead()
						if err != nil {
							return utils.StackError(err, "Unable to revert updates in transaction key")
						}
					}
				}
			} else {
				//check if key corresponds to an object
				/*rep, err := dmlset.obj.GetByKey(dmlset.id, transset.Key)
				if err != nil {
					return utils.StackError(err, "Unable to access key in object")
				}*/
				for _, dskey := range dskeys {
					entry, err := self.rntm.datastore.GetEntry(dskey)
					if err != nil {
						return utils.StackError(err, "Unable to get DS entry for transaction keys")
					}
					err = entry.Erase()
					if err != nil {
						return utils.StackError(err, "Unable to erase datastore entry")
					}
				}
			}

			//remove from key map
			err = tMap.Remove(key)
			if err != nil {
				return utils.StackError(err, "Unable to remove key from transaction while closing")
			}
		}
	}

	//handle the newly created subobjects
	if err := self.eraseSubobjects(id, transIdent); err != nil {
		return utils.StackError(err, "Unable to erase objects created during transaction")
	}
	return self.clear(id, transIdent)
}
