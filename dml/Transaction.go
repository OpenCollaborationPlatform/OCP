//Special behaviour that describes the transaction handling

package dml

import (
	"CollaborationNode/datastores"
	"crypto/sha256"

	"github.com/satori/go.uuid"
)

/*********************************************************************************
								Object
*********************************************************************************/

type Transaction struct {
	identification datastore.ListEntry
	objects        datastore.List
	user           datastore.Value
	rntm           *Runtime
}

func NewTransaction(rntm *Runtime) (Transaction, error) {

	//create a new transaction
	var setKey [32]byte
	copy(setKey[:], []byte("internal"))
	set, err := rntm.datastore.GetOrCreateSet(datastore.ListType, false, setKey)
	if err != nil {
		return Transaction{}, err
	}
	listSet := set.(*datastore.ListSet)
	list, err := listSet.GetOrCreateList([]byte("transactions"))
	if err != nil {
		return Transaction{}, err
	}

	id, err := uuid.NewV4()
	if err != nil {
		return Transaction{}, err
	}
	key := sha256.Sum256(id.Bytes())
	entry, err := list.Add(key)
	if err != nil {
		return Transaction{}, err
	}

	return LoadTransaction(entry, rntm)
}

func LoadTransaction(id datastore.ListEntry, rntm *Runtime) (Transaction, error) {

	data, err := id.Read()
	if err != nil {
		return Transaction{}, err
	}
	var key [32]byte
	copy(key[:], data.([]byte))
	set, err := rntm.datastore.GetOrCreateSet(datastore.ListType, false, key)
	if err != nil {
		return Transaction{}, err
	}
	listSet := set.(*datastore.ListSet)

	//load the participants
	objects, err := listSet.GetOrCreateList([]byte("participants"))
	if err != nil {
		return Transaction{}, err
	}

	//and the user
	set, err = rntm.datastore.GetOrCreateSet(datastore.ValueType, false, key)
	if err != nil {
		return Transaction{}, err
	}
	valueSet := set.(*datastore.ValueSet)
	user, err := valueSet.GetOrCreateValue([]byte("user"))
	if err != nil {
		return Transaction{}, err
	}

	return Transaction{id, *objects, *user, rntm}, nil
}

func (self *Transaction) User(user User) (User, error) {

	data, err := self.user.Read()
	if err != nil {
		return User(""), err
	}

	return User(data.(string)), nil
}

func (self *Transaction) SetUser(user User) error {

	return self.user.Write(user)
}

func (self *Transaction) Objects() []Object {

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

/*********************************************************************************
								Manager
*********************************************************************************/

/*********************************************************************************
								Behaviour
*********************************************************************************/
type transactionBehaviour struct {
	*behaviour
}

func NewTransactionBehaviour(name string, parent identifier, rntm *Runtime) Object {

	behaviour, _ := NewBehaviour(parent, name, `TransactionBehaviour`, rntm)

	//add default events
	behaviour.AddEvent(`onChangedData`, NewEvent(rntm.jsvm))   //called once after finishing all changes on the parent object
	behaviour.AddEvent(`onParticipation`, NewEvent(rntm.jsvm)) //called when added to a transaction
	behaviour.AddEvent(`onAbortion`, NewEvent(rntm.jsvm))      //called when transaction, to which the parent was added, is aborted
	behaviour.AddEvent(`onClosing`, NewEvent(rntm.jsvm))       //called when transaction, to which the parent was added, is closed (means finished)

	return &transactionBehaviour{behaviour}
}
