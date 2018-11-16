//Special behaviour that describes the transaction handling

package dml

import (
	"CollaborationNode/datastores"

	"github.com/dop251/goja"
)

type Transaction struct {
	participants []identifier
}

func NewTransactionBehaviour(store *datastore.Datastore, name string, parent Object, vm *goja.Runtime) Object {

	behaviour, _ := NewBehaviour(parent, name, `TransactionBehaviour`, vm, store)

	//add default events
	behaviour.AddEvent(`onChangedData`, NewEvent(vm))   //called once after finishing all changes on the parent object
	behaviour.AddEvent(`onParticipation`, NewEvent(vm)) //called when added to a transaction
	behaviour.AddEvent(`onAbortion`, NewEvent(vm))      //called when transaction, to which the parent was added, is aborted
	behaviour.AddEvent(`onClosing`, NewEvent(vm))       //called when transaction, to which the parent was added, is closed (means finished)

	return &transactionBehaviour{behaviour}
}

type transactionBehaviour struct {
	*behaviour
}
