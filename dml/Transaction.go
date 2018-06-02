//Special behaviour that describes the transaction handling

package dml

import (
	"CollaborationNode/datastores"

	"github.com/dop251/goja"
)

func NewTransaction(ds *datastore.Datastore, name string, vm *goja.Runtime) {

}

type transaction struct {
	behaviour
}
