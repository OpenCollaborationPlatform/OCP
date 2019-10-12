package document

import (
	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"

	cid "github.com/ipfs/go-cid"
)

//Raw dml type: stores data identifiers and allows some information gathering
type Raw struct {
	*dml.DataImpl

	//data storage
	value *datastore.ValueVersioned
}

func NewRawDmlObject(id, parent dml.Identifier, rntm *dml.Runtime) dml.Object {

	base := dml.NewDataBaseClass(id, parent, rntm)

	//get the db entry
	set, _ := base.GetDatabaseSet(datastore.ValueType)
	valueSet := set.(*datastore.ValueVersionedSet)
	value, _ := valueSet.GetOrCreateValue([]byte("__raw_cid"))

	//build the raw object
	raw := &Raw{
		base,
		value,
	}

	//add methods
	raw.AddMethod("Set", dml.MustNewMethod(raw.Set))
	raw.AddMethod("Get", dml.MustNewMethod(raw.Get))
	raw.AddMethod("IsSet", dml.MustNewMethod(raw.IsSet))
	raw.AddMethod("Clear", dml.MustNewMethod(raw.Clear))

	return raw
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Set(cidstr string) error {

	//check if it is a valid cid
	id, err := cid.Decode(cidstr)
	if err != nil {
		return err
	}

	//we do not check if it is really available in the swarm store. This is dangerous
	//as the dml operation can be finished before we received the block!

	//store the cid!
	return self.value.Write(id)
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Get() (string, error) {

	var id p2p.Cid
	err := self.value.ReadType(&id)
	return id.String(), err
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) IsSet() (bool, error) {

	if holds, err := self.value.HoldsValue(); !holds || err != nil {
		return false, err
	}

	//could also be invalid CID after clear!
	var id p2p.Cid
	err := self.value.ReadType(&id)
	if err != nil {
		return false, err
	}
	return id.Defined(), nil
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Clear() error {

	return self.value.Write(p2p.Cid{})
}
