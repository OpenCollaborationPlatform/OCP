package document

import (
	"encoding/gob"

	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"

	cid "github.com/ipfs/go-cid"
)

func init() {
	gob.Register(new(p2p.Cid))
}

//Raw dml type: stores data identifiers and allows some information gathering
type Raw struct {
	*dml.DataImpl

	//data storage
	value *datastore.ValueVersioned
}

func NewRawDmlObject(id, parent dml.Identifier, rntm *dml.Runtime) (dml.Object, error) {

	base, err := dml.NewDataBaseClass(id, parent, rntm)
	if err != nil {
		return nil, err
	}

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

	//add events
	err = raw.AddEvent("onDataChanged", dml.NewEvent(raw.GetJSObject(), rntm, dml.MustNewDataType("string")))

	return raw, err
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
	err = self.value.Write(id)
	if err != nil {
		return err
	}
	return self.GetEvent("onDataChanged").Emit(cidstr)
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Get() (string, error) {

	id, err := self.value.Read()
	return id.(*p2p.Cid).String(), err
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) IsSet() (bool, error) {

	if holds, err := self.value.HoldsValue(); !holds || err != nil {
		return false, err
	}

	//could also be invalid CID after clear!
	id, err := self.value.Read()
	if err != nil {
		return false, err
	}
	return id.(*p2p.Cid).Defined(), nil
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Clear() error {

	err := self.value.Write(p2p.Cid{})
	if err != nil {
		return err
	}
	return self.GetEvent("onDataChanged").Emit(p2p.Cid{}.String())
}
