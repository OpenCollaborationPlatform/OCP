package document

import (
	"encoding/gob"

	"github.com/ickby/CollaborationNode/dml"
	"github.com/ickby/CollaborationNode/p2p"

	cid "github.com/ipfs/go-cid"
)

var cidKey = []byte("__raw_cid")

func init() {
	gob.Register(new(p2p.Cid))
}

//Raw dml type: stores data identifiers and allows some information gathering
type Raw struct {
	*dml.DataImpl
}

func NewRawDmlObject(rntm *dml.Runtime) (dml.Object, error) {

	base, err := dml.NewDataBaseClass(rntm)
	if err != nil {
		return nil, err
	}

	//build the raw object
	raw := &Raw{
		base,
	}

	//add methods
	raw.AddMethod("Set", dml.MustNewMethod(raw.Set, false))
	raw.AddMethod("Get", dml.MustNewMethod(raw.Get, true))
	raw.AddMethod("IsSet", dml.MustNewMethod(raw.IsSet, true))
	raw.AddMethod("Clear", dml.MustNewMethod(raw.Clear, false))

	//add events
	err = raw.AddEvent(dml.NewEvent("onDataChanged", raw.GetJSPrototype(), rntm))

	return raw, err
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Set(id dml.Identifier, cidstr string) error {

	//check if it is a valid cid
	cId, err := cid.Decode(cidstr)
	if err != nil {
		return err
	}

	//we do not check if it is really available in the swarm store. This is dangerous
	//as the dml operation can be finished before we received the block!

	//store the cid!
	value, err := self.GetDBValueVersioned(id, cidKey)
	if err != nil {
		return err
	}
	err = value.Write(cId)
	if err != nil {
		return err
	}
	return self.GetEvent("onDataChanged").Emit(id, cidstr)
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Get(id dml.Identifier) (string, error) {

	value, err := self.GetDBValueVersioned(id, cidKey)
	if err != nil {
		return "", err
	}
	cId, err := value.Read()
	return cId.(*p2p.Cid).String(), err
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) IsSet(id dml.Identifier) (bool, error) {

	value, err := self.GetDBValueVersioned(id, cidKey)
	if err != nil {
		return false, err
	}
	if !value.IsValid() {
		return false, nil
	}

	//could also be invalid CID after clear!
	cId, err := value.Read()
	if err != nil {
		return false, err
	}
	return cId.(*p2p.Cid).Defined(), nil
}

//adds the path, either file or directory, to the Raw object
func (self *Raw) Clear(id dml.Identifier) error {

	value, err := self.GetDBValueVersioned(id, cidKey)
	if err != nil {
		return err
	}
	err = value.Write(p2p.Cid{})
	if err != nil {
		return err
	}
	return self.GetEvent("onDataChanged").Emit(id, p2p.Cid{}.String())
}
