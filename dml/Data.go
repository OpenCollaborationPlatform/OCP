// Data descibes a Data object with its respective properties to store things in a database
package dml

import (
	"github.com/OpenCollaborationPlatform/OCP/datastores"
	"github.com/OpenCollaborationPlatform/OCP/utils"
)

var childKey = []byte("__children")

/* +extract prio:1
.. dml:object:: Data

	The most basic implementation of a DML Object. It allows to add properties, events
	and functions and can hold other Objects and Behaviours as children. It has no other
	special functionality. It is intended as dml grouping 	object as well as base object
	for all other data types
	Data does allow for children. Note that children are static values, they cannot change at
	runtime. Hence they are different to dynamic objects as are possible with Maps etc. Children are
	used purely for the static DML hirarchy.

	.. dml:property:: name
		:const:
		:type: string

		A property defining the name of the object. The name can than be used to access in
		the hirarchy, either in JavaScript code or as WAMP uri. It is mandatory to set the name
		of each object.

	.. dml:event:: onBeforePropertyChange

		Emitted bevore a property of the object changes. At time of emit the
		property still has its old value.

		:argument string Property: Name of the property thats about to be changed

	.. dml:event:: onPropertyChanged

		Emitted when a property was changed. The value of the property is already the
		new one when emitted.

		:argument string Property: Name of the property thats was changed

	.. dml:event:: onCreated

		Emitted after the object was created and fully setup. It will be emitted
		only dynamically created objects, for example when used as value in Maps,
		and not the ones in the static DML hirarchy.

	.. dml:event:: onRemove

		Emitted after the object is about to be removed. At the time of emitting
		the object is still fully setup and accessible, the removing will happen after
		all handlers have been executed.
		As static hirarchy objects cannot be removed this event will be emitted only
		for dynamically created objects, for example when used as value in Maps etc.

	.. dml:event:: onBeforeChange

		This is a general event, emitted bevore the object itself changes, no matter
		what the changes are. This is not emitted for changed properties, there is a
		custom event for that, but if the objects content is manipulated. This means that
		for a Data object it will never be emitted, as it does not have any object
		content, but it may be emitted for derived object types like maps.

		.. note:: Most derived classes that emit this event will also have custom
				  events that are more specialized for the eexact changes that happend.


	.. dml:event:: onChanged

		This is a general event, emitted after the object has changed, no matter
		what the changes are. This is not emitted for changed properties, there is a
		custom event for that, but if the objects content was manipulated. This means that
		for a Data object it will never be emitted, as it does not have any object
		content, but it may be emitted for derived object types like maps.

		.. note:: Most derived classes that emit this event will also have custom
				  events that are more specialized for the eexact changes that happend.

*/
type Data interface {
	Object
	BehaviourHandler

	//Data hirarchy allows childs. Here we add the structure and logic by
	//adding static objects. Database access by identifiers is handled seperatly
	AddChildObject(Data)
	GetChildObjects() []Data

	//Data hirarchy allows childs
	AddChildIdentifier(Identifier, Identifier) error
	GetChildIdentifiers(Identifier) ([]Identifier, error)
	GetChildIdentifierByName(Identifier, string) (Identifier, error)

	//little convinience function for children hirarchy combining objects and IDs
	GetChildren(Identifier) ([]dmlSet, error)
	GetChildByName(Identifier, string) (dmlSet, error)

	GetSubobjects(id Identifier) ([]dmlSet, error)                                               //Convinience function to get all subobjects, including childs, behaviours any any created ones
	Created(id Identifier) error                                                                 //emits onCreated event for this and all subobjects (not behaviours)
	recursiveHandleBehaviourEvent(Identifier, Identifier, string, []interface{}, []string) error //Helper function to propagate behaviour events to the parent (if availbabe)

}

type DataImpl struct {
	*object
	behaviourHandler

	children []Data
}

func NewData(rntm *Runtime) (Object, error) {
	return NewDataBaseClass(rntm)
}

func NewDataBaseClass(rntm *Runtime) (*DataImpl, error) {

	obj, err := NewObject(rntm)
	if err != nil {
		return nil, err
	}

	dat := &DataImpl{
		obj,
		NewBehaviourHandler(rntm),
		make([]Data, 0),
	}

	dat.AddEvent(NewEvent("onCreated", dat))
	dat.AddEvent(NewEvent("onRemove", dat))
	dat.AddEvent(NewEvent("onNewSubobject", dat))
	dat.AddEvent(NewEvent("onBeforeChange", obj))
	dat.AddEvent(NewEvent("onChanged", obj))

	return dat, nil
}

func (self *DataImpl) SetObjectPath(id Identifier, path string) error {

	err := self.object.SetObjectPath(id, path)
	if err != nil {
		return utils.StackError(err, "Unable to set own object path")
	}

	//we also need to update the path for all children. Note: Not subobjects,
	//as we do not know exactly how to expose them in paths, that must be done by
	//the concrete implementations
	subs, err := self.GetChildIdentifiers(id)
	if err != nil {
		return utils.StackError(err, "Unable to access subobjects")
	}
	for _, sub := range subs {

		//path in general is the listing of names of parents, but if no name is set it is the
		//full encoded identifier
		var fullpath string
		if sub.Name != "" {
			fullpath = path + "." + sub.Name
		} else {
			fullpath = sub.Encode()
		}
		if err := self.SetObjectPath(sub, fullpath); err != nil {
			return err
		}
	}

	return nil
}

func (self *DataImpl) AddChildObject(child Data) {
	self.children = append(self.children, child)
}

func (self *DataImpl) GetChildObjects() []Data {
	return self.children
}

func (self *DataImpl) AddChildIdentifier(id Identifier, child Identifier) error {

	list, err := self.GetDBList(id, childKey)
	if err != nil {
		return utils.StackError(err, "Unable to access DB for %v", id)
	}
	_, err = list.Add(child)
	if err != nil {
		return utils.StackError(err, "Unable to add child to Data object")
	}
	return nil
}

func (self *DataImpl) GetChildIdentifiers(id Identifier) ([]Identifier, error) {

	list, err := self.GetDBList(id, childKey)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access DB for %v", id)
	}
	entries, err := list.GetValues()
	if err != nil {
		return nil, utils.StackError(err, "Unable to query object children")
	}

	result := make([]Identifier, len(entries))
	for i, child := range entries {
		id, err := child.Read()
		if err != nil {
			return nil, utils.StackError(err, "Unable to read child entry")
		}
		result[i] = *id.(*Identifier)
	}
	return result, nil
}

func (self *DataImpl) GetChildIdentifierByName(id Identifier, name string) (Identifier, error) {

	childs, err := self.GetChildIdentifiers(id)
	if err != nil {
		return Identifier{}, utils.StackError(err, "Unable to access child identifiers in DB for %v", id)
	}
	for _, child := range childs {

		if child.Name == name {
			return child, nil
		}
	}
	return Identifier{}, newUserError(Error_Key_Not_Available, "No such object available", "name", name)
}

func (self *DataImpl) GetChildren(id Identifier) ([]dmlSet, error) {

	childIDs, err := self.GetChildIdentifiers(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access child identifiers in DB for %v", id)
	}

	result := make([]dmlSet, len(childIDs))
	for i, childID := range childIDs {
		childDT, err := self.GetDataType(childID)
		if err != nil {
			return nil, utils.StackError(err, "Unable to query child datatype")
		}
		result[i] = dmlSet{obj: self.rntm.objects[childDT].(Data), id: childID}
	}
	return result, nil
}

func (self *DataImpl) GetChildByName(id Identifier, name string) (dmlSet, error) {

	childID, err := self.GetChildIdentifierByName(id, name)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to find child %v in object %v", name, id)
	}

	childDT, err := self.GetDataType(childID)
	if err != nil {
		return dmlSet{}, utils.StackError(err, "Unable to query child datatype")
	}

	return dmlSet{obj: self.rntm.objects[childDT].(Data), id: childID}, nil
}

func (self *DataImpl) GetSubobjects(id Identifier) ([]dmlSet, error) {

	result := make([]dmlSet, 0)

	//add hirarchy
	children, err := self.GetChildren(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access children for %v", id)
	}
	result = append(result, children...)

	//add behaviour
	bhvrs := self.Behaviours()
	for _, name := range bhvrs {
		bhvr, err := self.GetBehaviour(id, name)
		if err != nil {
			return nil, utils.StackError(err, "Unable to query behaviour manager")
		}
		result = append(result, bhvr)
	}

	return result, nil
}

func (self *DataImpl) Created(id Identifier) error {

	//call on created events
	err := self.GetEvent("onCreated").Emit(id)
	if err != nil {
		return utils.StackError(err, "Unable to emit event")
	}

	subs, err := self.GetSubobjects(id)
	if err != nil {
		return utils.StackError(err, "Unable to access subobjects")
	}
	for _, dbSet := range subs {

		data, ok := dbSet.obj.(Data)
		if ok {
			err := data.Created(dbSet.id)
			if err != nil {
				return err
			}

		}
	}

	return nil
}

func (self *DataImpl) GetByKey(id Identifier, key Key) (interface{}, error) {
	//first check if it is a property/event/method
	prop, err := self.object.HasKey(id, key)
	if err != nil {
		return nil, err
	}
	if prop {
		return self.object.GetByKey(id, key)
	}

	//check if it is a child
	child, err := self.GetChildByName(id, key.AsString())
	if err == nil {
		return child, nil
	}

	//search behaviour
	bhvrs := self.Behaviours()
	for _, bhvr := range bhvrs {
		bhvrId, err := self.GetBehaviourIdentifier(id, bhvr)
		if err == nil {
			if bhvrId.Name == key.AsString() {
				bhvrObj := self.GetBehaviourObject(bhvr)
				return dmlSet{bhvrObj, bhvrId}, nil
			}
		}
	}

	//it could be an identifier which may be a cild
	subid, err := IdentifierFromEncoded(key.AsString())
	if err == nil {
		if subid.Parent == id.Hash() {
			dt, err := self.GetDataType(subid)
			if err != nil {
				return dmlSet{}, newInternalError(Error_Setup_Invalid, "Datatype not available for sub ID")
			}
			dtObj, ok := self.rntm.objects[dt]
			if !ok {
				return dmlSet{}, newInternalError(Error_Setup_Invalid, "Object not available for datatype")
			}
			return dmlSet{id: subid, obj: dtObj}, nil
		}
	}

	return nil, newUserError(Error_Key_Not_Available, "Key not available", "key", key, "object", id.Name)
}

func (self *DataImpl) HasKey(id Identifier, key Key) (bool, error) {

	prop, err := self.object.HasKey(id, key)
	if prop || err != nil {
		return prop, err
	}

	//check if it is a child
	_, err = self.GetChildByName(id, key.AsString())
	if err == nil {
		return true, nil
	}

	//search behaviour
	bhvrs := self.Behaviours()
	for _, bhvr := range bhvrs {
		bhvrId, err := self.GetBehaviourIdentifier(id, bhvr)
		if err == nil {
			if bhvrId.Name == key.AsString() {
				return true, nil
			}
		}
	}

	return false, nil
}

func (self *DataImpl) GetKeys(id Identifier) ([]Key, error) {

	keys, err := self.object.GetKeys(id)
	if err != nil {
		return nil, err
	}
	childs, err := self.GetChildIdentifiers(id)
	if err != nil {
		return nil, utils.StackError(err, "Unable to access object children")
	}
	for _, child := range childs {
		keys = append(keys, MustNewKey(child.Name))
	}
	bhvrs := self.Behaviours()
	for _, bhvr := range bhvrs {
		id, err := self.GetBehaviourIdentifier(id, bhvr)
		if err != nil {
			return nil, utils.StackError(err, "Unable to access object behaviour", "Behaviour", bhvr)
		}
		keys = append(keys, MustNewKey(id.Name))
	}
	return keys, nil
}

func (self *DataImpl) keyToDS(id Identifier, key Key) ([]datastore.Key, error) {

	if has, _ := self.object.HasKey(id, key); has {
		return self.object.keyToDS(id, key)
	}

	//check children
	list, err := self.GetDBList(id, childKey)
	if err != nil {
		return nil, err
	}
	entries, err := list.GetValues()
	if err != nil {
		return nil, err
	}

	for _, child := range entries {
		data, err := child.Read()
		if err != nil {
			return nil, err
		}
		id = *data.(*Identifier)
		if id.Name == key.AsString() {
			return []datastore.Key{datastore.NewKey(datastore.ListType, false, id.Hash(), childKey, child.Reference())}, nil
		}
	}

	//check behaviours

	return nil, newInternalError(Error_Key_Not_Available, "Key not available in data object")
}

func (self *DataImpl) keyToBeRemoved(id Identifier, key Key) ([]datastore.Key, error) {
	//children are a static hirarchy, cannot be removed, as well as all object keys... so error!
	return nil, newInternalError(Error_Operation_Invalid, "Keys of Data object cannot be removed", "Key", key)
}

/*Override  event emitted, to forward them to the behaviour handler*/
func (self *DataImpl) EventEmitted(id Identifier, event string, args ...interface{}) error {

	//call ourself and our parents till all behaviours are handled. Note that initially we use
	//unrecursive behaviours, all parents only use the recursive ones.
	behaviours := self.GetRuntime().behaviours.GetEventBehaviours(event)
	res, err := self.HandleBehaviourEvent(id, id, event, args, behaviours, false)
	if err != nil {
		return err
	}

	if len(res) != 0 {
		err = self.recursiveHandleBehaviourEvent(id, id, event, args, res)
		if err != nil {
			return err
		}
	}

	//call base class implementation
	return self.object.EventEmitted(id, event, args...)
}

func (self *DataImpl) recursiveHandleBehaviourEvent(id Identifier, source Identifier, name string, args []interface{}, behaviours []string) error {

	if len(behaviours) > 0 {
		res, err := self.HandleBehaviourEvent(id, source, name, args, behaviours, true)
		if err != nil {
			return err
		}

		//if we have unhadled behaviours left we forward them to the parent object
		if len(res) > 0 {
			parentSet, err := self.GetParent(id)
			if err == nil {
				if parent, ok := parentSet.obj.(Data); ok {
					err := parent.recursiveHandleBehaviourEvent(parentSet.id, source, name, args, res)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
