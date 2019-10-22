// Vector
package dml

import (
	"fmt"
	
	gonum "gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"

	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
)

//map type: stores requested data type by index (0-based)
type graph struct {
	*DataImpl

	nodeData *datastore.MapVersioned
	edgeData *datastore.MapVersioned
}

func NewGraph(id Identifier, parent Identifier, rntm *Runtime) Object {

	base := NewDataBaseClass(id, parent, rntm)

	//get the db entries
	mset, _ := base.GetDatabaseSet(datastore.MapType)
	mapSet := mset.(*datastore.MapVersionedSet)
	nodeData, _ := mapSet.GetOrCreateMap([]byte("__map_node"))
	edgeData, _ := mapSet.GetOrCreateMap([]byte("__map_edge"))

	//build the graph object
	gr := &graph{
		base,
		nodeData,
		edgeData,
	}

	//add properties
	gr.AddProperty("directed", MustNewDataType("bool"), false, true)
	gr.AddProperty("node", MustNewDataType("type"), MustNewDataType("int"), true)
	gr.AddProperty("edge", MustNewDataType("type"), MustNewDataType("none"), true)

	//add methods
	gr.AddMethod("AddNode", MustNewMethod(gr.AddNode))
	/*gr.AddMethod("NewNode", MustNewMethod(gr.NewNode))
	gr.AddMethod("RemoveNode", MustNewMethod(gr.NewNode))
	gr.AddMethod("Nodes", MustNewMethod(gr.Node))
	gr.AddMethod("AddEdge", MustNewMethod(gr.AddEdge))
	gr.AddMethod("RemoveEdge", MustNewMethod(gr.RemoveEdge))
	gr.AddMethod("HasEdge", MustNewMethod(gr.HasEdge))
	gr.AddMethod("Edge", MustNewMethod(gr.Edge))*/

	return gr
}

//							Graph helpers 
//******************************************************************************
type graphEdge struct {
	source interface{}
	target interface{}
}

type graphNode struct {
	id int64
	node interface{}
} 

func (self graphNode) ID() int64 {
	return self.id
}

func (self *graph) getGonumDirected() gonum.Directed {
	
	directed := simple.NewDirectedGraph()
	keys, _ := self.nodeData.GetKeys()
	
	//add all nodes first: keys are nodes!
	mapper  := make(map[interface{}] gonum.Node, 0)
	for _, key := range keys {
		
		idx := directed.NewNode().ID()
		node := graphNode{idx, key}
		directed.AddNode(node)
		mapper[key] = node
	}
	
	//all edges next
	keys, _ = self.edgeData.GetKeys()
	for _, key := range keys {
		var edge graphEdge
		self.edgeData.ReadType(key, &edge)
		
		graphedge := directed.NewEdge(mapper[edge.source], mapper[edge.target])
		directed.SetEdge(graphedge)
	}
	
	return directed
}

//inverse of keyToDB
func (self *graph) dbToType(key interface{}, dt DataType) (interface{}, error) {
	
	if dt.IsObject() || dt.IsComplex() {

		encoded, _ := key.(string)
		id, err := IdentifierFromEncoded(encoded)
		if err != nil {
			return nil, utils.StackError(err, "Invalid identifier stored")
		}
		obj, has := self.rntm.objects[id]
		if !has {
			return nil, utils.StackError(err, "Invalid object stored")
		}
		return obj, nil
		
	} else if dt.IsType() {

		val, _ := key.(string)
		return NewDataType(val)
	}
	
	//everything else is simply used
	return key, nil
}


//convert all possible types to something usable in the DB
func (self *graph) typeToDB(input interface{}, dt DataType) interface{} {
	
	if dt.IsObject() || dt.IsComplex() {

		obj, _ := input.(Object)
		return obj.Id().Encode()
		
	} else if dt.IsType() {

		val, _ := input.(DataType)
		return val.AsString()
	}
	
	//everything else is simply used as key
	return input
}

func (self *graph) Nodes() ([]interface{}, error) {

	keys, err := self.nodeData.GetKeys()
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(keys))
	for i, key := range keys {
		
		typeVal, err := self.dbToType(key, self.nodeDataType())
		if err != nil {
			return nil, err
		}
		
		result[i] = typeVal
	}
	return result, nil
}


func (self *graph) AddNode(value interface{}) error {

	//check key type
	dt := self.nodeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot add node, has wrong type")
	}

	dbentry := self.typeToDB(value, dt)
	return self.nodeData.Write(dbentry, struct{}{})
}

/*
func (self *graph) Get(key interface{}) (interface{}, error) {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return nil, utils.StackError(err, "Key has wrong type")
	}
	
	//check if key is availbale
	dbkey := self.keyToDB(key)
	if !self.entries.HasKey(dbkey) {
		return nil, fmt.Errorf("Key is not available in Map")
	}

	//check if the type of the value is correct
	dt := self.valueDataType()
	var result interface{}

	if dt.IsObject() || dt.IsComplex() {
		var res string
		err = self.entries.ReadType(dbkey, &res)
		if err == nil {
			id, err := IdentifierFromEncoded(res)
			if err != nil {
				return nil, utils.StackError(err, "Invalid identifier stored in DB")
			} else {
				res, ok := self.rntm.objects[id]
				if !ok {
					return nil, fmt.Errorf("Map entry is invalid object")
				}
				result = res
			}
		}

	} else if dt.IsType() {

		var res string
		err := self.entries.ReadType(dbkey, &res)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get stored type")	
		}
		result, err = NewDataType(res)
		if err != nil {
			return nil, utils.StackError(err, "Invalid datatype stored")
		}

	} else {
		//plain types remain
		result, err = self.entries.Read(dbkey)
		if err != nil {
			return nil, utils.StackError(err, "Unable to access database of Map")
		}
	}

	return result, nil
}


//creates a new entry with a all new type, returns the index of the new one
func (self *graph) New(key interface{}) (interface{}, error) {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return nil, utils.StackError(err, "Cannot create new map value, key has wrong type")
	}
	
	//if we already have it we cannot create new!
	dbkey := self.keyToDB(key)
	if self.entries.HasKey(dbkey) {
		return nil, fmt.Errorf("Key exists already, cannot create new object")
	}

	//create a new entry (with correct refcount if needed)
	var result interface{}
	dt := self.valueDataType()
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "")
		if err != nil {
			return nil, utils.StackError(err, "Unable to append new object to graph: construction failed")
		}
		obj.IncreaseRefcount()
		result = obj

	} else {
		result = dt.GetDefaultValue()
	}

	//write new entry
	return result, self.Set(key, result)
}


//remove a entry from the graph
func (self *graph) Remove(key interface{}) error {

	//check key type
	kdt := self.keyDataType()
	err := kdt.MustBeTypeOf(key)
	if err != nil {
		return utils.StackError(err, "Cannot create new map value, key has wrong type")
	}
	
	//if we already have it we cannot create new!
	dbkey := self.keyToDB(key)
	if !self.entries.HasKey(dbkey) {
		return fmt.Errorf("Key does not exist, cannot be removed")
	}

	//decrease refcount if required
	dt := self.valueDataType()
	if dt.IsComplex() || dt.IsObject() {
		//we have a object stored, hence delete must remove it completely!
		val, err := self.Get(key)
		if err != nil {
			return utils.StackError(err, "Unable to delete entry")
		}
		data, ok := val.(Data)
		if ok {
			data.DecreaseRefcount()
		}
	}

	//and delete the old key
	return self.entries.Remove(dbkey)
}
*/

//*****************************************************************************
//			Internal functions
//*****************************************************************************

//implement DynamicData interface
func (self *graph) Load() error {

	//nodes: we only need to load when we store objects
	dt := self.nodeDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.nodeData.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to load graph entries: Nodes cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to load graph: Stored identifier is invalid")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {

				//we made sure the object does not exist. We need to load it
				obj, err := LoadObject(self.rntm, dt, id)
				if err != nil {
					return utils.StackError(err, "Unable to load object for graph: construction failed")
				}
				obj.IncreaseRefcount()
				self.rntm.objects[id] = obj.(Data)
			} else {
				existing.IncreaseRefcount()
			}
		}
	}

	//values: we only need to load when we store objects
	dt = self.edgeDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.edgeData.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to load graph entries: Edges cannot be accessed")
		}
		for _, key := range keys {
			
			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to load graph: Stored identifier is invalid")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {

				//we made sure the object does not exist. We need to load it
				obj, err := LoadObject(self.rntm, dt, id)
				if err != nil {
					return utils.StackError(err, "Unable to load object for graph: construction failed")
				}
				obj.IncreaseRefcount()
				self.rntm.objects[id] = obj.(Data)
			} else {
				existing.IncreaseRefcount()
			}
		}
	}
	return nil
}


//override to handle children refcount additional to our own
func (self *graph) IncreaseRefcount() error {

	//handle nodes!
	dt := self.nodeDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.nodeData.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase graph refcount: Nodes cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to increase graph refcount: Invalid child identifier stored")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {
				return fmt.Errorf("Unable to increase graph refcount: Invalid child stored")
			}
			existing.IncreaseRefcount()
		}
	}

	//handle edges!
	dt = self.edgeDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.edgeData.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase graph refcount: Edges cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to increase graph refcount: Invalid child identifier stored")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {
				return fmt.Errorf("Unable to increase graph refcount: Invalid child stored")
			}
			existing.IncreaseRefcount()
		}
	}
	
	//now increase our own refcount
	return self.object.IncreaseRefcount()
}

//override to handle children refcount additional to our own
func (self *graph) DecreaseRefcount() error {
	
	//handle nodes!
	dt := self.nodeDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.nodeData.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase graph refcount: Nodes cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to increase graph refcount: Invalid child identifier stored")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {
				return fmt.Errorf("Unable to increase graph refcount: Invalid child stored")
			}
			existing.DecreaseRefcount()
		}
	}
	
	//handle edges
	dt = self.edgeDataType()
	if dt.IsObject() || dt.IsComplex() {

		keys, err := self.edgeData.GetKeys()
		if err != nil {
			return utils.StackError(err, "Unable to increase graph refcount: Edges cannot be accessed")
		}
		for _, key := range keys {

			id, err := IdentifierFromEncoded(key.(string))
			if err != nil {
				return utils.StackError(err, "Unable to increase graph refcount: Invalid child identifier stored")
			}
			existing, ok := self.rntm.objects[id]
			if !ok {
				return fmt.Errorf("Unable to increase graph refcount: Invalid child stored")
			}
			existing.DecreaseRefcount()
		}
	}
	
	//now decrease our own refcount
	return self.object.DecreaseRefcount()
}

func (self *graph) nodeDataType() DataType {

	prop := self.GetProperty("node").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}

func (self *graph) edgeDataType() DataType {

	prop := self.GetProperty("edge").(*constTypeProperty)
	dt := prop.GetDataType()
	return dt
}
