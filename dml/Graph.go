// Vector
package dml

import (
	"fmt"
	"encoding/gob"
	
	gonum "gonum.org/v1/gonum/graph"
	topo "gonum.org/v1/gonum/graph/topo"
	"gonum.org/v1/gonum/graph/simple"
	uuid "github.com/satori/go.uuid"

	"github.com/ickby/CollaborationNode/datastores"
	"github.com/ickby/CollaborationNode/utils"
)

func init() { 
	gob.Register(new(struct{}))
	gob.Register(new(graphEdge))
}

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
	gr.AddMethod("NewNode", MustNewMethod(gr.NewNode))
	gr.AddMethod("RemoveNode", MustNewMethod(gr.RemoveNode))
	gr.AddMethod("HasNode", MustNewMethod(gr.HasNode))
	gr.AddMethod("Nodes", MustNewMethod(gr.Nodes))
	gr.AddMethod("AddEdge", MustNewMethod(gr.AddEdge))
	gr.AddMethod("NewEdge", MustNewMethod(gr.NewEdge))
	gr.AddMethod("RemoveEdge", MustNewMethod(gr.RemoveEdge))
	gr.AddMethod("RemoveEdgeBetween", MustNewMethod(gr.RemoveEdgeBetween))
	gr.AddMethod("HasEdge", MustNewMethod(gr.HasEdge))
	gr.AddMethod("HasEdgeBetween", MustNewMethod(gr.HasEdgeBetween))
	gr.AddMethod("Edge", MustNewMethod(gr.Edge))

	gr.AddMethod("FromNode", MustNewMethod(gr.FromNode))
	gr.AddMethod("ToNode", MustNewMethod(gr.ToNode))
	gr.AddMethod("Sorted", MustNewMethod(gr.Sorted))
	gr.AddMethod("Cycles", MustNewMethod(gr.Cycles))

	return gr
}

//							Graph helpers 
//******************************************************************************
type graphEdge struct {
	Source interface{}
	Target interface{}
}

type graphNode struct {
	id int64
	node interface{}
} 

func (self graphNode) ID() int64 {
	return self.id
}

func (self *graph) getGonumDirected() (gonum.Directed, map[interface{}] gonum.Node) {
	
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
		data, err := self.edgeData.Read(key)
		if err != nil {
			continue
		}
		edge := data.(*graphEdge)
		graphedge := directed.NewEdge(mapper[edge.Source], mapper[edge.Target])
		directed.SetEdge(graphedge)
	}
	
	return directed, mapper
}

func (self *graph) getGonumGraph() (gonum.Graph, map[interface{}] gonum.Node) {
	directed, mapper := self.getGonumDirected()
	if !self.isDirected() {
		return &gonum.Undirect{directed}, mapper
	}
	
	return directed, mapper
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

func (self *graph) HasNode(node interface{}) (bool, error) {

	//check key type
	dt := self.nodeDataType()
	err := dt.MustBeTypeOf(node)
	if err != nil {
		return false, utils.StackError(err, "Wrong type for node")
	}

	dbkey := self.typeToDB(node, dt)
	return self.nodeData.HasKey(dbkey), nil
}


func (self *graph) AddNode(value interface{}) error {

	//check key type
	dt := self.nodeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot add node, has wrong type")
	}

	if dt.IsComplex() || dt.IsObject() {
		obj := value.(Object)
		obj.IncreaseRefcount()
	}

	dbentry := self.typeToDB(value, dt)
	return self.nodeData.Write(dbentry, struct{}{})
}

//creates a new entry with a all new type, returns the new node
func (self *graph) NewNode() (interface{}, error) {

	//check key type
	dt := self.nodeDataType()

	//create a new entry
	var result interface{}
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "")
		if err != nil {
			return nil, utils.StackError(err, "Unable to append new object to graph: construction failed")
		}
		result = obj

	} else {
		result = dt.GetDefaultValue()
	}

	//write new entry
	return result, self.AddNode(result)
}

func (self *graph) RemoveNode(value interface{}) error {

	if has, _ := self.HasNode(value); !has {
		return fmt.Errorf("No such node available, cannot remove")
	}
	
	dt := self.nodeDataType()
	if dt.IsComplex() || dt.IsObject() {
		obj := value.(Object)
		obj.DecreaseRefcount()
	}

	dbentry := self.typeToDB(value, dt)
	err := self.nodeData.Remove(dbentry)
	if err != nil {
		return utils.StackError(err, "unable to remove node from graph")
	}
	
	//remove all edges that connect to this node
	keys, err := self.edgeData.GetKeys()
	if err != nil {
		return fmt.Errorf("Unable to access edges")
	}
	for _, key := range keys {

		//check if this is the edge
		data, err := self.edgeData.Read(key)
		if err != nil {
			return utils.StackError(err, "Unable to access edge, wrong type stored")
		}
		edge := data.(*graphEdge)
		
		if 	(edge.Source == value || edge.Target == value) { 
			self.edgeData.Remove(key)
		}
	}
	
	return nil
}

func (self *graph) AddEdge(source, target, value interface{}) error {

	//check edge type
	dt := self.edgeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot add node, has wrong type")
	}
	
	//check if we have the two nodes
	hassource, err := self.HasNode(source)
	if err != nil {
		return utils.StackError(err, "Source node is of wrong type")
	}
	hastarget, err := self.HasNode(target)
	if err != nil {
		return utils.StackError(err, "Target node is of wrong type")
	}
	if !hassource || !hastarget {
		return fmt.Errorf("Source and target nodes must be available, but are not")
	}

	//check if edge already exists
	if has, _ := self.HasEdgeBetween(source, target); has {
		return fmt.Errorf("Edge does already exist")
	}

	//write
	dbentry := self.typeToDB(value, dt)
	edge := graphEdge{source, target}
	fmt.Printf("Add edge %v\n", edge)

	err = self.edgeData.Write(dbentry, edge)
	
	//handle ref count
	if err == nil && (dt.IsComplex() || dt.IsObject()) {
		obj := value.(Object)
		obj.IncreaseRefcount()
	}
	
	return err
}

//creates a new entry with a all new type, returns the new node
func (self *graph) NewEdge(source, target interface{}) (interface{}, error) {

	//check if we have the two nodes
	hassource, err := self.HasNode(source)
	if err != nil {
		return nil, utils.StackError(err, "Source node is of wrong type")
	}
	hastarget, err := self.HasNode(target)
	if err != nil {
		return nil, utils.StackError(err, "Target node is of wrong type")
	}
	if !hassource || !hastarget {
		return nil, fmt.Errorf("Source and target nodes must be available, but are not")
	}

	//check if edge already exists
	if has, _ := self.HasEdgeBetween(source, target); has {
		return nil, fmt.Errorf("Edge does already exist")
	}

	//create a new entry if possible
	dt := self.edgeDataType()
	var result interface{}
	if dt.IsComplex() {
		obj, err := ConstructObject(self.rntm, dt, "")
		if err != nil {
			return nil, utils.StackError(err, "Unable to append new object to graph: construction failed")
		}
		obj.IncreaseRefcount()
		result = obj

	} else if dt.IsNone() {
		
		//use a custom internal scheme
		result = uuid.NewV4().Bytes()
	
	} else {
		//all other type would result in a default value that is always the same,
		//hence not allowing to add multiple edges. This is stupid!
		return nil, fmt.Errorf("New edge only workes with complex or none edge type")
	}
	
	//write
	dbentry := self.typeToDB(result, dt)
	edge := graphEdge{source, target}

	return result, self.edgeData.Write(dbentry, edge)
}

func (self *graph) RemoveEdge(value interface{}) error {

	//check key type
	dt := self.edgeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot remove edge, has wrong type")
	}


	dbentry := self.typeToDB(value, dt)
	err = self.edgeData.Remove(dbentry)
	if err == nil && (dt.IsComplex() || dt.IsObject()) {
		obj := value.(Object)
		obj.DecreaseRefcount()
	}	
	return err
}

func (self *graph) RemoveEdgeBetween(source, target interface{}) error {

	//check if we have the two nodes
	hassource, err := self.HasNode(source)
	if err != nil {
		return utils.StackError(err, "Source node is of wrong type")
	}
	hastarget, err := self.HasNode(target)
	if err != nil {
		return utils.StackError(err, "Target node is of wrong type")
	}
	if !hassource || !hastarget {
		return fmt.Errorf("Source and target nodes must be available, but are not")
	}

	//we need to iterate all edges!
	keys, err := self.edgeData.GetKeys()
	if err != nil {
		return utils.StackError(err, "Unable to remove edge")
	}
	edt := self.edgeDataType()
	
	for _, key := range keys {

		//check if this is the edge to remove
		data, err := self.edgeData.Read(key)
		if err != nil {
			return utils.StackError(err, "Unable to access edge, wrong type stored")
		}
		edge := data.(*graphEdge)

		if 	(edge.Source == source && edge.Target == target) || 
		 	(!self.isDirected() && (edge.Source == target && edge.Target == source)) { 

			//remove it!
			if edt.IsComplex() || edt.IsObject() {
				value, err := self.dbToType(key, edt)
				if err != nil {
					return utils.StackError(err, "Faulty edge stored")
				}
				obj := value.(Object)
				obj.DecreaseRefcount()
			}
			return self.edgeData.Remove(key)
		}		
	}

	return fmt.Errorf("No edge between the two nodes, cannot remove")
}

func (self *graph) HasEdge(value interface{}) (bool, error) {
	
	//check key type
	dt := self.edgeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return false, utils.StackError(err, "Edge has wrong type")
	}
	
	dbkey := self.typeToDB(value, dt)
	return self.edgeData.HasKey(dbkey), nil
}
	
func (self *graph) HasEdgeBetween(source, target interface{}) (bool, error) {

	//check if we have the two nodes
	hassource, err := self.HasNode(source)
	if err != nil {
		return false, utils.StackError(err, "Source node is of wrong type")
	}
	hastarget, err := self.HasNode(target)
	if err != nil {
		return false, utils.StackError(err, "Target node is of wrong type")
	}
	if !hassource || !hastarget {
		return false, fmt.Errorf("Source and target nodes must be available, but are not")
	}

	//we need to iterate all edges!
	keys, err := self.edgeData.GetKeys()
	if err != nil {
		return false, fmt.Errorf("Unable to access edges")
	}
	
	for _, key := range keys {

		//check if this is the edge
		data, err := self.edgeData.Read(key)
		if err != nil {
			return false, utils.StackError(err, "Unable to access edge, wrong type stored")
		}
		edge := data.(*graphEdge)
		
		if 	(edge.Source == source && edge.Target == target) || 
		 	(!self.isDirected() && (edge.Source == target && edge.Target == source) ) { 

			return true, nil
		}
	}

	return false, nil
}

func (self *graph) Edge(source, target interface{}) (interface{}, error) {

	//check key type
	ndt := self.nodeDataType()
	err := ndt.MustBeTypeOf(source)
	if err != nil {
		return nil, utils.StackError(err, "Source node has wrong type")
	}
	err = ndt.MustBeTypeOf(target)
	if err != nil {
		return nil, utils.StackError(err, "Target node has wrong type")
	}

	//we need to iterate all edges!
	keys, err := self.edgeData.GetKeys()
	if err != nil {
		return nil, fmt.Errorf("Unable to access edges")
	}
	
	for _, key := range keys {

		//check if this is the edge
		data, err := self.edgeData.Read(key)
		if err != nil {
			return nil, utils.StackError(err, "Unable to access edge, wrong type stored")
		}
		edge := data.(*graphEdge)
		
		if 	(edge.Source == source && edge.Target == target) || 
		 	(!self.isDirected() && (edge.Source == target && edge.Target == source) ) { 

			return self.dbToType(key, self.edgeDataType())
		}
	}

	return nil, fmt.Errorf("No edge between nodes")
}

//*****************************************************************************
//			Graph functions
//*****************************************************************************

//return all nodes reachable from the given one
func (self *graph) FromNode(node interface{}) (interface{}, error) {

	//check if node exists
	has, err := self.HasNode(node)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, fmt.Errorf("Not does not exist")
	}
	
	gg, mapper := self.getGonumGraph()
	res := gg.From(mapper[node].ID())

	result := make([]interface{}, 0)
	for k:= res.Next(); k; k = res.Next() {
		gnode := res.Node().(graphNode)
		result = append(result, gnode.node)
	}

	return result, nil
}

//return all nodes which can reach the given one
func (self *graph) ToNode(node interface{}) (interface{}, error) {

	if !self.isDirected() {
		return self.FromNode(node)
	}

	//check if node exists
	has, err := self.HasNode(node)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, fmt.Errorf("Not does not exist")
	}
	
	gg, mapper := self.getGonumDirected()
	res := gg.To(mapper[node].ID())

	result := make([]interface{}, 0)
	for k:= res.Next(); k; k = res.Next() {
		gnode := res.Node().(graphNode)
		result = append(result, gnode.node)
	}
	return result, nil
}

//return a topological sort of all nodes
//the returned value is a slice of interfaces. The interfaces are nodes or 
//slices of nodes if they form a circle that cannot be ordered
func (self *graph) Sorted() ([]interface{}, error) {

	if !self.isDirected() {
		return nil, fmt.Errorf("This is only available for directed graphs")
	}

	gg, _ := self.getGonumDirected()
	sorted, err := topo.Sort(gg)
	
	result := make([]interface{}, len(sorted))
	
	//check if there are cycles 
	cycles, hascycles := err.(topo.Unorderable)
	if hascycles {
		
		cycleCnt := 0
		for i, node := range sorted {
			
			if node == nil {
				//build the cycle node slice
				cyc := cycles[cycleCnt]
				cycSlice := make([]interface{}, len(cyc)) 
				for j, n := range cyc { 
					gn := n.(graphNode)
					cycSlice[j] = gn.node
				}
				//and store it
				result[i] = cycSlice
				cycleCnt++
			
			} else {
				gnode := node.(graphNode)
				result[i] = gnode.node
			}
		}		
	} else {
		for i, node := range sorted {
			gnode := node.(graphNode)
			result[i] = gnode.node
		}
	}
	
	return result, nil
}

//return all cycles that form the basis of the graph
func (self *graph) Cycles() ([][]interface{}, error) {

	gg, _ := self.getGonumDirected()
	var cycles [][]gonum.Node
	
	if self.isDirected() { 
		cycles = topo.DirectedCyclesIn(gg)
	
	} else {
		cycles = topo.UndirectedCyclesIn(gonum.Undirect{gg})
	}
	
	result := make([][]interface{}, len(cycles))
	for i, cycle := range cycles {
		nc := make([]interface{}, len(cycle))
		for j, node := range cycle {
			gnode := node.(graphNode)
			nc[j] = gnode.node
		}
		result[i] = nc
	}

	return result, nil
}

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

func (self *graph) isDirected() bool {

	prop := self.GetProperty("directed").(*constProperty)
	return prop.value.(bool)
}
