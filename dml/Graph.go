// Vector
package dml

import (
	"encoding/gob"
	"fmt"

	uuid "github.com/satori/go.uuid"
	gonum "gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	topo "gonum.org/v1/gonum/graph/topo"
	trav "gonum.org/v1/gonum/graph/traverse"

	"github.com/OpenCollaborationPlatform/OCP/utils"
)

var nodeKey = []byte("__graph_node")
var edgeKey = []byte("__graph_edge")

func init() {
	gob.Register(new(struct{}))
	gob.Register(new(graphEdge))
}

//map type: stores requested data type by index (0-based)
type graph struct {
	*DataImpl

	//nodeData *datastore.MapVersioned
	//edgeData *datastore.MapVersioned
}

func NewGraph(rntm *Runtime) (Object, error) {

	base, err := NewDataBaseClass(rntm)
	if err != nil {
		return nil, err
	}

	//build the graph object
	gr := &graph{
		base,
	}

	//add properties
	gr.AddProperty("directed", MustNewDataType("bool"), true, true)
	gr.AddProperty("node", MustNewDataType("type"), MustNewDataType("int"), true)
	gr.AddProperty("edge", MustNewDataType("type"), MustNewDataType("none"), true)

	//add methods
	gr.AddMethod("AddNode", MustNewIdMethod(gr.AddNode, false))
	gr.AddMethod("NewNode", MustNewIdMethod(gr.NewNode, false))
	gr.AddMethod("RemoveNode", MustNewIdMethod(gr.RemoveNode, false))
	gr.AddMethod("HasNode", MustNewIdMethod(gr.HasNode, true))
	gr.AddMethod("Nodes", MustNewIdMethod(gr.Nodes, true))
	gr.AddMethod("AddEdge", MustNewIdMethod(gr.AddEdge, false))
	gr.AddMethod("NewEdge", MustNewIdMethod(gr.NewEdge, false))
	gr.AddMethod("RemoveEdge", MustNewIdMethod(gr.RemoveEdge, false))
	gr.AddMethod("RemoveEdgeBetween", MustNewIdMethod(gr.RemoveEdgeBetween, false))
	gr.AddMethod("HasEdge", MustNewIdMethod(gr.HasEdge, true))
	gr.AddMethod("HasEdgeBetween", MustNewIdMethod(gr.HasEdgeBetween, true))
	gr.AddMethod("Edge", MustNewIdMethod(gr.Edge, true))

	gr.AddMethod("FromNode", MustNewIdMethod(gr.FromNode, true))
	gr.AddMethod("ToNode", MustNewIdMethod(gr.ToNode, true))
	gr.AddMethod("Sorted", MustNewIdMethod(gr.Sorted, true))
	gr.AddMethod("Cycles", MustNewIdMethod(gr.Cycles, true))
	gr.AddMethod("ReachableNodes", MustNewIdMethod(gr.ReachableNodes, true))

	return gr, nil
}

//							Graph helpers
//******************************************************************************
type graphEdge struct {
	Source interface{}
	Target interface{}
}

type graphNode struct {
	id   int64
	node interface{}
}

func (self graphNode) ID() int64 {
	return self.id
}

func (self *graph) getGonumDirected(id Identifier) (gonum.Directed, map[interface{}]gonum.Node) {

	directed := simple.NewDirectedGraph()
	mapper := make(map[interface{}]gonum.Node, 0)

	dbNodes, err := self.GetDBMapVersioned(id, nodeKey)
	if err != nil {
		return directed, mapper
	}
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return directed, mapper
	}

	//add all nodes first: keys are nodes!
	keys, _ := dbNodes.GetKeys()
	for _, key := range keys {

		idx := directed.NewNode().ID()
		node := graphNode{idx, key}
		directed.AddNode(node)
		mapper[key] = node
	}

	//all edges next
	keys, _ = dbEdges.GetKeys()
	for _, key := range keys {
		data, err := dbEdges.Read(key)
		if err != nil {
			continue
		}
		edge := data.(*graphEdge)
		graphedge := directed.NewEdge(mapper[edge.Source], mapper[edge.Target])
		directed.SetEdge(graphedge)
	}

	return directed, mapper
}

func (self *graph) getGonumGraph(id Identifier) (gonum.Graph, map[interface{}]gonum.Node) {
	directed, mapper := self.getGonumDirected(id)
	if !self.isDirected() {
		return &gonum.Undirect{directed}, mapper
	}

	return directed, mapper
}

//inverse of keyToDB
func (self *graph) dbToType(key interface{}, dt DataType) (interface{}, error) {

	if dt.IsComplex() {

		id, ok := key.(*Identifier)
		if !ok {
			return nil, newInternalError(Error_Fatal, "Complex datatype, but key is not identifier")
		}
		set, err := self.rntm.getObjectSet(*id)
		if err != nil {
			return nil, utils.StackError(err, "Invalid object stored")
		}
		return set, nil

	} else if dt.IsType() {

		val, _ := key.(string)
		return NewDataType(val)
	}

	//everything else is simply used
	return key, nil
}

//convert all possible types to something usable in the DB
func (self *graph) typeToDB(input interface{}, dt DataType) interface{} {

	if dt.IsComplex() {

		if set, ok := input.(dmlSet); ok {
			return set.id
		}
		if id, ok := input.(Identifier); ok {
			return id
		}

	} else if dt.IsType() {

		val, _ := input.(DataType)
		return val.AsString()
	}

	//everything else is simply used as key
	return UnifyDataType(input)
}

func (self *graph) Nodes(id Identifier) ([]interface{}, error) {

	dbNodes, err := self.GetDBMapVersioned(id, nodeKey)
	if err != nil {
		return nil, err
	}

	keys, err := dbNodes.GetKeys()
	if err != nil {
		return nil, utils.StackError(err, "Unable to get keys from DB map")
	}
	result := make([]interface{}, len(keys))
	for i, key := range keys {

		typeVal, err := self.dbToType(key, self.nodeDataType())
		if err != nil {
			return nil, utils.StackError(err, "Unable to get convert key data to type")
		}

		result[i] = typeVal
	}
	return result, nil
}

func (self *graph) HasNode(id Identifier, node interface{}) (bool, error) {

	//check key type
	dt := self.nodeDataType()
	err := dt.MustBeTypeOf(node)
	if err != nil {
		return false, utils.StackError(err, "Wrong type for node")
	}

	dbNodes, err := self.GetDBMapVersioned(id, nodeKey)
	if err != nil {
		return false, err
	}

	dbkey := self.typeToDB(node, dt)
	return dbNodes.HasKey(dbkey), nil
}

func (self *graph) AddNode(id Identifier, value interface{}) error {

	//check key type
	dt := self.nodeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot add node, has wrong type")
	}

	dbentry := self.typeToDB(value, dt)

	dbNodes, err := self.GetDBMapVersioned(id, nodeKey)
	if err != nil {
		return err
	}
	return utils.StackError(dbNodes.Write(dbentry, struct{}{}), "Unable to write to DB")
}

//creates a new entry with a all new type, returns the new node
func (self *graph) NewNode(id Identifier) (interface{}, error) {

	//check key type
	dt := self.nodeDataType()

	//create a new entry
	var result interface{}
	if dt.IsComplex() {
		set, err := self.rntm.constructObjectSet(dt, id)
		if err != nil {
			return nil, utils.StackError(err, "Construction of object failed")
		}
		result = set

	} else {
		result = dt.GetDefaultValue()
	}

	err := self.AddNode(id, result)
	if err != nil {
		return nil, err
	}

	if dt.IsComplex() {
		set := result.(dmlSet)
		if data, ok := set.obj.(Data); ok {
			data.Created(result.(dmlSet).id)
		}
	}

	//write new entry
	return result, nil
}

func (self *graph) RemoveNode(id Identifier, value interface{}) error {

	if has, _ := self.HasNode(id, value); !has {
		return newUserError(Error_Key_Not_Available, "No such node available, cannot remove")
	}

	//make sure to use unified types
	value = UnifyDataType(value)

	dt := self.nodeDataType()

	dbNodes, err := self.GetDBMapVersioned(id, nodeKey)
	if err != nil {
		return err
	}

	dbentry := self.typeToDB(value, dt)
	err = dbNodes.Remove(dbentry)
	if err != nil {
		return utils.StackError(err, "Unable to remove from DB")
	}

	//remove all edges that connect to this node
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return err
	}
	keys, err := dbEdges.GetKeys()
	if err != nil {
		return utils.StackError(err, "Unable to get edge keys from DB")
	}
	for _, key := range keys {

		//check if this is the edge
		data, err := dbEdges.Read(key)
		if err != nil {
			return utils.StackError(err, "Unable to read edge from DB")
		}
		edge, ok := data.(*graphEdge)
		if !ok {
			return newInternalError(Error_Fatal, "Edge stored as wrong datatype")
		}

		if edge.Source == value || edge.Target == value {
			if err := dbEdges.Remove(key); err != nil {
				return utils.StackError(err, "Unable to remove edge from DB")
			}
		}
	}

	return nil
}

func (self *graph) AddEdge(id Identifier, source, target, value interface{}) error {

	//check edge type
	dt := self.edgeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Value has wrong type for edge")
	}

	//make sure to use unified types
	source = UnifyDataType(source)
	target = UnifyDataType(target)
	value = UnifyDataType(value)

	//check if we have the two nodes
	hassource, err := self.HasNode(id, source)
	if err != nil {
		return utils.StackError(err, "Checking for source node failed")
	}
	hastarget, err := self.HasNode(id, target)
	if err != nil {
		return utils.StackError(err, "Checking for target node failed")
	}
	if !hassource || !hastarget {
		return newUserError(Error_Key_Not_Available, "Source and target nodes must be available, but are not")
	}

	//check if edge already exists
	if has, _ := self.HasEdgeBetween(id, source, target); has {
		return newUserError(Error_Operation_Invalid, "Edge does already exist")
	}

	//write
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return err
	}
	dbentry := self.typeToDB(value, dt)
	edge := graphEdge{source, target}
	return utils.StackError(dbEdges.Write(dbentry, edge), "Unable to write into DB")
}

//creates a new entry with a all new type, returns the new edge
func (self *graph) NewEdge(id Identifier, source, target interface{}) (interface{}, error) {

	//check if we have the two nodes
	hassource, err := self.HasNode(id, source)
	if err != nil {
		return nil, utils.StackError(err, "Checking for source node failed")
	}
	hastarget, err := self.HasNode(id, target)
	if err != nil {
		return nil, utils.StackError(err, "Checking for target node failed")
	}
	if !hassource || !hastarget {
		return nil, newUserError(Error_Key_Not_Available, "Source and target nodes must be available, but are not")
	}

	//make sure to use unified types
	source = UnifyDataType(source)
	target = UnifyDataType(target)

	//check if edge already exists
	if has, _ := self.HasEdgeBetween(id, source, target); has {
		return nil, newUserError(Error_Operation_Invalid, "Edge does already exist")
	}

	//create a new entry if possible
	dt := self.edgeDataType()
	var result interface{}
	if dt.IsComplex() {
		set, err := self.rntm.constructObjectSet(dt, id)
		if err != nil {
			return nil, utils.StackError(err, "Construction of edge object failed")
		}
		result = set

	} else if dt.IsNone() {

		//use a custom internal scheme
		result = uuid.NewV4().Bytes()

	} else {
		//all other type would result in a default value that is always the same,
		//hence not allowing to add multiple edges. This is stupid!
		return nil, fmt.Errorf("New edge only workes with complex or none edge type")
	}

	//write
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return nil, err
	}
	dbentry := self.typeToDB(result, dt)
	edge := graphEdge{source, target}

	err = dbEdges.Write(dbentry, edge)
	if err != nil {
		return nil, err
	}
	if dt.IsComplex() {
		set := result.(dmlSet)
		if data, ok := set.obj.(Data); ok {
			data.Created(result.(dmlSet).id)
		}
	}
	return result, nil
}

func (self *graph) RemoveEdge(id Identifier, value interface{}) error {

	//check key type
	dt := self.edgeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return utils.StackError(err, "Cannot remove edge, has wrong type")
	}

	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return err
	}
	dbentry := self.typeToDB(value, dt)
	err = dbEdges.Remove(dbentry)
	return utils.StackError(err, "Unable to remove from DB")
}

func (self *graph) RemoveEdgeBetween(id Identifier, source, target interface{}) error {

	//check if we have the two nodes
	hassource, err := self.HasNode(id, source)
	if err != nil {
		return utils.StackError(err, "Checking for source node failed")
	}
	hastarget, err := self.HasNode(id, target)
	if err != nil {
		return utils.StackError(err, "Checking for target node failed")
	}
	if !hassource || !hastarget {
		return newUserError(Error_Key_Not_Available, "Source and target nodes must be available, but are not")
	}

	//make sure to use unified types
	source = UnifyDataType(source)
	target = UnifyDataType(target)

	//we need to iterate all edges!
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return err
	}
	keys, err := dbEdges.GetKeys()
	if err != nil {
		return utils.StackError(err, "Unable to get edge keys from DB")
	}

	for _, key := range keys {

		//check if this is the edge to remove
		data, err := dbEdges.Read(key)
		if err != nil {
			return utils.StackError(err, "Unable to read edge from DB")
		}
		edge := data.(*graphEdge)

		if (edge.Source == source && edge.Target == target) ||
			(!self.isDirected() && (edge.Source == target && edge.Target == source)) {

			//remove it!
			return utils.StackError(dbEdges.Remove(key), "Umable to remove key from DB")
		}
	}

	return newUserError(Error_Operation_Invalid, "No edge between the two nodes, cannot remove")
}

func (self *graph) HasEdge(id Identifier, value interface{}) (bool, error) {

	//check key type
	dt := self.edgeDataType()
	err := dt.MustBeTypeOf(value)
	if err != nil {
		return false, utils.StackError(err, "Edge has wrong type")
	}

	dbkey := self.typeToDB(value, dt)

	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return false, err
	}
	return dbEdges.HasKey(dbkey), nil
}

func (self *graph) HasEdgeBetween(id Identifier, source, target interface{}) (bool, error) {

	//check if we have the two nodes
	hassource, err := self.HasNode(id, source)
	if err != nil {
		return false, utils.StackError(err, "Checking for source node failed")
	}
	hastarget, err := self.HasNode(id, target)
	if err != nil {
		return false, utils.StackError(err, "Checking for target node failed")
	}
	if !hassource || !hastarget {
		return false, newUserError(Error_Operation_Invalid, "Source and target nodes must be available, but are not")
	}

	//make sure to use unified types
	source = UnifyDataType(source)
	target = UnifyDataType(target)

	//we need to iterate all edges!
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return false, err
	}
	keys, err := dbEdges.GetKeys()
	if err != nil {
		return false, utils.StackError(err, "Unable to access edge keys in DB")
	}

	for _, key := range keys {

		//check if this is the edge
		data, err := dbEdges.Read(key)
		if err != nil {
			return false, utils.StackError(err, "Unable to read edge from DB")
		}
		edge, ok := data.(*graphEdge)
		if !ok {
			return false, newInternalError(Error_Fatal, "Graph edge stored in wrong type")
		}

		if (edge.Source == source && edge.Target == target) ||
			(!self.isDirected() && (edge.Source == target && edge.Target == source)) {

			return true, nil
		}
	}

	return false, nil
}

func (self *graph) Edge(id Identifier, source, target interface{}) (interface{}, error) {

	//check key type
	ndt := self.nodeDataType()
	err := ndt.MustBeTypeOf(source)
	if err != nil {
		return nil, utils.StackError(err, "Checking for target node failed")
	}
	err = ndt.MustBeTypeOf(target)
	if err != nil {
		return nil, utils.StackError(err, "Checking for target node failed")
	}

	//make sure to use unified types
	source = UnifyDataType(source)
	target = UnifyDataType(target)

	//we need to iterate all edges!
	dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
	if err != nil {
		return nil, err
	}
	keys, err := dbEdges.GetKeys()
	if err != nil {
		return nil, utils.StackError(err, "Unable to access edge keys in DB")
	}

	for _, key := range keys {

		//check if this is the edge
		data, err := dbEdges.Read(key)
		if err != nil {
			return nil, utils.StackError(err, "Unable to read edge from DB")
		}
		edge := data.(*graphEdge)

		if (edge.Source == source && edge.Target == target) ||
			(!self.isDirected() && (edge.Source == target && edge.Target == source)) {

			return self.dbToType(key, self.edgeDataType())
		}
	}

	return nil, newUserError(Error_Operation_Invalid, "No edge between nodes")
}

//*****************************************************************************
//			Graph functions
//*****************************************************************************

//return all nodes reachable from the given one
func (self *graph) FromNode(id Identifier, node interface{}) ([]interface{}, error) {

	//check if node exists
	has, err := self.HasNode(id, node)
	if err != nil {
		return nil, utils.StackError(err, "Checking for node failed")
	}
	if !has {
		return nil, newUserError(Error_Key_Not_Available, "Node does not exist")
	}

	//make sure to use unified types
	node = UnifyDataType(node)

	gg, mapper := self.getGonumGraph(id)
	res := gg.From(mapper[node].ID())

	result := make([]interface{}, 0)
	for k := res.Next(); k; k = res.Next() {
		gnode := res.Node().(graphNode)
		result = append(result, gnode.node)
	}

	return result, nil
}

//return all nodes which can reach the given one
func (self *graph) ToNode(id Identifier, node interface{}) ([]interface{}, error) {

	if !self.isDirected() {
		return self.FromNode(id, node)
	}

	//check if node exists
	has, err := self.HasNode(id, node)
	if err != nil {
		return nil, utils.StackError(err, "Checking for node failed")
	}
	if !has {
		return nil, newUserError(Error_Key_Not_Available, "Node does not exist")
	}

	//make sure to use unified types
	node = UnifyDataType(node)

	gg, mapper := self.getGonumDirected(id)
	res := gg.To(mapper[node].ID())

	result := make([]interface{}, 0)
	for k := res.Next(); k; k = res.Next() {
		gnode := res.Node().(graphNode)
		result = append(result, gnode.node)
	}
	return result, nil
}

//return a topological sort of all nodes
//the returned value is a slice of interfaces. The interfaces are nodes or
//slices of nodes if they form a circle that cannot be ordered
func (self *graph) Sorted(id Identifier) ([]interface{}, error) {

	if !self.isDirected() {
		return nil, newUserError(Error_Operation_Invalid, "Sorting is only available for directed graphs")
	}

	gg, _ := self.getGonumDirected(id)
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
func (self *graph) Cycles(id Identifier) ([][]interface{}, error) {

	gg, _ := self.getGonumDirected(id)
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

func (self *graph) ReachableNodes(id Identifier, node interface{}) ([]interface{}, error) {

	//check if node exists
	has, err := self.HasNode(id, node)
	if err != nil {
		return nil, utils.StackError(err, "Checking for node failed")
	}
	if !has {
		return nil, newUserError(Error_Key_Not_Available, "Node does not exist")
	}

	//make sure to use unified types
	node = UnifyDataType(node)

	//get the graph
	graph, mapper := self.getGonumGraph(id)

	//build the search fnc
	nodes := make([]gonum.Node, 0)
	search := trav.BreadthFirst{
		Visit: func(node gonum.Node) {
			nodes = append(nodes, node)
		},
	}

	//run the search
	search.Walk(graph, mapper[node], nil)

	//convert the result nodes to dml interface values
	result := make([]interface{}, 0)
	for _, node := range nodes {
		gnode := node.(graphNode)
		result = append(result, gnode.node)
	}
	return result, nil
}

//*****************************************************************************
//			Internal functions
//*****************************************************************************
func (self *graph) GetSubobjects(id Identifier) ([]dmlSet, error) {

	//get default objects
	res, err := self.DataImpl.GetSubobjects(id)
	if err != nil {
		return nil, utils.StackError(err, "Get Data Subobjects failed")
	}

	//handle nodes
	dt := self.nodeDataType()
	if dt.IsComplex() {
		dbNodes, err := self.GetDBMapVersioned(id, nodeKey)
		if err != nil {
			return nil, err
		}
		keys, err := dbNodes.GetKeys()
		if err == nil {

			for _, key := range keys {

				val, err := self.dbToType(key, dt)
				if err != nil {
					return nil, err
				}
				if set, ok := val.(dmlSet); ok {
					res = append(res, set)
				}
			}
		}
	}

	//handle edges
	dt = self.edgeDataType()
	if dt.IsComplex() {

		dbEdges, err := self.GetDBMapVersioned(id, edgeKey)
		if err != nil {
			return nil, err
		}
		keys, err := dbEdges.GetKeys()
		if err == nil {

			for _, key := range keys {

				val, err := self.dbToType(key, dt)
				if err != nil {
					return nil, err
				}
				if set, ok := val.(dmlSet); ok {
					res = append(res, set)
				}
			}
		} else {
			return nil, utils.StackError(err, "Unable to access edge keys in DB")
		}
	}

	return res, nil
}

func (self *graph) nodeDataType() DataType {

	prop, _ := self.GetProperty("node").GetValue(Identifier{})
	return prop.(DataType)
}

func (self *graph) edgeDataType() DataType {

	prop, _ := self.GetProperty("edge").GetValue(Identifier{})
	return prop.(DataType)
}

func (self *graph) isDirected() bool {

	prop := self.GetProperty("directed").(*constProperty)
	return prop.value.(bool)
}
