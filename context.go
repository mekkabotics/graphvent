package graphvent

import (
  "github.com/graphql-go/graphql"
  badger "github.com/dgraph-io/badger/v3"
  "reflect"
  "fmt"
)

// For persistance, each node needs the following functions(* is a placeholder for the node/state type):
// Load*State - StateLoadFunc that returns the NodeState interface to attach to the node
// Load* - NodeLoadFunc that returns the GraphNode restored from it's loaded state

// For convenience, the following functions are a good idea to define for composability:
// Restore*State - takes in the nodes serialized data to allow for easier nesting of inherited Load*State functions
// Save*State - serialize the node into it's json counterpart to be included as part of a larger json

type NodeLoadFunc func(*Context, NodeID, []byte, NodeMap)(Node, error)
type NodeDef struct {
  Load NodeLoadFunc
  Type NodeType
}

func NewNodeDef(type_name string, load_func NodeLoadFunc) NodeDef {
  return NodeDef{
    Type: NodeType(type_name),
    Load: load_func,
  }
}

type Context struct {
  DB * badger.DB
  Log Logger
  Types map[uint64]NodeDef
  GQL * GQLContext
}

func (ctx * Context) RegisterNodeType(type_name string, load_func NodeLoadFunc) error {
  if load_func == nil {
    return fmt.Errorf("Cannot register a node without a load function")
  }

  def := NodeDef{
    Type: NodeType(type_name),
    Load: load_func,
  }

  type_hash := def.Type.Hash()
  _, exists := ctx.Types[type_hash]
  if exists == true {
    return fmt.Errorf("Cannot register node of type %s, type already exists in context", type_name)
  }

  ctx.Types[type_hash] = def
  return nil
}

type TypeList []graphql.Type
type ObjTypeMap map[reflect.Type]*graphql.Object
type FieldMap map[string]*graphql.Field

type GQLContext struct {
  Schema graphql.Schema
  ValidNodes ObjTypeMap
  NodeType reflect.Type
  ValidLockables ObjTypeMap
  LockableType reflect.Type
  ValidThreads ObjTypeMap
  ThreadType reflect.Type
}

/*func NewGQLContext(additional_types TypeList, extended_types ObjTypeMap, extended_queries FieldMap, extended_subscriptions FieldMap, extended_mutations FieldMap) (*GQLContext, error) {
  type_list := TypeList{
    GQLTypeSignalInput(),
  }

  for _, gql_type := range(additional_types) {
    type_list = append(type_list, gql_type)
  }

  type_map := ObjTypeMap{}
  type_map[reflect.TypeOf((*BaseLockable)(nil))] =  GQLTypeBaseLockable()
  type_map[reflect.TypeOf((*BaseThread)(nil))] = GQLTypeBaseThread()
  type_map[reflect.TypeOf((*GQLThread)(nil))] = GQLTypeGQLThread()
  type_map[reflect.TypeOf((*BaseSignal)(nil))] = GQLTypeSignal()

  for go_t, gql_t := range(extended_types) {
    type_map[go_t] = gql_t
  }

  valid_nodes := ObjTypeMap{}
  valid_lockables := ObjTypeMap{}
  valid_threads := ObjTypeMap{}

  node_type := reflect.TypeOf((*GraphNode)(nil)).Elem()
  lockable_type := reflect.TypeOf((*Lockable)(nil)).Elem()
  thread_type := reflect.TypeOf((*Thread)(nil)).Elem()

  for go_t, gql_t := range(type_map) {
    if go_t.Implements(node_type) {
      valid_nodes[go_t] = gql_t
    }
    if go_t.Implements(lockable_type) {
      valid_lockables[go_t] = gql_t
    }
    if go_t.Implements(thread_type) {
      valid_threads[go_t] = gql_t
    }
    type_list = append(type_list, gql_t)
  }

  queries := graphql.Fields{
    "Self": GQLQuerySelf(),
  }

  for key, val := range(extended_queries) {
    queries[key] = val
  }

  subscriptions := graphql.Fields{
    "Update": GQLSubscriptionUpdate(),
    "Self": GQLSubscriptionSelf(),
  }

  for key, val := range(extended_subscriptions) {
    subscriptions[key] = val
  }

  mutations := graphql.Fields{
    "SendUpdate": GQLMutationSendUpdate(),
  }

  for key, val := range(extended_mutations) {
    mutations[key] = val
  }

  schemaConfig  := graphql.SchemaConfig{
    Types: type_list,
    Query: graphql.NewObject(graphql.ObjectConfig{
      Name: "Query",
      Fields: queries,
    }),
    Mutation: graphql.NewObject(graphql.ObjectConfig{
      Name: "Mutation",
      Fields: mutations,
    }),
    Subscription: graphql.NewObject(graphql.ObjectConfig{
      Name: "Subscription",
      Fields: subscriptions,
    }),
  }

  schema, err := graphql.NewSchema(schemaConfig)
  if err != nil{
    return nil, err
  }

  ctx := GQLContext{
    Schema: schema,
    ValidNodes: valid_nodes,
    NodeType: node_type,
    ValidThreads: valid_threads,
    ThreadType: thread_type,
    ValidLockables: valid_lockables,
    LockableType: lockable_type,
  }

  return &ctx, nil
}*/

func NewContext(db * badger.DB, log Logger, extra_nodes map[string]NodeLoadFunc, types TypeList, type_map ObjTypeMap, queries FieldMap, subscriptions FieldMap, mutations FieldMap) * Context {
  /*gql, err := NewGQLContext(types, type_map, queries, subscriptions, mutations)
  if err != nil {
    panic(err)
  }*/

  ctx := &Context{
    GQL: nil,
    DB: db,
    Log: log,
    Types: map[uint64]NodeDef{},
  }



  err := ctx.RegisterNodeType("graph_node", LoadGraphNode)
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType("simple_lockable", LoadSimpleLockable)
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType("simple_thread", LoadSimpleThread)
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType("gql_thread", LoadGQLThread)
  if err != nil {
    panic(err)
  }

  for name, load_fn := range(extra_nodes) {
    err := ctx.RegisterNodeType(name, load_fn)
    if err != nil {
      panic(err)
    }
  }

  return ctx
}
