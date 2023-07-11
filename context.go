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
  GQLType *graphql.Object
  Reflect reflect.Type
}

func NewNodeDef(type_name string, reflect reflect.Type, load_func NodeLoadFunc, gql_type *graphql.Object) NodeDef {
  return NodeDef{
    Type: NodeType(type_name),
    Load: load_func,
    GQLType: gql_type,
    Reflect: reflect,
  }
}

type Context struct {
  DB * badger.DB
  Log Logger
  Types map[uint64]NodeDef
  GQL GQLContext
}

func (ctx * Context) RebuildSchema() error {
  schemaConfig := graphql.SchemaConfig{
    Types: ctx.GQL.TypeList,
    Query: ctx.GQL.Query,
    Mutation: ctx.GQL.Mutation,
    Subscription: ctx.GQL.Subscription,
  }

  schema, err := graphql.NewSchema(schemaConfig)
  if err != nil {
    return err
  }

  ctx.GQL.Schema = schema
  return nil
}

func (ctx * Context) AddGQLType(gql_type graphql.Type) {
  ctx.GQL.TypeList = append(ctx.GQL.TypeList, gql_type)
}

func (ctx * Context) RegisterNodeType(def NodeDef) error {
  if def.Load == nil {
    return fmt.Errorf("Cannot register a node without a load function: %s", def.Type)
  }

  if def.Reflect == nil {
    return fmt.Errorf("Cannot register a node without a reflect type: %s", def.Type)
  }

  if def.GQLType == nil {
    return fmt.Errorf("Cannot register a node without a gql type: %s", def.Type)
  }

  type_hash := def.Type.Hash()
  _, exists := ctx.Types[type_hash]
  if exists == true {
    return fmt.Errorf("Cannot register node of type %s, type already exists in context", def.Type)
  }

  ctx.Types[type_hash] = def

  if def.Reflect.Implements(ctx.GQL.NodeType) {
    ctx.GQL.ValidNodes[def.Reflect] = def.GQLType
  }
  if def.Reflect.Implements(ctx.GQL.LockableType) {
    ctx.GQL.ValidLockables[def.Reflect] = def.GQLType
  }
  if def.Reflect.Implements(ctx.GQL.ThreadType) {
    ctx.GQL.ValidThreads[def.Reflect] = def.GQLType
  }
  ctx.GQL.TypeList = append(ctx.GQL.TypeList, def.GQLType)

  return nil
}

type TypeList []graphql.Type
type ObjTypeMap map[reflect.Type]*graphql.Object
type FieldMap map[string]*graphql.Field

type GQLContext struct {
  Schema graphql.Schema

  NodeType reflect.Type
  LockableType reflect.Type
  ThreadType reflect.Type

  TypeList TypeList

  ValidNodes ObjTypeMap
  ValidLockables ObjTypeMap
  ValidThreads ObjTypeMap

  Query *graphql.Object
  Mutation *graphql.Object
  Subscription *graphql.Object
}

func NewGQLContext() GQLContext {
  query := graphql.NewObject(graphql.ObjectConfig{
    Name: "Query",
    Fields: graphql.Fields{},
  })

  mutation := graphql.NewObject(graphql.ObjectConfig{
    Name: "Mutation",
    Fields: graphql.Fields{},
  })

  subscription := graphql.NewObject(graphql.ObjectConfig{
    Name: "Subscription",
    Fields: graphql.Fields{},
  })

  ctx := GQLContext{
    Schema: graphql.Schema{},
    TypeList: TypeList{},
    ValidNodes: ObjTypeMap{},
    NodeType: reflect.TypeOf((*Node)(nil)).Elem(),
    ValidThreads: ObjTypeMap{},
    ThreadType: reflect.TypeOf((*Thread)(nil)).Elem(),
    ValidLockables: ObjTypeMap{},
    LockableType: reflect.TypeOf((*Lockable)(nil)).Elem(),
    Query: query,
    Mutation: mutation,
    Subscription: subscription,
  }

  return ctx
}

func NewContext(db * badger.DB, log Logger) * Context {
  ctx := &Context{
    GQL: NewGQLContext(),
    DB: db,
    Log: log,
    Types: map[uint64]NodeDef{},
  }

  err := ctx.RegisterNodeType(NewNodeDef("graph_node", reflect.TypeOf((*GraphNode)(nil)), LoadGraphNode, GQLTypeGraphNode()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef("simple_lockable", reflect.TypeOf((*SimpleLockable)(nil)), LoadSimpleLockable, GQLTypeSimpleLockable()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef("simple_thread", reflect.TypeOf((*SimpleThread)(nil)), LoadSimpleThread, GQLTypeSimpleThread()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef("gql_thread", reflect.TypeOf((*GQLThread)(nil)), LoadGQLThread, GQLTypeGQLThread()))
  if err != nil {
    panic(err)
  }

  ctx.AddGQLType(GQLTypeSignal())

  ctx.GQL.Query.AddFieldConfig("Self", GQLQuerySelf())

  ctx.GQL.Subscription.AddFieldConfig("Update", GQLSubscriptionUpdate())
  ctx.GQL.Subscription.AddFieldConfig("Self", GQLSubscriptionSelf())

  ctx.GQL.Mutation.AddFieldConfig("SendUpdate", GQLMutationSendUpdate())

  err = ctx.RebuildSchema()
  if err != nil {
    panic(err)
  }

  return ctx
}
