package graphvent

import (
  "github.com/graphql-go/graphql"
  badger "github.com/dgraph-io/badger/v3"
  "reflect"
  "fmt"
)

// NodeLoadFunc is the footprint of the function used to create a new node in memory from persisted bytes
type NodeLoadFunc func(*Context, NodeID, []byte, NodeMap)(Node, error)

// A NodeDef is a description of a node that can be added to a Context
type NodeDef struct {
  Load NodeLoadFunc
  Type NodeType
  GQLType *graphql.Object
  Reflect reflect.Type
}

// Create a new Node def, extracting the Type and Reflect from example
func NewNodeDef(example Node, load_func NodeLoadFunc, gql_type *graphql.Object) NodeDef {
  return NodeDef{
    Type: example.Type(),
    Load: load_func,
    GQLType: gql_type,
    Reflect: reflect.TypeOf(example),
  }
}

// A Context is all the data needed to run a graphvent
type Context struct {
  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Log is an interface used to record events happening
  Log Logger
  // A mapping between type hashes and their corresponding node definitions
  Types map[uint64]NodeDef
  // GQL substructure
  GQL GQLContext
}

// Recreate the GQL schema after making changes
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

// Add a non-node type to the gql context
func (ctx * Context) AddGQLType(gql_type graphql.Type) {
  ctx.GQL.TypeList = append(ctx.GQL.TypeList, gql_type)
}

// Add a node to a context, returns an error if the def is invalid or already exists in the context
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

// Map of go types to graphql types
type ObjTypeMap map[reflect.Type]*graphql.Object

// GQL Specific Context information
type GQLContext struct {
  // Generated GQL schema
  Schema graphql.Schema

  // Interface types to compare against
  NodeType reflect.Type
  LockableType reflect.Type
  ThreadType reflect.Type

  // List of GQL types
  TypeList []graphql.Type

  // Interface type maps to map go types of specific interfaces to gql types
  ValidNodes ObjTypeMap
  ValidLockables ObjTypeMap
  ValidThreads ObjTypeMap

  Query *graphql.Object
  Mutation *graphql.Object
  Subscription *graphql.Object
}

// Create a new GQL context without any content
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
    TypeList: []graphql.Type{},
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

// Create a new Context with all the library content added
func NewContext(db * badger.DB, log Logger) * Context {
  ctx := &Context{
    GQL: NewGQLContext(),
    DB: db,
    Log: log,
    Types: map[uint64]NodeDef{},
  }

  err := ctx.RegisterNodeType(NewNodeDef((*GraphNode)(nil), LoadGraphNode, GQLTypeGraphNode()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef((*SimpleLockable)(nil), LoadSimpleLockable, GQLTypeSimpleLockable()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef((*SimpleThread)(nil), LoadSimpleThread, GQLTypeSimpleThread()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef((*GQLThread)(nil), LoadGQLThread, GQLTypeGQLThread()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef((*User)(nil), LoadUser, GQLTypeUser()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef((*PerNodePolicy)(nil), LoadPerNodePolicy, GQLTypeGraphNode()))
  if err != nil {
    panic(err)
  }
  err = ctx.RegisterNodeType(NewNodeDef((*AllNodePolicy)(nil), LoadAllNodePolicy, GQLTypeGraphNode()))
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
