package graphvent

import (
	"crypto/ecdh"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"

	badger "github.com/dgraph-io/badger/v3"
)

var (
  NodeNotFoundError = errors.New("Node not found in DB")
  ECDH = ecdh.X25519()
)

type TypeInfo struct {
  Type graphql.Type
}

type ExtensionInfo struct {
  Interface *graphql.Interface
  Fields map[string][]int
  Data interface{}
}

type SignalInfo struct {
  Type graphql.Type
}

type FieldIndex struct {
  Extension ExtType
  Field string
}

type NodeInfo struct {
  GQL *graphql.Object
  Extensions []ExtType
  Fields map[string]FieldIndex
}

// A Context stores all the data to run a graphvent process
type Context struct {

  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Logging interface
  Log Logger

  // Mapped types
  TypeMap map[SerializedType]TypeInfo
  TypeTypes map[reflect.Type]SerializedType

  // Map between database extension hashes and the registered info
  Extensions map[ExtType]ExtensionInfo
  ExtensionTypes map[reflect.Type]ExtType

  // Map between database type hashes and the registered info
  Nodes map[NodeType]NodeInfo
  NodeTypes map[string]NodeType

  // Routing map to all the nodes local to this context
  nodeMapLock sync.RWMutex
  nodeMap map[NodeID]*Node
}

func BuildSchema(ctx *Context, query, mutation *graphql.Object) (graphql.Schema, error) {
  types := []graphql.Type{}

  subscription := graphql.NewObject(graphql.ObjectConfig{

  })

  return graphql.NewSchema(graphql.SchemaConfig{
    Types: types,
    Query: query,
    Subscription: subscription,
    Mutation: mutation,
  })
}

func RegisterSignal[S Signal](ctx *Context) error {
  reflect_type := reflect.TypeFor[S]()
  signal_type := SignalTypeFor[S]()

  err := RegisterObject[S](ctx)
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered SignalType: %+v - %+v", reflect_type, signal_type)
  return nil
}

func RegisterExtension[E any, T interface { *E; Extension}](ctx *Context, data interface{}) error {
  reflect_type := reflect.TypeFor[T]()
  ext_type := ExtType(SerializedTypeFor[E]())
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension %+v of type %+v, type already exists in context", reflect_type, ext_type)
  }

  gql_interface := graphql.NewInterface(graphql.InterfaceConfig{
    Name: reflect_type.String(),
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("resolve").(*ResolveContext)
      if ok == false {
        return nil
      }

      node, ok := p.Value.(NodeResult)
      if ok == false {
        return nil
      }

      type_info, type_exists := ctx.Context.Nodes[node.NodeType]
      if type_exists == false {
        return ctx.Context.Nodes[ctx.Context.NodeTypes["Base"]].GQL
      }

      return type_info.GQL
    },
    Fields: graphql.Fields{
      "ID": &graphql.Field{
        Type: graphql.String,
      },
    },
  })

  fields := map[string][]int{}
  for _, field := range reflect.VisibleFields(reflect.TypeFor[E]()) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      fields[gv_tag] = field.Index

      type_ser, type_mapped := ctx.TypeTypes[field.Type]
      if type_mapped == false {
        return fmt.Errorf("Extension %s has field %s of unregistered type %s", reflect_type, gv_tag, field.Type)
      }

      gql_interface.AddFieldConfig(gv_tag, &graphql.Field{
        Type: ctx.TypeMap[type_ser].Type,
      })
    }
  }

  err := RegisterObject[E](ctx)
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered ExtType: %+v - %+v", reflect_type, ext_type)

  ctx.Extensions[ext_type] = ExtensionInfo{
    Interface: gql_interface,
    Data: data,
    Fields: fields,
  }
  ctx.ExtensionTypes[reflect_type] = ext_type

  return nil
}

func RegisterNodeType(ctx *Context, name string, extensions []ExtType, mappings map[string]FieldIndex) error {
  node_type := NodeTypeFor(name, extensions, mappings)
  _, exists := ctx.Nodes[node_type]
  if exists == true {
    return fmt.Errorf("Cannot register node type %+v, type already exists in context", node_type)
  }

  ext_found := map[ExtType]bool{}
  for _, extension := range(extensions) {
    _, in_ctx := ctx.Extensions[extension]
    if in_ctx == false {
      return fmt.Errorf("Cannot register node type %+v, required extension %+v not in context", node_type, extension)
    }

    _, duplicate := ext_found[extension]
    if duplicate == true {
      return fmt.Errorf("Duplicate extension %+v found in extension list", extension)
    }

    ext_found[extension] = true
  }

  ctx.Nodes[node_type] = NodeInfo{
    Extensions: extensions,
    Fields: mappings,
  }
  ctx.NodeTypes[name] = node_type

  return nil
}

func RegisterObject[T any](ctx *Context) error {
  reflect_type := reflect.TypeFor[T]()
  serialized_type := SerializedTypeFor[T]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  gql := graphql.NewObject(graphql.ObjectConfig{
    Name: reflect_type.String(),
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      return reflect_type == reflect.TypeOf(p.Value)
    },
    Fields: graphql.Fields{},
  })
  
  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      field_type, mapped := ctx.TypeTypes[field.Type]
      if mapped == false {
        return fmt.Errorf("Object %+v has field %s of unknown type %+v", reflect_type, gv_tag, field_type)
      }
      gql.AddFieldConfig(gv_tag, &graphql.Field{
        Type: ctx.TypeMap[field_type].Type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          val, ok := p.Source.(T)
          if ok == false {
            return nil, fmt.Errorf("%s is not %s", reflect.TypeOf(p.Source), reflect_type)
          }
          
          value, err := reflect.ValueOf(val).FieldByIndexErr(field.Index)
          if err != nil {
            return nil, err
          }

          return value.Interface(), nil
        },
      })
    }
  }

  ctx.TypeTypes[reflect_type] = serialized_type
  ctx.TypeMap[serialized_type] = TypeInfo{
    Type: gql,
  }

  return nil
}

func RegisterScalar[T any](ctx *Context, to_json func(interface{})interface{}, from_json func(interface{})interface{}, from_ast func(ast.Value)interface{}) error {
  reflect_type := reflect.TypeFor[T]()
  serialized_type := SerializedTypeFor[T]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  gql := graphql.NewScalar(graphql.ScalarConfig{
    Name: reflect_type.String(),
    Serialize: to_json,
    ParseValue: from_json,
    ParseLiteral: from_ast,
  })

  ctx.TypeTypes[reflect_type] = serialized_type
  ctx.TypeMap[serialized_type] = TypeInfo{
    Type: gql,
  }

  return nil
}


func (ctx *Context) AddNode(id NodeID, node *Node) {
  ctx.nodeMapLock.Lock()
  ctx.nodeMap[id] = node
  ctx.nodeMapLock.Unlock()
}

func (ctx *Context) Node(id NodeID) (*Node, bool) {
  ctx.nodeMapLock.RLock()
  node, exists := ctx.nodeMap[id]
  ctx.nodeMapLock.RUnlock()
  return node, exists
}

func (ctx *Context) Delete(id NodeID) error {
  err := ctx.Unload(id)
  if err != nil {
    return err
  }
  // TODO: also delete any associated data
  return nil
}

func (ctx *Context) Unload(id NodeID) error {
  ctx.nodeMapLock.Lock()
  defer ctx.nodeMapLock.Unlock()
  node, exists := ctx.nodeMap[id]
  if exists == false {
    return fmt.Errorf("%s is not a node in ctx", id)
  }

  err := node.Unload(ctx)
  delete(ctx.nodeMap, id)
  return err
}

func (ctx *Context) Stop() {
  ctx.nodeMapLock.Lock()
  for id, node := range(ctx.nodeMap) {
    node.Unload(ctx)
    delete(ctx.nodeMap, id)
  }
  ctx.nodeMapLock.Unlock()
}

// Get a node from the context, or load from the database if not loaded
func (ctx *Context) getNode(id NodeID) (*Node, error) {
  target, exists := ctx.Node(id)

  if exists == false {
    var err error
    target, err = LoadNode(ctx, id)
    if err != nil {
      return nil, err
    }
  }
  return target, nil
}

// Route Messages to dest. Currently only local context routing is supported
func (ctx *Context) Send(node *Node, messages []SendMsg) error {
  for _, msg := range(messages) {
    ctx.Log.Logf("signal", "Sending %s -> %+v", msg.Dest, msg)
    if msg.Dest == ZeroID {
      panic("Can't send to null ID")
    }
    target, err := ctx.getNode(msg.Dest)
    if err == nil {
      select {
      case target.MsgChan <- RecvMsg{node.ID, msg.Signal}:
        ctx.Log.Logf("signal", "Sent %s -> %+v", target.ID, msg)
      default:
        buf := make([]byte, 4096)
        n := runtime.Stack(buf, false)
        stack_str := string(buf[:n])
        return fmt.Errorf("SIGNAL_OVERFLOW: %s - %s", msg.Dest, stack_str)
      }
    } else if errors.Is(err, NodeNotFoundError) {
      // TODO: Handle finding nodes in other contexts
      return err
    } else {
      return err
    }
  }
  return nil
}

// Create a new Context with the base library content added
func NewContext(db * badger.DB, log Logger) (*Context, error) {
  ctx := &Context{
    DB: db,
    Log: log,

    TypeMap: map[SerializedType]TypeInfo{},
    TypeTypes: map[reflect.Type]SerializedType{},

    Extensions: map[ExtType]ExtensionInfo{},
    ExtensionTypes: map[reflect.Type]ExtType{},

    Nodes: map[NodeType]NodeInfo{},
    NodeTypes: map[string]NodeType{},

    nodeMap: map[NodeID]*Node{},
  }

  var err error

  err = RegisterExtension[ListenerExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[LockableExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[EventExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[GQLExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  return ctx, nil
}
