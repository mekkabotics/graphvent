package graphvent

import (
	"crypto/ecdh"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/constraints"

	"github.com/google/uuid"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"

	badger "github.com/dgraph-io/badger/v3"
)

var (
  NodeNotFoundError = errors.New("Node not found in DB")
  ECDH = ecdh.X25519()
)

type SerializeFn func(ctx *Context, value reflect.Value) ([]byte, error)
type DeserializeFn func(ctx *Context, data []byte) (reflect.Value, []byte, error)

type FieldInfo struct {
  Index []int
  Tag FieldTag
  Type reflect.Type
}

type TypeInfo struct {
  Serialized SerializedType
  Reflect reflect.Type
  Type graphql.Type

  Fields map[FieldTag]FieldInfo
  PostDeserializeIndex int

  Serialize SerializeFn
  Deserialize DeserializeFn
}

type ExtensionInfo struct {
  ExtType
  Interface *graphql.Interface
  Fields map[string][]int
  Data interface{}
}

type FieldIndex struct {
  FieldTag
  Extension ExtType
  Field string
}

type NodeInfo struct {
  NodeType
  Type *graphql.Object
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
  TypeMap map[SerializedType]*TypeInfo
  TypeTypes map[reflect.Type]*TypeInfo

  // Map between database extension hashes and the registered info
  Extensions map[ExtType]*ExtensionInfo
  ExtensionTypes map[reflect.Type]*ExtensionInfo

  // Map between database type hashes and the registered info
  Nodes map[NodeType]*NodeInfo
  NodeTypes map[string]*NodeInfo

  // Routing map to all the nodes local to this context
  nodeMapLock sync.RWMutex
  nodeMap map[NodeID]*Node
}

func (ctx *Context) GQLType(t reflect.Type) (graphql.Type, error) {
  info, mapped := ctx.TypeTypes[t]
  if mapped {
    return info.Type, nil
  } else {
    switch t.Kind() {
    case reflect.Array:
      info, mapped := ctx.TypeTypes[t.Elem()]
      if mapped {
        return graphql.NewList(info.Type), nil
      }
    case reflect.Slice:
      info, mapped := ctx.TypeTypes[t.Elem()]
      if mapped {
        return graphql.NewList(info.Type), nil
      }
    case reflect.Map:
      info, exists := ctx.TypeTypes[t]
      if exists {
        return info.Type, nil
      } else {
        err := RegisterMap(ctx, t)
        if err != nil {
          return nil, err
        }
        map_type := ctx.TypeTypes[t].Type
        ctx.Log.Logf("gql", "Getting type for %s: %s", t, map_type)
        return map_type, nil
      }
    case reflect.Pointer:
      info, mapped := ctx.TypeTypes[t.Elem()]
      if mapped {
        return info.Type, nil
      }
    }
    return nil, fmt.Errorf("Can't convert %s to GQL type", t)
  }
}

func RegisterMap(ctx *Context, reflect_type reflect.Type) error {
  key_type, err := ctx.GQLType(reflect_type.Key())
  if err != nil {
    return err
  }

  val_type, err := ctx.GQLType(reflect_type.Elem())
  if err != nil {
    return err
  }

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
  gql_name = strings.ReplaceAll(gql_name, "[", "_")
  gql_name = strings.ReplaceAll(gql_name, "]", "_")
  ctx.Log.Logf("gql", "Registering %s with gql name %s", reflect_type, gql_name)

  gql_pair := graphql.NewObject(graphql.ObjectConfig{
    Name: gql_name,
    Fields: graphql.Fields{
      "Key": &graphql.Field{
        Type: key_type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          return nil, fmt.Errorf("NOT_IMPLEMENTED")
        },
      },
      "Value": &graphql.Field{
        Type: val_type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          return nil, fmt.Errorf("NOT_IMPLEMENTED")
        },
      },
    },
  })

  ctx.Log.Logf("gql", "Registering new map with pair type %+v", gql_pair)
  gql_map := graphql.NewList(gql_pair)

  serialized_type := SerializeType(reflect_type)
  ctx.TypeMap[serialized_type] = &TypeInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
    Type: gql_map,
  }
  ctx.TypeTypes[reflect_type] = ctx.TypeMap[serialized_type]
 
  return nil
}

func BuildSchema(ctx *Context, query, mutation *graphql.Object) (graphql.Schema, error) {
  types := []graphql.Type{}

  for _, info := range(ctx.TypeMap) {
    types = append(types, info.Type)
  }

  for _, info := range(ctx.Extensions) {
    types = append(types, info.Interface) 
  }

  for _, info := range(ctx.Nodes) {
    types = append(types, info.Type)
  }

  subscription := graphql.NewObject(graphql.ObjectConfig{
    Name: "Subscription",
    Fields: graphql.Fields{
      "Test": &graphql.Field{
        Type: graphql.String,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          return "TEST", nil
        },
      },
    },
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
  reflect_type := reflect.TypeFor[E]()
  ext_type := ExtType(SerializedTypeFor[E]())
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension %+v of type %+v, type already exists in context", reflect_type, ext_type)
  }

  gql_name := "interface_" + strings.ReplaceAll(reflect_type.String(), ".", "_")
  ctx.Log.Logf("gql", "Registering %s with gql name %s", reflect_type, gql_name)
  gql_interface := graphql.NewInterface(graphql.InterfaceConfig{
    Name: gql_name,
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
        return ctx.Context.NodeTypes["Base"].Type
      }

      return type_info.Type
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

      gql_type, err := ctx.GQLType(field.Type)
      if err != nil {
        return err
      }

      gql_interface.AddFieldConfig(gv_tag, &graphql.Field{
        Type: gql_type,
      })
    }
  }

  err := RegisterObject[E](ctx)
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered ExtType: %+v - %+v", reflect_type, ext_type)

  ctx.Extensions[ext_type] = &ExtensionInfo{
    ExtType: ext_type,
    Interface: gql_interface,
    Data: data,
    Fields: fields,
  }
  ctx.ExtensionTypes[reflect_type] = ctx.Extensions[ext_type]

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
  
  gql := graphql.NewObject(graphql.ObjectConfig{
    Name: name,
    Interfaces: []*graphql.Interface{},
    Fields: graphql.Fields{
      "ID": &graphql.Field{
        Type: graphql.String,
      },
      "Type": &graphql.Field{
        Type: graphql.String,
      },
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        return false
    },
  })

  ctx.Nodes[node_type] = &NodeInfo{
    NodeType: node_type,
    Type: gql,
    Extensions: extensions,
    Fields: mappings,
  }
  ctx.NodeTypes[name] = ctx.Nodes[node_type]

  return nil
}

func RegisterObject[T any](ctx *Context) error {
  reflect_type := reflect.TypeFor[T]()
  serialized_type := SerializedTypeFor[T]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
  ctx.Log.Logf("gql", "Registering %s with gql name %s", reflect_type, gql_name)
  gql := graphql.NewObject(graphql.ObjectConfig{
    Name: gql_name,
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      return reflect_type == reflect.TypeOf(p.Value)
    },
    Fields: graphql.Fields{},
  })

  field_infos := map[FieldTag]FieldInfo{}

  post_deserialize, post_deserialize_exists := reflect.PointerTo(reflect_type).MethodByName("PostDeserialize")
  post_deserialize_index := -1
  if post_deserialize_exists {
    post_deserialize_index = post_deserialize.Index
  }
  
  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      field_infos[GetFieldTag(gv_tag)] = FieldInfo{
        Type: field.Type,
        Tag: GetFieldTag(gv_tag),
        Index: field.Index,
      }

      gql_type, err := ctx.GQLType(field.Type)
      if err != nil {
        return err
      }
      gql.AddFieldConfig(gv_tag, &graphql.Field{
        Type: gql_type,
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

  ctx.TypeMap[serialized_type] = &TypeInfo{
    PostDeserializeIndex: post_deserialize_index,
    Serialized: serialized_type,
    Reflect: reflect_type,
    Fields: field_infos,
    Type: gql,
  }
  ctx.TypeTypes[reflect_type] = ctx.TypeMap[serialized_type]

  return nil
}

func identity(value interface{}) interface{} {
  return value
}

func stringify(value interface{}) interface{} {
  v, ok := value.(encoding.TextMarshaler)
  if ok {
    b, err := v.MarshalText()
    if err != nil {
      return nil
    }
    return string(b)
  }
  return nil
}

func unstringify[T any, E interface { *T; encoding.TextUnmarshaler }](value interface{}) interface{} {
  str, ok := value.(string)
  if ok == false {
    return nil
  }

  var tmp E
  err := tmp.UnmarshalText([]byte(str))
  if err != nil {
    return nil
  }

  return *tmp
}

func unstringifyAST[T any, E interface { *T; encoding.TextUnmarshaler}](value ast.Value)interface{} {
  str, ok := value.(*ast.StringValue)
  if ok == false {
    return nil
  }

  var tmp E
  err := tmp.UnmarshalText([]byte(str.Value))
  if err != nil {
    return nil
  }

  return *tmp
}

func coerce[T any](value interface{}) interface{} {
  t := reflect.TypeFor[T]()
  if reflect.TypeOf(value).ConvertibleTo(t) {
    return value.(T)
  } else {
    return nil
  }
}

func astString[T ~string](value ast.Value) interface{} {
  str, ok := value.(*ast.StringValue)
  if ok == false {
    return nil
  }

  return T(str.Value)
}

func astBool[T ~bool](value ast.Value) interface{} {
  switch value := value.(type) {
  case *ast.BooleanValue:
    if value.Value {
      return T(true)
    } else {
      return T(false)
    }
  case *ast.IntValue:
    i, err := strconv.Atoi(value.Value)
    if err != nil {
      return nil
    }
    return i != 0
  case *ast.StringValue:
    b, err := strconv.ParseBool(value.Value)
    if err != nil {
      return nil
    }
    return b
  default:
    return nil
  }
}

func astInt[T constraints.Integer](value ast.Value) interface{} {
  switch value := value.(type) {
  case *ast.BooleanValue:
    if value.Value {
      return T(1)
    } else {
      return T(0)
    }
  case *ast.StringValue:
    i, err := strconv.Atoi(value.Value)
    if err != nil {
      return nil
    } else {
      return T(i)
    }
  case *ast.IntValue:
    i, err := strconv.Atoi(value.Value)
    if err != nil {
      return nil
    } else {
      return T(i)
    }
  default:
    return nil
  }
}

func RegisterScalar[S any](ctx *Context, to_json func(interface{})interface{}, from_json func(interface{})interface{}, from_ast func(ast.Value)interface{}) error {
  reflect_type := reflect.TypeFor[S]()
  serialized_type := SerializedTypeFor[S]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
  ctx.Log.Logf("gql", "Registering %s with gql name %s", reflect_type, gql_name)
  gql := graphql.NewScalar(graphql.ScalarConfig{
    Name: gql_name,
    Serialize: to_json,
    ParseValue: from_json,
    ParseLiteral: from_ast,
  })

  ctx.TypeMap[serialized_type] = &TypeInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
    Type: gql,
  }
  ctx.TypeTypes[reflect_type] = ctx.TypeMap[serialized_type]

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

    TypeMap: map[SerializedType]*TypeInfo{},
    TypeTypes: map[reflect.Type]*TypeInfo{},

    Extensions: map[ExtType]*ExtensionInfo{},
    ExtensionTypes: map[reflect.Type]*ExtensionInfo{},

    Nodes: map[NodeType]*NodeInfo{},
    NodeTypes: map[string]*NodeInfo{},

    nodeMap: map[NodeID]*Node{},
  }

  var err error

  err = RegisterScalar[bool](ctx, identity, coerce[bool], astBool[bool])
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[int](ctx, identity, coerce[int], astInt[int]) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[uint32](ctx, identity, coerce[uint32], astInt[uint32])
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[uint8](ctx, identity, coerce[uint8], astInt[uint8])
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[time.Time](ctx, stringify, unstringify[time.Time], unstringifyAST[time.Time])
  if err != nil {
    return nil, err
  }
  
  err = RegisterScalar[string](ctx, identity, coerce[string], astString[string])
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[EventState](ctx, identity, coerce[EventState], astString[EventState])
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[ReqState](ctx, identity, coerce[ReqState], astInt[ReqState]) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[uuid.UUID](ctx, stringify, unstringify[uuid.UUID], unstringifyAST[uuid.UUID]) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[NodeID](ctx, stringify, unstringify[NodeID], unstringifyAST[NodeID]) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[WaitReason](ctx, identity, coerce[WaitReason], astString[WaitReason])
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[NodeType](ctx, identity, coerce[NodeType], astInt[NodeType])
  if err != nil {
    return nil, err
  }

  err = RegisterObject[Node](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterObject[WaitInfo](ctx)
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

  err = RegisterExtension[ListenerExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[GQLExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterNodeType(ctx, "Base", []ExtType{}, map[string]FieldIndex{})
  if err != nil {
    return nil, err
  }

  schema, err := BuildSchema(ctx, graphql.NewObject(graphql.ObjectConfig{
    Name: "Query",
    Fields: graphql.Fields{
      "Test": &graphql.Field{
        Type: graphql.String,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          return "TEST", nil
        },
      },
    },
  }), graphql.NewObject(graphql.ObjectConfig{
    Name: "Mutation",
    Fields: graphql.Fields{
      "Test": &graphql.Field{
        Type: graphql.String,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          return "TEST", nil
        },
      },
    },
  }))
  if err != nil {
    return nil, err
  }

  ctx.ExtensionTypes[reflect.TypeFor[GQLExt]()].Data = schema

  return ctx, nil
}
