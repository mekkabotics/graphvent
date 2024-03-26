package graphvent

import (
	"crypto/ecdh"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
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
  Tag string
  NodeTag string
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
  Fields map[string]FieldInfo
  Data interface{}
}

type NodeInfo struct {
  NodeType
  Type *graphql.Object
  Interface *graphql.Interface
  Extensions []ExtType
  Fields map[string]ExtType
}

type InterfaceInfo struct {
  Serialized SerializedType
  Reflect reflect.Type
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

  Interfaces map[SerializedType]*InterfaceInfo
  InterfaceTypes map[reflect.Type]*InterfaceInfo

  // Map between database type hashes and the registered info
  Nodes map[NodeType]*NodeInfo
  NodeTypes map[string]*NodeInfo

  // Routing map to all the nodes local to this context
  nodeMapLock sync.RWMutex
  nodeMap map[NodeID]*Node
}

func (ctx *Context) GQLType(t reflect.Type, node_type string) (graphql.Type, error) {
  if t == reflect.TypeFor[NodeID]() {
    if node_type == "" {
      node_type = "Base"
    }
    node_info, mapped := ctx.NodeTypes[node_type]
    if mapped == false {
      return nil, fmt.Errorf("Cannot get GQL type for unregistered Node Type \"%s\"", node_type)
    } else {
      return node_info.Interface, nil
    }
  } else {
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
          err := RegisterMap(ctx, t, node_type)
          if err != nil {
            return nil, err
          }
          map_type := ctx.TypeTypes[t].Type
          return map_type, nil
        }
      case reflect.Pointer:
        return ctx.GQLType(t.Elem(), node_type)
      }
      return nil, fmt.Errorf("Can't convert %s to GQL type", t)
    }
  }
}

type Pair struct {
  Key any
  Val any
}

func RegisterMap(ctx *Context, reflect_type reflect.Type, node_type string) error {
  var node_types []string
  if node_type == "" {
    node_types = []string{"", ""}
  } else {
    node_types = strings.SplitN(node_type, ":", 2)

    if len(node_types) != 2 {
      return fmt.Errorf("Invalid node tag for map type %s: \"%s\"", reflect_type, node_type)
    }
  }

  key_type, err := ctx.GQLType(reflect_type.Key(), node_types[0])
  if err != nil {
    return err
  }

  key_resolve := ctx.GQLResolve(reflect_type.Key(), node_types[0])

  val_type, err := ctx.GQLType(reflect_type.Elem(), node_types[1])
  if err != nil {
    return err
  }

  val_resolve := ctx.GQLResolve(reflect_type.Elem(), node_types[1])

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
  gql_name = strings.ReplaceAll(gql_name, "[", "_")
  gql_name = strings.ReplaceAll(gql_name, "]", "_")

  gql_pair := graphql.NewObject(graphql.ObjectConfig{
    Name: gql_name,
    Fields: graphql.Fields{
      "Key": &graphql.Field{
        Type: key_type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          source, ok := p.Source.(Pair)
          if ok == false {
            return nil, fmt.Errorf("%+v is not Pair", source)
          }

          if key_resolve == nil {
            return source.Key, nil
          } else {
            return key_resolve(source.Key, p)
          }
        },
      },
      "Value": &graphql.Field{
        Type: val_type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          source, ok := p.Source.(Pair)
          if ok == false {
            return nil, fmt.Errorf("%+v is not Pair", source)
          }

          if val_resolve == nil {
            return source.Val, nil
          } else {
            return val_resolve(source.Val, p)
          }
        },
      },
    },
  })

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
  ctx.Log.Logf("gql", "Building Schema")

  for _, info := range(ctx.TypeMap) {
    if info.Type != nil {
      types = append(types, info.Type)
    }
  }

  for _, info := range(ctx.Nodes) {
    types = append(types, info.Type)
    types = append(types, info.Interface) 
  }

  subscription := graphql.NewObject(graphql.ObjectConfig{
    Name: "Subscription",
    Fields: graphql.Fields{},
  })

  for query_name, query := range(query.Fields()) {
    args := graphql.FieldConfigArgument{}
    for _, arg := range(query.Args) {
      args[arg.Name()] = &graphql.ArgumentConfig{
        Type: arg.Type,
        DefaultValue: arg.DefaultValue,
        Description: arg.Description(),
      }
    }
    subscription.AddFieldConfig(query_name, &graphql.Field{
      Type: query.Type,
      Args: args,
      Subscribe: func(p graphql.ResolveParams) (interface{}, error) {
        ctx, err := PrepResolve(p)
        if err != nil {
          return nil, err
        }

        c, err := ctx.Ext.AddSubscription(ctx.ID, ctx)
        if err != nil {
          return nil, err
        }

        c <- nil
        return c, nil
      },
      Resolve: query.Resolve,
    })
  }

  return graphql.NewSchema(graphql.SchemaConfig{
    Types: types,
    Query: query,
    Subscription: subscription,
    Mutation: mutation,
  })
}

func RegisterExtension[E any, T interface { *E; Extension}](ctx *Context, data interface{}) error {
  reflect_type := reflect.TypeFor[E]()
  ext_type := ExtType(SerializedTypeFor[E]())
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension %+v of type %+v, type already exists in context", reflect_type, ext_type)
  }

  fields := map[string]FieldInfo{}

  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      fields[gv_tag] = FieldInfo{
        Index: field.Index,
        Tag: gv_tag,
        NodeTag: field.Tag.Get("node"),
        Type: field.Type,
      }
    }
  }

  ctx.Extensions[ext_type] = &ExtensionInfo{
    ExtType: ext_type,
    Data: data,
    Fields: fields,
  }
  ctx.ExtensionTypes[reflect_type] = ctx.Extensions[ext_type]

  return nil
}

func RegisterNodeType(ctx *Context, name string, extensions []ExtType) error {
  node_type := NodeTypeFor(extensions)
  _, exists := ctx.Nodes[node_type]
  if exists == true {
    return fmt.Errorf("Cannot register node type %+v, type already exists in context", node_type)
  }

  fields := map[string]ExtType{}

  ext_found := map[ExtType]bool{}
  for _, extension := range(extensions) {
    ext_info, in_ctx := ctx.Extensions[extension]
    if in_ctx == false {
      return fmt.Errorf("Cannot register node type %+v, required extension %+v not in context", name, extension)
    }

    _, duplicate := ext_found[extension]
    if duplicate == true {
      return fmt.Errorf("Duplicate extension %+v found in extension list", extension)
    }

    ext_found[extension] = true

    for field_name := range(ext_info.Fields) {
      _, exists := fields[field_name]
      if exists {
        return fmt.Errorf("Cannot register NodeType %s with duplicate field name %s", name, field_name)
      }
      fields[field_name] = extension
    }
  }
 
  gql_interface := graphql.NewInterface(graphql.InterfaceConfig{
    Name: name,
    Fields: graphql.Fields{
      "ID": &graphql.Field{
        Type: ctx.TypeTypes[reflect.TypeFor[NodeID]()].Type,
      },
      "Type": &graphql.Field{
        Type: ctx.TypeTypes[reflect.TypeFor[NodeType]()].Type,
      },
    },
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx_val := p.Context.Value("resolve")
      ctx, ok := ctx_val.(*ResolveContext)
      if ok == false {
        return nil
      }

      val, ok := p.Value.(NodeResult)
      if ok == false {
        ctx.Context.Log.Logf("gql", "Interface ResolveType got bad Value %+v", p.Value)
        return nil
      }

      node_info, exists := ctx.Context.Nodes[val.NodeType]
      if exists == false {
        ctx.Context.Log.Logf("gql", "Interface ResolveType got bad NodeType", val.NodeType)
        return nil
      }

      for _, ext_type := range(extensions) {
        if slices.Contains(node_info.Extensions, ext_type) == false {
          ctx.Context.Log.Logf("gql", "Interface ResolveType for %s missing extensions %s: %+v", name, ext_type, val)
          return nil
        }
      }

      return node_info.Type
    },
  })

  gql := graphql.NewObject(graphql.ObjectConfig{
    Name: name + "Node",
    Interfaces: ctx.GQLInterfaces(node_type, extensions),
    Fields: graphql.Fields{
      "ID": &graphql.Field{
        Type: ctx.TypeTypes[reflect.TypeFor[NodeID]()].Type,
        Resolve: ResolveNodeID,
      },
      "Type": &graphql.Field{
        Type: ctx.TypeTypes[reflect.TypeFor[NodeType]()].Type,
        Resolve: ResolveNodeType,
      },
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      source, ok := p.Value.(NodeResult)
      if ok == false {
        return false
      }
      return source.NodeType == node_type
    },
  })

  ctx.Nodes[node_type] = &NodeInfo{
    NodeType: node_type,
    Interface: gql_interface,
    Type: gql,
    Extensions: extensions,
    Fields: fields,
  }
  ctx.NodeTypes[name] = ctx.Nodes[node_type]

  for _, ext_type := range(extensions) {
    ext_info, ext_found := ctx.Extensions[ext_type]
    if ext_found == false {
      return fmt.Errorf("Extension %s not found", ext_type)
    }

    for field_name, field_info := range(ext_info.Fields) {
      gql_type, err := ctx.GQLType(field_info.Type, field_info.NodeTag)
      if err != nil {
        return err 
      }

      gql_resolve := ctx.GQLResolve(field_info.Type, field_info.NodeTag)

      gql_interface.AddFieldConfig(field_name, &graphql.Field{
        Type: gql_type,
      })

      gql.AddFieldConfig(field_name, &graphql.Field{
        Type: gql_type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          node, ok := p.Source.(NodeResult)
          if ok == false {
            return nil, fmt.Errorf("Can't resolve Node field on non-Node %s", reflect.TypeOf(p.Source))
          }
          
          node_info, mapped := ctx.Nodes[node.NodeType]
          if mapped == false {
            return nil, fmt.Errorf("Can't resolve unknown NodeType %s", node.NodeType)
          }

          return gql_resolve(node.Data[node_info.Fields[field_name]][field_name], p)
        },
      })
    }

  }

  return nil
}

func (ctx *Context) GQLInterfaces(known_type NodeType, extensions []ExtType) graphql.InterfacesThunk {
  return func() []*graphql.Interface {
    interfaces := []*graphql.Interface{}
    for node_type, node_info := range(ctx.Nodes) {
      if node_type != known_type {
        has_ext := true
        for _, ext := range(node_info.Extensions) {
          if slices.Contains(extensions, ext) == false {
            has_ext = false
            break
          }
        }
        if has_ext == false {
          continue
        }
      }
      interfaces = append(interfaces, node_info.Interface)
    }
    return interfaces
  }
}

func RegisterSignal[S Signal](ctx *Context) error {
  return RegisterObject[S](ctx)
}

func RegisterObject[T any](ctx *Context) error {
  reflect_type := reflect.TypeFor[T]()
  serialized_type := SerializedTypeFor[T]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
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
      node_tag := field.Tag.Get("node")
      field_infos[GetFieldTag(gv_tag)] = FieldInfo{
        Type: field.Type,
        Index: field.Index,
        NodeTag: node_tag,
        Tag: gv_tag,
      }
      gql_type, err := ctx.GQLType(field.Type, node_tag)
      if err != nil {
        return err
      }

      gql_resolve := ctx.GQLResolve(field.Type, node_tag)
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

          if gql_resolve == nil {
            return value.Interface(), nil
          } else {
            return gql_resolve(value.Interface(), p)
          }
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

func RegisterObjectNoGQL[T any](ctx *Context) error {
  reflect_type := reflect.TypeFor[T]()
  serialized_type := SerializedTypeFor[T]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  field_infos := map[FieldTag]FieldInfo{}

  post_deserialize, post_deserialize_exists := reflect.PointerTo(reflect_type).MethodByName("PostDeserialize")
  post_deserialize_index := -1
  if post_deserialize_exists {
    post_deserialize_index = post_deserialize.Index
  }
  
  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      node_tag := field.Tag.Get("node")
      field_infos[GetFieldTag(gv_tag)] = FieldInfo{
        Type: field.Type,
        Index: field.Index,
        NodeTag: node_tag,
        Tag: gv_tag,
      }
    }
  }

  ctx.TypeMap[serialized_type] = &TypeInfo{
    PostDeserializeIndex: post_deserialize_index,
    Serialized: serialized_type,
    Reflect: reflect_type,
    Fields: field_infos,
    Type: nil,
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

  var tmp E = new(T)
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

func RegisterEnum[E comparable](ctx *Context, str_map map[E]string) error {
  reflect_type := reflect.TypeFor[E]()
  serialized_type := SerializedTypeFor[E]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  value_config := graphql.EnumValueConfigMap{}

  for value, value_name := range(str_map) {
    value_config[value_name] = &graphql.EnumValueConfig{
      Value: value,
    }
  }

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
  gql := graphql.NewEnum(graphql.EnumConfig{
    Name: gql_name,
    Values: value_config,
  })

  ctx.TypeMap[serialized_type] = &TypeInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
    Type: gql,
  }
  ctx.TypeTypes[reflect_type] = ctx.TypeMap[serialized_type]

  return nil
}

func RegisterScalar[S any](ctx *Context, to_json func(interface{})interface{}, from_json func(interface{})interface{}, from_ast func(ast.Value)interface{}) error {
  reflect_type := reflect.TypeFor[S]()
  serialized_type := SerializedTypeFor[S]()

  _, exists := ctx.TypeTypes[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  gql_name := strings.ReplaceAll(reflect_type.String(), ".", "_")
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

func (ctx *Context) Load(id NodeID) (*Node, error) {
  node, err := LoadNode(ctx, id)
  if err != nil {
    return nil, err
  }

  ctx.AddNode(id, node)
  started := make(chan error, 1)
  go runNode(ctx, node, started)

  err = <- started
  if err != nil {
    return nil, err
  }

  return node, nil
}

// Get a node from the context, or load from the database if not loaded
func (ctx *Context) getNode(id NodeID) (*Node, error) {
  target, exists := ctx.Node(id)

  if exists == false {
    var err error
    target, err = ctx.Load(id)
    if err != nil {
      return nil, err
    }
  }
  return target, nil
}

// Route Messages to dest. Currently only local context routing is supported
func (ctx *Context) Send(node *Node, messages []SendMsg) error {
  for _, msg := range(messages) {
    ctx.Log.Logf("signal", "Sending %s to %s", msg.Signal, msg.Dest)
    if msg.Dest == ZeroID {
      panic("Can't send to null ID")
    }
    target, err := ctx.getNode(msg.Dest)
    if err == nil {
      select {
      case target.MsgChan <- RecvMsg{node.ID, msg.Signal}:
        ctx.Log.Logf("signal_sent", "Sent %s to %s", msg.Signal, msg.Dest)
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

func resolveNodeID(val interface{}, p graphql.ResolveParams) (interface{}, error) {
  id, ok := p.Source.(NodeID)
  if ok == false {
    return nil, fmt.Errorf("%+v is not NodeID", p.Source)
  }

  return ResolveNode(id, p)
}

// TODO: Cache these functions so they're not duplicated when called with the same t
func (ctx *Context)GQLResolve(t reflect.Type, node_type string) (func(interface{},graphql.ResolveParams)(interface{},error)) {
  if t == reflect.TypeFor[NodeID]() {
    return resolveNodeID
  } else {
    switch t.Kind() {
    case reflect.Map:
      return func(v interface{}, p graphql.ResolveParams) (interface{}, error) {
        val := reflect.ValueOf(v)
        if val.Type() != t {
          return nil, fmt.Errorf("%s is not %s", reflect.TypeOf(val), t)
        } else {
          pairs := make([]Pair, val.Len())
          iter := val.MapRange()
          i := 0
          for iter.Next() {
            pairs[i] = Pair{
              Key: iter.Key().Interface(),
              Val: iter.Value().Interface(),
            }
            i += 1
          }
          return pairs, nil
        }
      }
    case reflect.Pointer:
      return ctx.GQLResolve(t.Elem(), node_type)
    default:
      return func(v interface{}, p graphql.ResolveParams) (interface{}, error) {
        return v, nil
      }
    }
  }
}

func RegisterInterface[T any](ctx *Context) error {
  serialized_type := SerializeType(reflect.TypeFor[T]())
  reflect_type := reflect.TypeFor[T]()

  _, exists := ctx.Interfaces[serialized_type]
  if exists == true {
    return fmt.Errorf("Interface %+v already exists in context", reflect_type)
  }

  ctx.Interfaces[serialized_type] = &InterfaceInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
  }
  ctx.InterfaceTypes[reflect_type] = ctx.Interfaces[serialized_type]

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

    Interfaces: map[SerializedType]*InterfaceInfo{},
    InterfaceTypes: map[reflect.Type]*InterfaceInfo{},

    Nodes: map[NodeType]*NodeInfo{},
    NodeTypes: map[string]*NodeInfo{},

    nodeMap: map[NodeID]*Node{},
  }

  var err error
 
  err = RegisterScalar[NodeID](ctx, stringify, unstringify[NodeID], unstringifyAST[NodeID]) 
  if err != nil {
    return nil, err
  }
  
  err = RegisterInterface[Extension](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterInterface[Signal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[NodeType](ctx, identity, coerce[NodeType], astInt[NodeType]) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[ExtType](ctx, identity, coerce[ExtType], astInt[ExtType])
  if err != nil {
    return nil, err
  }

  err = RegisterNodeType(ctx, "Base", []ExtType{})
  if err != nil {
    return nil, err
  }

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

  err = RegisterEnum[ReqState](ctx, ReqStateStrings) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[uuid.UUID](ctx, stringify, unstringify[uuid.UUID], unstringifyAST[uuid.UUID]) 
  if err != nil {
    return nil, err
  }

  err = RegisterScalar[Change](ctx, identity, coerce[Change], astString[Change])
  if err != nil {
    return nil, err
  }

  // TODO: Register as a GQL type with Signal as an interface
  err = RegisterObjectNoGQL[QueuedSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[TimeoutSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[StatusSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterObject[Node](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[LockableExt](ctx, nil)
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

  err = RegisterNodeType(ctx, "Lockable", []ExtType{ExtTypeFor[LockableExt]()})
  if err != nil {
    return nil, err
  }

  err = RegisterObject[LockableExt](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterObject[ListenerExt](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterObject[GQLExt](ctx)
  if err != nil {
    return nil, err
  }
  
  schema, err := BuildSchema(ctx, graphql.NewObject(graphql.ObjectConfig{
    Name: "Query",
    Fields: graphql.Fields{
      "Self": &graphql.Field{
        Type: ctx.NodeTypes["Base"].Interface,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          ctx, err := PrepResolve(p)
          if err != nil {
            return nil, err
          }

          return ResolveNode(ctx.Server.ID, p)
        },
      },
      "Node": &graphql.Field{
        Type: ctx.NodeTypes["Base"].Interface,
        Args: graphql.FieldConfigArgument{
          "id": &graphql.ArgumentConfig{
            Type: ctx.TypeTypes[reflect.TypeFor[NodeID]()].Type,
          },
        },
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          id, err := ExtractParam[NodeID](p, "id")
          if err != nil {
            return nil, err
          }

          return ResolveNode(id, p)
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
