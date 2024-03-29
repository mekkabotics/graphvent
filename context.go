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

type NodeFieldInfo struct {
  Extension ExtType
  Index []int
  Type graphql.Type
}

type StructFieldInfo struct {
  Index []int
  Type reflect.Type
}

type TypeInfo struct {
  Serialized SerializedType
  Reflect reflect.Type
  Type graphql.Type

  Fields map[FieldTag]StructFieldInfo
  PostDeserializeIndex int

  Serialize SerializeFn
  Deserialize DeserializeFn
}

type ExtensionFieldInfo struct {
  Index []int
  Type reflect.Type
  NodeTag string
}

type ExtensionInfo struct {
  ExtType
  Type reflect.Type
  Fields map[string]ExtensionFieldInfo
  Data interface{}
}

type NodeInfo struct {
  NodeType
  Type *graphql.Object
  RequiredExtensions []ExtType
  Fields map[string]NodeFieldInfo
  ReverseFields map[ExtType]map[Tag]string
}

type InterfaceInfo struct {
  Type *graphql.Interface
  Fields map[string]graphql.Type
}

// A Context stores all the data to run a graphvent process
type Context struct {

  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Logging interface
  Log Logger

  // Mapped types
  Types map[reflect.Type]*TypeInfo
  TypesReverse map[SerializedType]*TypeInfo

  // Map between database extension hashes and the registered info
  Extensions map[ExtType]ExtensionInfo

  // Map between GQL interface name and the registered info
  Interfaces map[string]InterfaceInfo

  // Map between database node type hashes and the registered info
  NodeTypes map[NodeType]NodeInfo

  // Routing map to all the nodes local to this context
  nodesLock sync.RWMutex
  nodes map[NodeID]*Node
}

func gqltype(ctx *Context, t reflect.Type, node_type string) graphql.Type {
  gql, err := ctx.GQLType(t, node_type)
  if err != nil {
    panic(err)
  } else {
    return gql
  }
}

func (ctx *Context) GQLType(t reflect.Type, node_type string) (graphql.Type, error) {
  if t == reflect.TypeFor[NodeID]() {
    if node_type == "" {
      node_type = "Base"
    }

    interface_info, mapped := ctx.Interfaces[node_type]
    if mapped == false {
      type_info, mapped := ctx.NodeTypes[NodeTypeFor(node_type)]
      if mapped {
        return type_info.Type, nil
      } else {
        return nil, fmt.Errorf("Cannot get GQL type for unregistered Node Type \"%s\"", node_type)
      }
    } else {
      return interface_info.Type, nil
    }

  } else {
    info, mapped := ctx.Types[t]
    if mapped {
      return info.Type, nil
    } else {
      switch t.Kind() {
      case reflect.Array:
        info, mapped := ctx.Types[t.Elem()]
        if mapped {
          return graphql.NewList(info.Type), nil
        }
      case reflect.Slice:
        info, mapped := ctx.Types[t.Elem()]
        if mapped {
          return graphql.NewList(info.Type), nil
        }
      case reflect.Map:
        info, exists := ctx.Types[t]
        if exists {
          return info.Type, nil
        } else {
          err := RegisterMap(ctx, t, node_type)
          if err != nil {
            return nil, err
          }
          map_type := ctx.Types[t].Type
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
  ctx.Types[reflect_type] = &TypeInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
    Type: gql_map,
  }
  ctx.TypesReverse[serialized_type] = ctx.Types[reflect_type]
 
  return nil
}

func BuildSchema(ctx *Context, query, mutation *graphql.Object) (graphql.Schema, error) {
  types := []graphql.Type{}
  ctx.Log.Logf("gql", "Building Schema")

  for _, info := range(ctx.Types) {
    if info.Type != nil {
      types = append(types, info.Type)
    }
  }

  for _, info := range(ctx.NodeTypes) {
    types = append(types, info.Type)
  }

  for _, info := range(ctx.Interfaces) {
    types = append(types, info.Type) 
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
  ext_type := ExtTypeFor[E, T]()
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension %+v of type %+v, type already exists in context", reflect_type, ext_type)
  }

  fields := map[string]ExtensionFieldInfo{}

  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    node_tag := field.Tag.Get("node")
    if tagged_gv {
      fields[gv_tag] = ExtensionFieldInfo{
        Index: field.Index,
        Type: field.Type,
        NodeTag: node_tag,
      }
    }
  }

  ctx.Extensions[ext_type] = ExtensionInfo{
    ExtType: ext_type,
    Type: reflect_type,
    Data: data,
    Fields: fields,
  }

  return nil
}

type FieldMapping struct {
  Extension ExtType
  Tag string
}

func RegisterNodeInterface(ctx *Context, name string, fields map[string]graphql.Type) error {
  _, exists := ctx.Interfaces[name]
  if exists {
    return fmt.Errorf("Cannot register Node Interface %s, already registered", name)
  }

  gql_fields := graphql.Fields{
    "ID": &graphql.Field{
      Type: ctx.Types[reflect.TypeFor[NodeID]()].Type,
    },
    "Type": &graphql.Field{
      Type: ctx.Types[reflect.TypeFor[NodeType]()].Type,
    },
  }

  for field_name, field_type := range(fields) {
    _, exists := gql_fields[field_name]
    if exists {
      return fmt.Errorf("Cannot register interface %s with duplicate field %s", name, field_name)
    }
    gql_fields[field_name] = &graphql.Field{
      Type: field_type,
    }
  }

  gql := graphql.NewInterface(graphql.InterfaceConfig{
    Name: name,
    Fields: gql_fields,
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

      node_info, exists := ctx.Context.NodeTypes[val.NodeType]
      if exists == false {
        ctx.Context.Log.Logf("gql", "Interface ResolveType got bad NodeType", val.NodeType)
        return nil
      }

      return node_info.Type
    },
  })

  ctx.Interfaces[name] = InterfaceInfo{
    Type: gql,
    Fields: fields,
  }

  return nil
}

func RegisterNodeType(ctx *Context, name string, mappings map[string]FieldMapping) error {
  node_type := NodeTypeFor(name)
  _, exists := ctx.NodeTypes[node_type]
  if exists {
    return fmt.Errorf("Cannot register node type %s, already registered", name)
  }

  fields := map[string]NodeFieldInfo{}
  reverse_fields := map[ExtType]map[Tag]string{}

  gql_fields := graphql.Fields{
    "ID": &graphql.Field{
      Type: ctx.Types[reflect.TypeFor[NodeID]()].Type,
      Resolve: ResolveNodeID,
    },
    "Type": &graphql.Field{
      Type: ctx.Types[reflect.TypeFor[NodeType]()].Type,
      Resolve: ResolveNodeType,
    },
  }

  ext_map := map[ExtType]bool{}
  for field_name, mapping := range(mappings) {
    _, duplicate := fields[field_name]
    if duplicate {
      return fmt.Errorf("Cannot register node type %s, contains duplicate field %s", name, field_name)
    }

    ext_info, exists := ctx.Extensions[mapping.Extension]
    if exists == false {
      return fmt.Errorf("Cannot register node type %s, unknown extension %s", name, mapping.Extension)
    }

    ext_map[mapping.Extension] = true

    ext_field, exists := ext_info.Fields[mapping.Tag]
    if exists == false {
      return fmt.Errorf("Cannot register node type %s, extension %s has no field %s", name, mapping.Extension, mapping.Tag)
    }

    gql_type, err := ctx.GQLType(ext_field.Type, ext_field.NodeTag)
    if err != nil {
      return fmt.Errorf("Cannot register node type %s, GQLType error: %w", name, err)
    }

    gql_resolve := ctx.GQLResolve(ext_field.Type, ext_field.NodeTag)

    fields[field_name] = NodeFieldInfo{
      Extension: mapping.Extension,
      Index: ext_field.Index,
      Type: gql_type,
    }

    gql_fields[field_name] = &graphql.Field{
      Type: gql_type,
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        node, ok := p.Source.(NodeResult)
        if ok == false {
          return nil, fmt.Errorf("Can't resolve Node field on non-Node %s", reflect.TypeOf(p.Source))
        }
        
        return gql_resolve(node.Data[field_name], p)
      },
    }
  }

  gql := graphql.NewObject(graphql.ObjectConfig{
    Name: name,
    Interfaces: ctx.GQLInterfaces(fields),
    Fields: gql_fields,
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      source, ok := p.Value.(NodeResult)
      if ok == false {
        return false
      }
      return source.NodeType == node_type
    },
  })

  extensions := []ExtType{}
  for ext_type := range(ext_map) {
    extensions = append(extensions, ext_type)
  }

  ctx.NodeTypes[node_type] = NodeInfo{
    NodeType: node_type,
    Type: gql,
    Fields: fields,
    ReverseFields: reverse_fields,
    RequiredExtensions: extensions,
  }

  return nil
}

// Returns a function which returns a list of interfaces from the context whose fields are a subset of fields
func (ctx *Context) GQLInterfaces(fields map[string]NodeFieldInfo) graphql.InterfacesThunk {
  return func() []*graphql.Interface {
    interfaces := []*graphql.Interface{}
    for _, interface_info := range(ctx.Interfaces) {
      match := true
      for field_name, field_type := range(interface_info.Fields) {
        field, exists := fields[field_name]
        if exists == false {
          match = false
          break
        } else if field.Type != field_type {
          match = false
          break
        }
      }
      if match {
        interfaces = append(interfaces, interface_info.Type)
      }
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

  _, exists := ctx.Types[reflect_type]
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

  field_infos := map[FieldTag]StructFieldInfo{}

  post_deserialize, post_deserialize_exists := reflect.PointerTo(reflect_type).MethodByName("PostDeserialize")
  post_deserialize_index := -1
  if post_deserialize_exists {
    post_deserialize_index = post_deserialize.Index
  }
  
  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      node_tag := field.Tag.Get("node")
      field_infos[GetFieldTag(gv_tag)] = StructFieldInfo{
        Type: field.Type,
        Index: field.Index,
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

  ctx.Types[reflect_type] = &TypeInfo{
    PostDeserializeIndex: post_deserialize_index,
    Serialized: serialized_type,
    Reflect: reflect_type,
    Fields: field_infos,
    Type: gql,
  }
  ctx.TypesReverse[serialized_type] = ctx.Types[reflect_type]

  return nil
}

func RegisterObjectNoGQL[T any](ctx *Context) error {
  reflect_type := reflect.TypeFor[T]()
  serialized_type := SerializedTypeFor[T]()

  _, exists := ctx.Types[reflect_type]
  if exists {
    return fmt.Errorf("%+v already registered in TypeMap", reflect_type)
  }

  field_infos := map[FieldTag]StructFieldInfo{}

  post_deserialize, post_deserialize_exists := reflect.PointerTo(reflect_type).MethodByName("PostDeserialize")
  post_deserialize_index := -1
  if post_deserialize_exists {
    post_deserialize_index = post_deserialize.Index
  }
  
  for _, field := range(reflect.VisibleFields(reflect_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      field_infos[GetFieldTag(gv_tag)] = StructFieldInfo{
        Type: field.Type,
        Index: field.Index,
      }
    }
  }

  ctx.Types[reflect_type] = &TypeInfo{
    PostDeserializeIndex: post_deserialize_index,
    Serialized: serialized_type,
    Reflect: reflect_type,
    Fields: field_infos,
    Type: nil,
  }
  ctx.TypesReverse[serialized_type] = ctx.Types[reflect_type]

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

  _, exists := ctx.Types[reflect_type]
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

  ctx.Types[reflect_type] = &TypeInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
    Type: gql,
  }
  ctx.TypesReverse[serialized_type] = ctx.Types[reflect_type]

  return nil
}

func RegisterScalar[S any](ctx *Context, to_json func(interface{})interface{}, from_json func(interface{})interface{}, from_ast func(ast.Value)interface{}, serialize SerializeFn, deserialize DeserializeFn) error {
  reflect_type := reflect.TypeFor[S]()
  serialized_type := SerializedTypeFor[S]()

  _, exists := ctx.Types[reflect_type]
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

  ctx.Types[reflect_type] = &TypeInfo{
    Serialized: serialized_type,
    Reflect: reflect_type,
    Type: gql,

    Serialize: serialize,
    Deserialize: deserialize,
  }
  ctx.TypesReverse[serialized_type] = ctx.Types[reflect_type]

  return nil
}

func (ctx *Context) AddNode(id NodeID, node *Node) {
  ctx.nodesLock.Lock()
  ctx.nodes[id] = node
  ctx.nodesLock.Unlock()
}

func (ctx *Context) Node(id NodeID) (*Node, bool) {
  ctx.nodesLock.RLock()
  node, exists := ctx.nodes[id]
  ctx.nodesLock.RUnlock()
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
  ctx.nodesLock.Lock()
  defer ctx.nodesLock.Unlock()
  node, exists := ctx.nodes[id]
  if exists == false {
    return fmt.Errorf("%s is not a node in ctx", id)
  }

  err := node.Unload(ctx)
  delete(ctx.nodes, id)
  return err
}

func (ctx *Context) Stop() {
  ctx.nodesLock.Lock()
  for id, node := range(ctx.nodes) {
    node.Unload(ctx)
    delete(ctx.nodes, id)
  }
  ctx.nodesLock.Unlock()
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
      return nil, fmt.Errorf("Failed to load node %s: %w", id, err)
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

// Create a new Context with the base library content added
func NewContext(db * badger.DB, log Logger) (*Context, error) {
  uuid.EnableRandPool()

  ctx := &Context{
    DB: db,
    Log: log,

    Types: map[reflect.Type]*TypeInfo{},
    TypesReverse: map[SerializedType]*TypeInfo{},
    Extensions: map[ExtType]ExtensionInfo{},
    Interfaces: map[string]InterfaceInfo{},
    NodeTypes: map[NodeType]NodeInfo{},

    nodes: map[NodeID]*Node{},
  }

  var err error
 
  err = RegisterScalar[NodeID](ctx, stringify, unstringify[NodeID], unstringifyAST[NodeID],
  func(ctx *Context, value reflect.Value) ([]byte, error) {
    return value.Bytes(), nil
  }, func(ctx *Context, data []byte) (reflect.Value, []byte, error) {
    if len(data) < 16 {
      return reflect.Value{}, nil, fmt.Errorf("Not enough bytes to decode NodeID(got %d, want 16)", len(data))
    }

    id := new(NodeID)
    err := id.UnmarshalBinary(data[0:16])
    if err != nil {
      return reflect.Value{}, nil, err
    }

    return reflect.ValueOf(id).Elem(), data[16:], nil
  }) 
  if err != nil {
    return nil, fmt.Errorf("Failed to register NodeID: %w", err)
  }

  err = RegisterScalar[uuid.UUID](ctx, stringify, unstringify[uuid.UUID], unstringifyAST[uuid.UUID],
  func(ctx *Context, value reflect.Value) ([]byte, error) {
    return value.Bytes(), nil
  }, func(ctx *Context, data []byte) (reflect.Value, []byte, error) {
    if len(data) < 16 {
      return reflect.Value{}, nil, fmt.Errorf("Not enough bytes to decode uuid.UUID(got %d, want 16)", len(data))
    }

    id := new(uuid.UUID)
    err := id.UnmarshalBinary(data[0:16])
    if err != nil {
      return reflect.Value{}, nil, err
    }

    return reflect.ValueOf(id).Elem(), data[16:], nil
  }) 
  if err != nil {
    return nil, fmt.Errorf("Failed to register uuid.UUID: %w", err)
  }
  
  err = RegisterScalar[NodeType](ctx, identity, coerce[NodeType], astInt[NodeType], nil, nil) 
  if err != nil {
    return nil, fmt.Errorf("Failed to register NodeType: %w", err)
  }

  err = RegisterScalar[ExtType](ctx, identity, coerce[ExtType], astInt[ExtType], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register ExtType: %w", err)
  }

  err = RegisterNodeInterface(ctx, "Base", map[string]graphql.Type{})
  if err != nil {
    return nil, fmt.Errorf("Failed to register NodeInterface Base: %w", err)
  }

  err = RegisterNodeType(ctx, "Node", map[string]FieldMapping{})
  if err != nil {
    return nil, fmt.Errorf("Failed to register NodeType Node: %w", err)
  }

  err = RegisterScalar[bool](ctx, identity, coerce[bool], astBool[bool], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register bool: %w", err)
  }

  err = RegisterScalar[int](ctx, identity, coerce[int], astInt[int], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register int: %w", err)
  }

  err = RegisterScalar[uint32](ctx, identity, coerce[uint32], astInt[uint32], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register uint32: %w", err)
  }

  err = RegisterScalar[uint8](ctx, identity, coerce[uint8], astInt[uint8], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register uint8: %w", err)
  }

  err = RegisterScalar[time.Time](ctx, stringify, unstringify[time.Time], unstringifyAST[time.Time], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register time.Time: %w", err)
  }
  
  err = RegisterScalar[string](ctx, identity, coerce[string], astString[string], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register string: %w", err)
  }

  err = RegisterEnum[ReqState](ctx, ReqStateStrings) 
  if err != nil {
    return nil, fmt.Errorf("Failed to register ReqState: %w", err)
  }

  err = RegisterScalar[Tag](ctx, identity, coerce[Tag], astString[Tag], nil, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register Tag: %w", err)
  }

  // TODO: Register as a GQL type with Signal as an interface
  err = RegisterObjectNoGQL[QueuedSignal](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register QueuedSignal: %w", err)
  }

  err = RegisterSignal[TimeoutSignal](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register TimeoutSignal: %w", err)
  }

  err = RegisterSignal[StatusSignal](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register StatusSignal: %w", err)
  }

  err = RegisterObject[Node](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register Node: %w", err)
  }

  err = RegisterExtension[LockableExt](ctx, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register LockableExt extension: %w", err)
  }

  err = RegisterExtension[ListenerExt](ctx, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register ListenerExt extension: %w", err)
  }

  err = RegisterExtension[GQLExt](ctx, nil)
  if err != nil {
    return nil, fmt.Errorf("Failed to register GQLExt extension: %w", err)
  }

  err = RegisterNodeInterface(ctx, "Lockable", map[string]graphql.Type{
    "LockableState": gqltype(ctx, reflect.TypeFor[ReqState](), ""),
    "Requirements": gqltype(ctx, reflect.TypeFor[map[NodeID]ReqState](), ":Lockable"),
  })
  if err != nil {
    return nil, fmt.Errorf("Failed to register NodeInterface Lockable: %w", err)
  }

  err = RegisterNodeType(ctx, "LockableNode", map[string]FieldMapping{
    "LockableState": {
      Extension: ExtTypeFor[LockableExt](),
      Tag: "state",
    },
    "Requirements": {
      Extension: ExtTypeFor[LockableExt](),
      Tag: "requirements",      
    },
  })
  if err != nil {
    return nil, fmt.Errorf("Failed to register NodeType LockableNode: %w", err)
  }

  err = RegisterObject[LockableExt](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register LockableExt object: %w", err)
  }

  err = RegisterObject[ListenerExt](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register ListenerExt object: %w", err)
  }

  err = RegisterObject[GQLExt](ctx)
  if err != nil {
    return nil, fmt.Errorf("Failed to register GQLExt object: %w", err)
  }
  
  schema, err := BuildSchema(ctx, graphql.NewObject(graphql.ObjectConfig{
    Name: "Query",
    Fields: graphql.Fields{
      "Self": &graphql.Field{
        Type: ctx.Interfaces["Base"].Type,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          ctx, err := PrepResolve(p)
          if err != nil {
            return nil, err
          }

          return ResolveNode(ctx.Server.ID, p)
        },
      },
      "Node": &graphql.Field{
        Type: ctx.Interfaces["Base"].Type,
        Args: graphql.FieldConfigArgument{
          "id": &graphql.ArgumentConfig{
            Type: ctx.Types[reflect.TypeFor[NodeID]()].Type,
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
    return nil, fmt.Errorf("Failed to build schema: %w", err)
  }

  ext_info := ctx.Extensions[ExtTypeFor[GQLExt]()]
  ext_info.Data = schema
  ctx.Extensions[ExtTypeFor[GQLExt]()] = ext_info

  return ctx, nil
}
