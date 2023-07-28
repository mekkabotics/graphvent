package graphvent

import (
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "errors"
  "runtime"
  "crypto/sha512"
  "encoding/binary"
)

type Type interface {
  String() string
  Prefix() string
}

func Hash(t Type) uint64 {
  hash := sha512.Sum512([]byte(fmt.Sprintf("%s%s", t.Prefix(), t.String())))
  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}


type NodeType string
func (node NodeType) Prefix() string { return "NODE: " }
func (node NodeType) String() string { return string(node) }

type ExtType string
func (ext ExtType) Prefix() string { return "EXTENSION: " }
func (ext ExtType) String() string { return string(ext) }

//Function to load an extension from bytes
type ExtensionLoadFunc func(*Context, []byte) (Extension, error)

const (
  ACLExtType = ExtType("ACL")
  ListenerExtType = ExtType("LISTENER")
  LockableExtType = ExtType("LOCKABLE")
  GQLExtType = ExtType("GQL")
  GroupExtType = ExtType("GROUP")
  ECDHExtType = ExtType("ECDH")

  GQLNodeType = NodeType("GQL")
)

// Information about a registered extension
type ExtensionInfo struct {
  Load ExtensionLoadFunc
  Type ExtType
  Data interface{}
}

// Information about a registered node type
type NodeInfo struct {
  Type NodeType
  Extensions []ExtType
}

// A Context stores all the data to run a graphvent process
type Context struct {
  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Logging interface
  Log Logger
  // Map between database extension hashes and the registered info
  Extensions map[uint64]ExtensionInfo
  // Map between database type hashes and the registered info
  Types map[uint64]NodeInfo
  // Routing map to all the nodes local to this context
  Nodes map[NodeID]*Node
}

// Register a NodeType to the context, with the list of extensions it requires
func (ctx *Context) RegisterNodeType(node_type NodeType, extensions []ExtType) error {
  type_hash := Hash(node_type)
  _, exists := ctx.Types[type_hash]
  if exists == true {
    return fmt.Errorf("Cannot register node type %s, type already exists in context", node_type)
  }

  ext_found := map[ExtType]bool{}
  for _, extension := range(extensions) {
    _, in_ctx := ctx.Extensions[Hash(extension)]
    if in_ctx == false {
      return fmt.Errorf("Cannot register node type %s, required extension %s not in context", node_type, extension)
    }

    _, duplicate := ext_found[extension]
    if duplicate == true {
      return fmt.Errorf("Duplicate extension %s found in extension list", extension)
    }

    ext_found[extension] = true
  }

  ctx.Types[type_hash] = NodeInfo{
    Type: node_type,
    Extensions: extensions,
  }
  return nil
}

// Add a node to a context, returns an error if the def is invalid or already exists in the context
func (ctx *Context) RegisterExtension(ext_type ExtType, load_fn ExtensionLoadFunc, data interface{}) error {
  if load_fn == nil {
    return fmt.Errorf("def has no load function")
  }

  type_hash := Hash(ext_type)
  _, exists := ctx.Extensions[type_hash]
  if exists == true {
    return fmt.Errorf("Cannot register extension of type %s, type already exists in context", ext_type)
  }

  ctx.Extensions[type_hash] = ExtensionInfo{
    Load: load_fn,
    Type: ext_type,
    Data: data,
  }
  return nil
}

var NodeNotFoundError = errors.New("Node not found in DB")

func (ctx *Context) GetNode(id NodeID) (*Node, error) {
  target, exists := ctx.Nodes[id]
  if exists == false {
    var err error
    target, err = LoadNode(ctx, id)
    if err != nil {
      return nil, err
    }
  }
  return target, nil
}

// Stop every running loop
func (ctx *Context) Stop() {
  for _, node := range(ctx.Nodes) {
    node.MsgChan <- Msg{ZeroID, StopSignal}
  }
}

// Route a Signal to dest. Currently only local context routing is supported
func (ctx *Context) Send(source NodeID, dest NodeID, signal Signal) error {
  target, err := ctx.GetNode(dest)
  if err == nil {
    select {
    case target.MsgChan <- Msg{source, signal}:
    default:
      buf := make([]byte, 4096)
      n := runtime.Stack(buf, false)
      stack_str := string(buf[:n])
      return fmt.Errorf("SIGNAL_OVERFLOW: %s - %s", dest, stack_str)
    }
    return nil
  } else if errors.Is(err, NodeNotFoundError) {
    // TODO: Handle finding nodes in other contexts
    return err
  }
  return err
}

// Create a new Context with the base library content added
func NewContext(db * badger.DB, log Logger) (*Context, error) {
  ctx := &Context{
    DB: db,
    Log: log,
    Extensions: map[uint64]ExtensionInfo{},
    Types: map[uint64]NodeInfo{},
    Nodes: map[NodeID]*Node{},
  }

  var err error
  err = ctx.RegisterExtension(ACLExtType, LoadACLExt, NewACLExtContext())
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(LockableExtType, LoadLockableExt, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(ListenerExtType, LoadListenerExt, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(ECDHExtType, LoadECDHExt, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(GroupExtType, LoadGroupExt, nil)
  if err != nil {
    return nil, err
  }

  gql_ctx := NewGQLExtContext()
  err = ctx.RegisterExtension(GQLExtType, LoadGQLExt, gql_ctx)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterNodeType(GQLNodeType, []ExtType{ACLExtType, GroupExtType, GQLExtType})
  if err != nil {
    return nil, err
  }

  err = gql_ctx.RegisterNodeType(GQLNodeType, GQLTypeGQLNode.Type)
  if err != nil {
    return nil, err
  }

  schema, err := BuildSchema(gql_ctx)
  if err != nil {
    return nil, err
  }

  gql_ctx.Schema = schema

  return ctx, nil
}
