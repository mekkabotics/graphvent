package graphvent

import (
  badger "github.com/dgraph-io/badger/v3"
  "github.com/graphql-go/graphql"
  "fmt"
  "sync"
  "errors"
  "runtime"
  "crypto/sha512"
  "crypto/elliptic"
  "crypto/ecdh"
  "encoding/binary"
)

// A Type can be Hashed by Hash
type TypeName interface {
  String() string
  Prefix() string
}

// Hashed a Type to a uint64
func Hash(t TypeName) uint64 {
  hash := sha512.Sum512([]byte(fmt.Sprintf("%s%s", t.Prefix(), t.String())))
  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}

// NodeType identifies the 'class' of a node
type NodeType string
func (node NodeType) Prefix() string { return "NODE: " }
func (node NodeType) String() string { return string(node) }

// ExtType identifies an extension on a node
type ExtType string
func (ext ExtType) Prefix() string { return "EXTENSION: " }
func (ext ExtType) String() string { return string(ext) }

//Function to load an extension from bytes
type ExtensionLoadFunc func(*Context, []byte) (Extension, error)

// ExtType and NodeType constants
const (
  ACLExtType = ExtType("ACL")
  ListenerExtType = ExtType("LISTENER")
  LockableExtType = ExtType("LOCKABLE")
  GQLExtType = ExtType("GQL")
  GroupExtType = ExtType("GROUP")
  ECDHExtType = ExtType("ECDH")

  GQLNodeType = NodeType("GQL")
)

var (
  NodeNotFoundError = errors.New("Node not found in DB")
)

type SignalLoadFunc func(*Context, []byte) (Signal, error)

type SignalInfo struct {
  Load SignalLoadFunc
  Type SignalType
}

// Information about a registered extension
type ExtensionInfo struct {
  // Function used to load extensions of this type from the database
  Load ExtensionLoadFunc
  Type ExtType
  // Extra context data shared between nodes of this class
  Data interface{}
}

// Information about a registered node type
type NodeInfo struct {
  Type NodeType
  // Required extensions to be a valid node of this class
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
  // Map between serialized signal hashes and the registered info
  Signals map[uint64]SignalInfo
  // Map between database type hashes and the registered info
  Types map[uint64]*NodeInfo
  // Curve used for signature operations
  ECDSA elliptic.Curve
  // Curve used for ecdh operations
  ECDH ecdh.Curve
  // Routing map to all the nodes local to this context
  NodesLock sync.RWMutex
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

  ctx.Types[type_hash] = &NodeInfo{
    Type: node_type,
    Extensions: extensions,
  }
  return nil
}

func (ctx *Context) RegisterSignal(signal_type SignalType, load_fn SignalLoadFunc) error {
  if load_fn == nil {
    return fmt.Errorf("def has no load function")
  }

  type_hash := Hash(signal_type)
  _, exists := ctx.Signals[type_hash]
  if exists == true {
    return fmt.Errorf("Cannot register signal of type %s, type already exists in context", signal_type)
  }

  ctx.Signals[type_hash] = SignalInfo{
    Load: load_fn,
    Type: signal_type,
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

func (ctx *Context) AddNode(id NodeID, node *Node) {
  ctx.NodesLock.Lock()
  ctx.Nodes[id] = node
  ctx.NodesLock.Unlock()
}

func (ctx *Context) Node(id NodeID) (*Node, bool) {
  ctx.NodesLock.RLock()
  node, exists := ctx.Nodes[id]
  ctx.NodesLock.RUnlock()
  return node, exists
}

// Get a node from the context, or load from the database if not loaded
func (ctx *Context) GetNode(id NodeID) (*Node, error) {
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
    Types: map[uint64]*NodeInfo{},
    Signals: map[uint64]SignalInfo{},
    Nodes: map[NodeID]*Node{},
    ECDH: ecdh.P256(),
    ECDSA: elliptic.P256(),
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

  err = ctx.RegisterSignal(StopSignalType, func(ctx *Context, data []byte) (Signal, error) {
    return StopSignal, nil
  })

  err = ctx.RegisterNodeType(GQLNodeType, []ExtType{ACLExtType, GroupExtType, GQLExtType})
  if err != nil {
    return nil, err
  }

  err = RegisterField(gql_ctx, graphql.String, "Listen", GQLExtType, "listen", func(listen string) (interface{}, error) {
    return listen, nil
  })
  if err != nil {
    return nil, err
  }

  err = gql_ctx.RegisterNodeType(GQLNodeType, "GQLServer", []string{"Node"}, []string{"Listen"})
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
