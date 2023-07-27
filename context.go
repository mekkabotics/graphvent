package graphvent

import (
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "runtime"
)

//Function to load an extension from bytes
type ExtensionLoadFunc func(*Context, []byte) (Extension, error)

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
  type_hash := node_type.Hash()
  _, exists := ctx.Types[type_hash]
  if exists == true {
    return fmt.Errorf("Cannot register node type %s, type already exists in context", node_type)
  }

  ext_found := map[ExtType]bool{}
  for _, extension := range(extensions) {
    _, in_ctx := ctx.Extensions[extension.Hash()]
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

  type_hash := ext_type.Hash()
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

// Route a Signal to dest. Currently only local context routing is supported
func (ctx *Context) Send(source NodeID, dest NodeID, signal Signal) error {
  target, exists := ctx.Nodes[dest]
  if exists == false {
    return fmt.Errorf("%s does not exist, cannot signal it", dest)
  }
  select {
  case target.MsgChan <- Msg{source, signal}:
  default:
    buf := make([]byte, 4096)
    n := runtime.Stack(buf, false)
    stack_str := string(buf[:n])
    return fmt.Errorf("SIGNAL_OVERFLOW: %s - %s", dest, stack_str)
  }
  return nil
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
