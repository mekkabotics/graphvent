package graphvent

import (
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "runtime"
)

//Function to load an extension from bytes
type ExtensionLoadFunc func(*Context, []byte) (Extension, error)
// Information about a loaded extension
type ExtensionInfo struct {
  Load ExtensionLoadFunc
  Type ExtType
  Data interface{}
}

// Information about a loaded node type
type NodeInfo struct {
  Type NodeType
  Extensions []ExtType
}

// A Context is all the data needed to run a graphvent
type Context struct {
  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Log is an interface used to record events happening
  Log Logger
  // A mapping between type hashes and their corresponding extension definitions
  Extensions map[uint64]ExtensionInfo
  // A mapping between type hashes and their corresponding node definitions
  Types map[uint64]NodeInfo
  // All loaded Nodes
  Nodes map[NodeID]*Node
}

func (ctx *Context) ExtByType(ext_type ExtType) *ExtensionInfo {
  type_hash := ext_type.Hash()
  ext, ok := ctx.Extensions[type_hash]
  if ok == true {
    return &ext
  } else {
    return nil
  }
}

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

// Create a new Context with all the library content added
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
