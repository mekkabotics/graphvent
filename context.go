package graphvent

import (
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
)

type ExtensionLoadFunc func(*Context, []byte) (Extension, error)
type ExtensionInfo struct {
  Load ExtensionLoadFunc
  Type ExtType
  Data interface{}
}

// A Context is all the data needed to run a graphvent
type Context struct {
  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Log is an interface used to record events happening
  Log Logger
  // A mapping between type hashes and their corresponding extension definitions
  Extensions map[uint64]ExtensionInfo
  // All loaded Nodes
  Nodes map[NodeID]*Node
}

func (ctx *Context) ExtByType(ext_type ExtType) ExtensionInfo {
  type_hash := ext_type.Hash()
  ext, _ := ctx.Extensions[type_hash]
  return ext
}

// Add a node to a context, returns an error if the def is invalid or already exists in the context
func (ctx *Context) RegisterExtension(ext_type ExtType, load_fn ExtensionLoadFunc) error {
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
  }
  return nil
}

// Create a new Context with all the library content added
func NewContext(db * badger.DB, log Logger) (*Context, error) {
  ctx := &Context{
    DB: db,
    Log: log,
    Extensions: map[uint64]ExtensionInfo{},
    Nodes: map[NodeID]*Node{},
  }

  err := ctx.RegisterExtension(ACLExtType, LoadACLExtension)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(ACLPolicyExtType, LoadACLPolicyExtension)
  if err != nil {
    return nil, err
  }

  return ctx, nil
}
