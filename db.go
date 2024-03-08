package graphvent

import (
	badger "github.com/dgraph-io/badger/v3"
)

func WriteNodeInit(ctx *Context, node *Node) error {
  return ctx.DB.Update(func(tx *badger.Txn) error {
    _, err := node.ID.MarshalBinary()
    if err != nil {
      return err
    }

    // Write node private key
    // Write node type
    // Write Node buffer size
    // Write node extension list
    // For each extension:
      // Write each extension's current value
    return nil
  })
}

func WriteNodeChanges(ctx *Context, node *Node, changes map[ExtType]Changes) error {
  return ctx.DB.Update(func(tx *badger.Txn) error {
    // Write the signal queue if it needs to be written
    if node.writeSignalQueue {
      node.writeSignalQueue = false
    }

    // For each ext in changes
      // Write each change
    return nil
  })
}

func LoadNode(ctx *Context, id NodeID) (*Node, error) {
  err := ctx.DB.Update(func(tx *badger.Txn) error {
    return nil
  })

  if err != nil {
    return nil, err
  }

  return nil, nil
} 
