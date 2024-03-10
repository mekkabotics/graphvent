package graphvent

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
)

func WriteNodeInit(ctx *Context, node *Node) error {
  if node == nil {
    return fmt.Errorf("Cannot serialize nil *Node")
  }

  return ctx.DB.Update(func(tx *badger.Txn) error {
    // Get the base key bytes
    id_ser, err := node.ID.MarshalBinary()
    if err != nil {
      return err
    }

    // Write Node value
    node_val, err := Serialize(ctx, node)
    if err != nil {
      return err
    }
    err = tx.Set(id_ser, node_val)
    if err != nil {
      return err
    }
    
    // Write empty signal queue
    sigqueue_id := append(id_ser, []byte(" - SIGQUEUE")...)
    sigqueue_val, err := Serialize(ctx, node.SignalQueue)
    if err != nil {
      return err
    }
    err = tx.Set(sigqueue_id, sigqueue_val)
    if err != nil {
      return err
    }

    // Write node extension list
    ext_list := []ExtType{}
    for ext_type := range(node.Extensions) {
      ext_list = append(ext_list, ext_type)
    }
    ext_list_val, err := Serialize(ctx, ext_list)
    if err != nil {
      return err
    }
    ext_list_id := append(id_ser, []byte(" - EXTLIST")...)
    err = tx.Set(ext_list_id, ext_list_val)
    if err != nil {
      return err
    }

    // For each extension:
    for ext_type, ext := range(node.Extensions) {
      // Write each extension's current value
      ext_id := binary.BigEndian.AppendUint64(id_ser, uint64(ext_type))
      ext_val, err := Serialize(ctx, ext)
      if err != nil {
        return err
      }
      err = tx.Set(ext_id, ext_val)
    }
    return nil
  })
}

func WriteNodeChanges(ctx *Context, node *Node, changes map[ExtType]Changes) error {
  return ctx.DB.Update(func(tx *badger.Txn) error {
    // Get the base key bytes
    id_ser, err := node.ID.MarshalBinary()
    if err != nil {
      return err
    }

    // Write the signal queue if it needs to be written
    if node.writeSignalQueue {
      node.writeSignalQueue = false

      sigqueue_id := append(id_ser, []byte(" - SIGQUEUE")...)
      sigqueue_val, err := Serialize(ctx, node.SignalQueue)
      if err != nil {
        return err
      }
      err = tx.Set(sigqueue_id, sigqueue_val)
      if err != nil {
        return err
      }
    }

    // For each ext in changes
    for ext_type := range(changes) {
      // Write each ext
      ext, exists := node.Extensions[ext_type]
      if exists == false {
        return fmt.Errorf("%s is not an extension in %s", ext_type, node.ID)
      }
      ext_id := binary.BigEndian.AppendUint64(id_ser, uint64(ext_type))
      ext_ser, err := Serialize(ctx, ext)
      if err != nil {
        return err
      }

      err = tx.Set(ext_id, ext_ser)
      if err != nil {
        return err
      }
    }
    return nil
  })
}

func LoadNode(ctx *Context, id NodeID) (*Node, error) {
  var node *Node = nil
  err := ctx.DB.View(func(tx *badger.Txn) error {
    // Get the base key bytes
    id_ser, err := id.MarshalBinary()
    if err != nil {
      return err
    }

    // Get the node value
    node_item, err := tx.Get(id_ser)
    if err != nil {
      return err
    }

    err = node_item.Value(func(val []byte) error {
      node, err = Deserialize[*Node](ctx, val)
      return err
    })

    if err != nil {
      return nil
    }

    // Get the signal queue
    sigqueue_id := append(id_ser, []byte(" - SIGQUEUE")...)
    sigqueue_item, err := tx.Get(sigqueue_id)
    if err != nil {
      return err
    }
    err = sigqueue_item.Value(func(val []byte) error {
      node.SignalQueue, err = Deserialize[[]QueuedSignal](ctx, val)
      return err
    })
    if err != nil {
      return err
    }

    // Get the extension list
    ext_list_id := append(id_ser, []byte(" - EXTLIST")...)
    ext_list_item, err := tx.Get(ext_list_id)
    if err != nil {
      return err
    }

    var ext_list []ExtType
    ext_list_item.Value(func(val []byte) error {
      ext_list, err = Deserialize[[]ExtType](ctx, val)
      return err
    })

    // Get the extensions
    for _, ext_type := range(ext_list) {
      ext_id := binary.BigEndian.AppendUint64(id_ser, uint64(ext_type))
      ext_item, err := tx.Get(ext_id)
      if err != nil {
        return err
      }

      var ext Extension
      err = ext_item.Value(func(val []byte) error {
        ext, err = Deserialize[Extension](ctx, val)
        return err
      })
      if err != nil {
        return err
      }
      node.Extensions[ext_type] = ext
    }

    return nil
  })

  if err != nil {
    return nil, err
  }

  return node, nil
} 
