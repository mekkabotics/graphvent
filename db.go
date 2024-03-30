package graphvent

import (
	"encoding/binary"
	"fmt"
	"reflect"
  "sync"

	badger "github.com/dgraph-io/badger/v3"
)

type Database interface {
  WriteNodeInit(*Context, *Node) error
  WriteNodeChanges(*Context, *Node, map[ExtType]Changes) error
  LoadNode(*Context, NodeID) (*Node, error)
}

const WRITE_BUFFER_SIZE = 1000000
type BadgerDB struct {
  *badger.DB
  sync.Mutex
  buffer [WRITE_BUFFER_SIZE]byte
}

func (db *BadgerDB) WriteNodeInit(ctx *Context, node *Node) error {
  if node == nil {
    return fmt.Errorf("Cannot serialize nil *Node")
  }

  return db.Update(func(tx *badger.Txn) error {
    db.Lock()
    defer db.Unlock()

    // Get the base key bytes
    id_ser, err := node.ID.MarshalBinary()
    if err != nil {
      return err
    }

    cur := 0

    // Write Node value
    written, err := Serialize(ctx, node, db.buffer[cur:])
    if err != nil {
      return err
    }

    err = tx.Set(id_ser, db.buffer[cur:cur+written])
    if err != nil {
      return err
    }

    cur += written
    
    // Write empty signal queue
    sigqueue_id := append(id_ser, []byte(" - SIGQUEUE")...)
    written, err = Serialize(ctx, node.SignalQueue, db.buffer[cur:])
    if err != nil {
      return err
    }

    err = tx.Set(sigqueue_id, db.buffer[cur:cur+written])
    if err != nil {
      return err
    }

    cur += written

    // Write node extension list
    ext_list := []ExtType{}
    for ext_type := range(node.Extensions) {
      ext_list = append(ext_list, ext_type)
    }
    written, err = Serialize(ctx, ext_list, db.buffer[cur:])
    if err != nil {
      return err
    }
    ext_list_id := append(id_ser, []byte(" - EXTLIST")...)
    err = tx.Set(ext_list_id, db.buffer[cur:cur+written])
    if err != nil {
      return err
    }
    cur += written

    // For each extension:
    for ext_type, ext := range(node.Extensions) {
      // Write each extension's current value
      ext_id := binary.BigEndian.AppendUint64(id_ser, uint64(ext_type))
      written, err := SerializeValue(ctx, reflect.ValueOf(ext).Elem(), db.buffer[cur:])
      if err != nil {
        return err
      }
      err = tx.Set(ext_id, db.buffer[cur:cur+written])
      if err != nil {
        return err
      }
      cur += written
    }
    return nil
  })
}

func (db *BadgerDB) WriteNodeChanges(ctx *Context, node *Node, changes map[ExtType]Changes) error {
  return db.Update(func(tx *badger.Txn) error {
    db.Lock()
    defer db.Unlock()

    // Get the base key bytes
    id_bytes := ([16]byte)(node.ID)

    cur := 0

    // Write the signal queue if it needs to be written
    if node.writeSignalQueue {
      node.writeSignalQueue = false

      sigqueue_id := append(id_bytes[:], []byte(" - SIGQUEUE")...)
      written, err := Serialize(ctx, node.SignalQueue, db.buffer[cur:])
      if err != nil {
        return fmt.Errorf("SignalQueue Serialize Error: %+v, %w", node.SignalQueue, err)
      }
      err = tx.Set(sigqueue_id, db.buffer[cur:cur+written])
      if err != nil {
        return fmt.Errorf("SignalQueue set error: %+v, %w", node.SignalQueue, err)
      }
      cur += written
    }

    // For each ext in changes
    for ext_type := range(changes) {
      // Write each ext
      ext, exists := node.Extensions[ext_type]
      if exists == false {
        return fmt.Errorf("%s is not an extension in %s", ext_type, node.ID)
      }
      ext_id := binary.BigEndian.AppendUint64(id_bytes[:], uint64(ext_type))
      written, err := SerializeValue(ctx, reflect.ValueOf(ext).Elem(), db.buffer[cur:])
      if err != nil {
        return fmt.Errorf("Extension serialize err: %s, %w", reflect.TypeOf(ext), err)
      }

      err = tx.Set(ext_id, db.buffer[cur:cur+written])
      if err != nil {
        return fmt.Errorf("Extension set err: %s, %w", reflect.TypeOf(ext), err)
      }
      cur += written
    }
    return nil
  })
}

func (db *BadgerDB) LoadNode(ctx *Context, id NodeID) (*Node, error) {
  var node *Node = nil

  err := db.View(func(tx *badger.Txn) error {
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
      ctx.Log.Logf("db", "DESERIALIZE_NODE(%d bytes): %+v", len(val), val)
      node, err = Deserialize[*Node](ctx, val)
      return err
    })

    if err != nil {
      return fmt.Errorf("Failed to deserialize Node %s - %w", id, err)
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

      ext_info, exists := ctx.Extensions[ext_type]
      if exists == false {
        return fmt.Errorf("Extension %s not in context", ext_type)
      }

      var ext Extension
      var ok bool
      err = ext_item.Value(func(val []byte) error {
        value, _, err := DeserializeValue(ctx, val, ext_info.Type)
        if err != nil {
          return err
        }

        ext, ok = value.Addr().Interface().(Extension)
        if ok == false {
          return fmt.Errorf("Parsed value %+v is not extension", value.Type())
        }

        return nil
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
  } else if node == nil {
    return nil, fmt.Errorf("Tried to return nil *Node from BadgerDB.LoadNode without error")
  }

  return node, nil
} 
