package graphvent

import (
	"encoding/binary"
	"testing"
)

func sendBatch(start, end uint64, in chan<- Message) {
  for i := start; i <= end; i++ {
    var id NodeID
    binary.BigEndian.PutUint64(id[:], i)
    in <- Message{id, nil}
  }
}

func TestMessageQueue(t *testing.T) {
  in, out := NewMessageQueue(10)

  for i := uint64(0); i < 1000; i++ {
    go sendBatch(1000*i, (1000*(i+1))-1, in)
  }

  seen := map[NodeID]any{}
  for i := uint64(0); i < 1000*1000; i++ {
    read := <-out
    _, already_seen := seen[read.Node]
    if already_seen {
      t.Fatalf("Signal %d had duplicate NodeID %s", i, read.Node)
    } else {
      seen[read.Node] = nil
    }
  }

  t.Logf("Processed 1M signals through queue")
}
