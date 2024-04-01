package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ed25519"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "node", "db"})

  node_listener := NewListenerExt(10)
  node, err := ctx.NewNode(nil, "Node", NewLockableExt(nil), node_listener)
  fatalErr(t, err)

  err = ctx.Stop()
  fatalErr(t, err)

  _, err = ctx.GetNode(node.ID)
  fatalErr(t, err)
}

func TestNodeRead(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})

  n1_pub, n1_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)
  n2_pub, n2_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)

  n1_id := KeyID(n1_pub)
  n2_id := KeyID(n2_pub)

  ctx.Log.Logf("test", "N1: %s", n1_id)
  ctx.Log.Logf("test", "N2: %s", n2_id)

  n2_listener := NewListenerExt(10)
  n2, err := ctx.NewNode(n2_key, "Node", n2_listener)
  fatalErr(t, err)

  n1, err := ctx.NewNode(n1_key, "Node", NewListenerExt(10)) 
  fatalErr(t, err)

  read_sig := NewReadSignal([]string{"buffer"})
  msgs := []Message{{n1.ID, read_sig}}
  err = ctx.Send(n2, msgs)
  fatalErr(t, err)

  res, err := WaitForSignal(n2_listener.Chan, 10*time.Millisecond, func(sig *ReadResultSignal) bool {
    return true
  })
  fatalErr(t, err)
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}
