package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ed25519"
  "slices"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "node", "db"})

  node_listener := NewListenerExt(10)
  node, err := NewNode(ctx, nil, "Node", 10, NewLockableExt(nil), node_listener)
  fatalErr(t, err)

  _, err = WaitForSignal(node_listener.Chan, 10*time.Millisecond, func(sig *StatusSignal) bool {
    return slices.Contains(sig.Fields, "state") && sig.Source == node.ID
  })

  err = ctx.Unload(node.ID)
  fatalErr(t, err)

  _, err = ctx.getNode(node.ID)
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
  n2, err := NewNode(ctx, n2_key, "Node", 10, n2_listener)
  fatalErr(t, err)

  n1, err := NewNode(ctx, n1_key, "Node", 10, NewListenerExt(10)) 
  fatalErr(t, err)

  read_sig := NewReadSignal([]string{"buffer"})
  msgs := []SendMsg{{n1.ID, read_sig}}
  err = ctx.Send(n2, msgs)
  fatalErr(t, err)

  res, err := WaitForSignal(n2_listener.Chan, 10*time.Millisecond, func(sig *ReadResultSignal) bool {
    return true
  })
  fatalErr(t, err)
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}
