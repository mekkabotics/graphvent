package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ed25519"
  "slices"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"signal", "serialize", "node", "db", "listener"})

  node_listener := NewListenerExt(10)
  node, err := NewNode(ctx, nil, "Base", 10, nil, NewGroupExt(nil), NewLockableExt(nil), node_listener)
  fatalErr(t, err)

  _, err = WaitForSignal(node_listener.Chan, 10*time.Millisecond, func(sig *StatusSignal) bool {
    gql_changes, has_gql := sig.Changes[ExtTypeFor[GQLExt]()]
    if has_gql == true {
      return slices.Contains(gql_changes, "state") && sig.Source == node.ID
    }
    return false
  })

  msgs := Messages{}
  msgs = msgs.Add(ctx, node.ID, node, nil, NewStopSignal())
  err = ctx.Send(msgs)
  fatalErr(t, err)

  _, err = WaitForSignal(node_listener.Chan, 10*time.Millisecond, func(sig *StoppedSignal) bool {
    return sig.Source == node.ID
  })
  fatalErr(t, err)

  ctx.nodeMap = map[NodeID]*Node{}
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

  n1_policy := NewPerNodePolicy(map[NodeID]Tree{
    n2_id: {
      SerializedType(SignalTypeFor[ReadSignal]()): nil,
    },
  })

  n2_listener := NewListenerExt(10)
  n2, err := NewNode(ctx, n2_key, "Base", 10, nil, NewGroupExt(nil), n2_listener)
  fatalErr(t, err)

  n1, err := NewNode(ctx, n1_key, "Base", 10, []Policy{n1_policy}, NewGroupExt(nil))
  fatalErr(t, err)

  read_sig := NewReadSignal(map[ExtType][]string{
    ExtTypeFor[GroupExt](): {"members"},
  })
  msgs := Messages{}
  msgs = msgs.Add(ctx, n1.ID, n2, nil, read_sig)
  err = ctx.Send(msgs)
  fatalErr(t, err)

  res, err := WaitForSignal(n2_listener.Chan, 10*time.Millisecond, func(sig *ReadResultSignal) bool {
    return true
  })
  fatalErr(t, err)
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}
