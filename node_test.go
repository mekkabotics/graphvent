package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ed25519"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"signal", "node", "db", "db_data", "serialize"})
  node_type := NewNodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType})
  fatalErr(t, err)

  node, err := NewNode(ctx, nil, node_type, 10, nil, NewGroupExt(nil), NewLockableExt(nil))
  fatalErr(t, err)

  ctx.nodeMap = map[NodeID]*Node{}
  _, err = ctx.getNode(node.ID)
  fatalErr(t, err)
}

func TestNodeRead(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})
  node_type := NewNodeType("TEST")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType, ECDHExtType})
  fatalErr(t, err)

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
      uint64(ReadSignalType): nil,
    },
  })

  n2_listener := NewListenerExt(10)
  n2, err := NewNode(ctx, n2_key, node_type, 10, nil, NewGroupExt(nil), n2_listener)
  fatalErr(t, err)

  n1, err := NewNode(ctx, n1_key, node_type, 10, map[PolicyType]Policy{
    PerNodePolicyType: &n1_policy,
  }, NewGroupExt(nil))
  fatalErr(t, err)

  read_sig := NewReadSignal(map[ExtType][]string{
    GroupExtType: {"members"},
  })
  msgs := Messages{}
  msgs = msgs.Add(ctx, n2.ID, n2.Key, read_sig, n1.ID)
  err = ctx.Send(msgs)
  fatalErr(t, err)

  res, err := WaitForSignal(n2_listener.Chan, 10*time.Millisecond, func(sig *ReadResultSignal) bool {
    return true
  })
  fatalErr(t, err)
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}
