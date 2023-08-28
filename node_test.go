package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ed25519"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"signal", "node", "db"})
  node_type := NodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType})
  fatalErr(t, err)

  node := NewNode(ctx, nil, node_type, 10, nil, NewGroupExt(nil))

  ctx.Nodes = map[NodeID]*Node{}
  _, err = ctx.GetNode(node.ID)
  fatalErr(t, err)
}

func TestNodeRead(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})
  node_type := NodeType("TEST")
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
    n2_id: Tree{
      ReadSignalType.String(): nil,
    },
  })

  n2_listener := NewListenerExt(10)
  n2 := NewNode(ctx, n2_key, node_type, 10, nil, NewGroupExt(nil), NewECDHExt(), n2_listener)

  n1 := NewNode(ctx, n1_key, node_type, 10, map[PolicyType]Policy{
    PerNodePolicyType: &n1_policy,
  }, NewGroupExt(nil), NewECDHExt())

  read_sig := NewReadSignal(map[ExtType][]string{
    GroupExtType: []string{"members"},
  })
  msgs := Messages{}
  msgs = msgs.Add(n2.ID, n2.Key, read_sig, n1.ID)
  err = ctx.Send(msgs)
  fatalErr(t, err)

  res, err := WaitForSignal(n2_listener.Chan, 10*time.Millisecond, ReadResultSignalType, func(sig *ReadResultSignal) bool {
    return true
  })
  fatalErr(t, err)
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}
