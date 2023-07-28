package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ecdsa"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{})
  node_type := NodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType})
  fatalErr(t, err)

  node := NewNode(ctx, nil, node_type, 10, nil, NewGroupExt(nil))

  ctx.Nodes = NodeMap{}
  _, err = ctx.GetNode(node.ID)
  fatalErr(t, err)
}

func TestNodeRead(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "read", "signal", "policy", "node", "loop"})
  node_type := NodeType("TEST")
  err := ctx.RegisterNodeType(node_type, []ExtType{ACLExtType, GroupExtType})
  fatalErr(t, err)

  n1_key, err := ecdsa.GenerateKey(ctx.ECDSA, rand.Reader)
  fatalErr(t, err)
  n2_key, err := ecdsa.GenerateKey(ctx.ECDSA, rand.Reader)
  fatalErr(t, err)

  n1_id := KeyID(&n1_key.PublicKey)
  n2_id := KeyID(&n2_key.PublicKey)

  ctx.Log.Logf("test", "N1: %s", n1_id)
  ctx.Log.Logf("test", "N2: %s", n2_id)

  n2_policy := NewPerNodePolicy(map[NodeID]Actions{
    n1_id: Actions{MakeAction(ReadResultSignalType, "+")},
  })
  n2_listener := NewListenerExt(10)
  n2 := NewNode(ctx, n2_key, node_type, 10, nil, NewACLExt(n2_policy), NewGroupExt(nil), n2_listener)

  n1_policy := NewPerNodePolicy(map[NodeID]Actions{
    n2_id: Actions{MakeAction(ReadSignalType, "+")},
  })
  n1 := NewNode(ctx, n1_key, node_type, 10, nil, NewACLExt(n1_policy), NewGroupExt(nil))

  ctx.Send(n2.ID, n1.ID, NewReadSignal(map[ExtType][]string{
    GroupExtType: []string{"members"},
  }))

  res := (*GraphTester)(t).WaitForReadResult(ctx, n2_listener, 10*time.Millisecond, "No read_result")
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}
