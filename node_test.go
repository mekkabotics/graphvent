package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ed25519"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"signal", "node"})
  node_type := NodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType})
  fatalErr(t, err)

  node := NewNode(ctx, nil, node_type, 10, nil, NewGroupExt(nil))

  ctx.Nodes = NodeMap{}
  _, err = ctx.GetNode(node.ID)
  fatalErr(t, err)
}

func TestNodeRead(t *testing.T) {
  ctx := logTestContext(t, []string{})
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

  n2_listener := NewListenerExt(10)
  n2 := NewNode(ctx, n2_key, node_type, 10, nil, NewGroupExt(nil), NewECDHExt(), n2_listener)

  n1 := NewNode(ctx, n1_key, node_type, 10, nil, NewGroupExt(nil), NewECDHExt())

  read_sig := NewReadSignal(map[ExtType][]string{
    GroupExtType: []string{"members"},
  })
  ctx.Send(n2.ID, []Message{{n1.ID, &read_sig}})

  res, err := WaitForSignal(ctx, n2_listener, 10*time.Millisecond, ReadResultSignalType, func(sig *ReadResultSignal) bool {
    return true
  })
  fatalErr(t, err)
  ctx.Log.Logf("test", "READ_RESULT: %+v", res)
}

func TestECDH(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "ecdh", "policy"})

  node_type := NodeType("TEST")
  err := ctx.RegisterNodeType(node_type, []ExtType{ECDHExtType})
  fatalErr(t, err)

  n1_listener := NewListenerExt(10)
  n1 := NewNode(ctx, nil, node_type, 10, nil, NewECDHExt(), n1_listener)
  n2 := NewNode(ctx, nil, node_type, 10, nil, NewECDHExt())
  n3_listener := NewListenerExt(10)
  n3 := NewNode(ctx, nil, node_type, 10, nil, NewECDHExt(), n3_listener)

  ctx.Log.Logf("test", "N1: %s", n1.ID)
  ctx.Log.Logf("test", "N2: %s", n2.ID)


  ecdh_req, n1_ec, err := NewECDHReqSignal(n1)
  ecdh_ext, err := GetExt[*ECDHExt](n1)
  fatalErr(t, err)
  ecdh_ext.ECDHStates[n2.ID] = ECDHState{
    ECKey: n1_ec,
    SharedSecret: nil,
  }
  fatalErr(t, err)
  ctx.Log.Logf("test", "N1_EC: %+v", n1_ec)
  err = ctx.Send(n1.ID, []Message{{n2.ID, ecdh_req}})
  fatalErr(t, err)

  _, err = WaitForSignal(ctx, n1_listener, 100*time.Millisecond, ECDHSignalType, func(sig *ECDHSignal) bool {
    return sig.Str == "resp"
  })
  fatalErr(t, err)
  time.Sleep(10*time.Millisecond)

  ecdh_sig, err := NewECDHProxySignal(n1.ID, n3.ID, &StopSignal, ecdh_ext.ECDHStates[n2.ID].SharedSecret)
  fatalErr(t, err)

  err = ctx.Send(n1.ID, []Message{{n2.ID, ecdh_sig}})
  fatalErr(t, err)
}
