package graphvent

import (
  "testing"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "db", "node", "policy"})
  node_type := NodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType})
  fatalErr(t, err)

  node := NewNode(ctx, RandID(), node_type)
  node.Extensions[GroupExtType] = NewGroupExt(nil)

  context := NewWriteContext(ctx)
  err = UpdateStates(context, node, NewACLInfo(node, []string{"test"}), func(context *StateContext) error {
    ser, err := node.Serialize()
    ctx.Log.Logf("test", "NODE_SER: %+v", ser)
    return err
  })
  fatalErr(t, err)

  ctx.Nodes = NodeMap{}
  _, err = LoadNode(ctx, node.ID)
  fatalErr(t, err)
}
