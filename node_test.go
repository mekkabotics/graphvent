package graphvent

import (
  "testing"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "db", "node", "policy"})
  node_type := NodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{})
  fatalErr(t, err)
  node := NewNode(RandID(), node_type)
  node.Extensions[ACLExtType] = &ACLExt{
    Delegations: NodeMap{},
  }
  ctx.Nodes[node.ID] = &node

  context := NewWriteContext(ctx)
  err = UpdateStates(context, &node, NewACLInfo(&node, []string{"test"}), func(context *StateContext) error {
    ser, err := node.Serialize()
    ctx.Log.Logf("test", "NODE_SER: %s", ser)
    return err
  })
  fatalErr(t, err)

  delete(ctx.Nodes, node.ID)
  _, err = LoadNode(ctx, node.ID)
  fatalErr(t, err)
}
