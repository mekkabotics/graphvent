package graphvent

import (
  "testing"
)

func TestNodeDB(t *testing.T) {
  ctx := logTestContext(t, []string{"db"})
  node_type := NodeType("test")
  err := ctx.RegisterNodeType(node_type, []ExtType{GroupExtType})
  fatalErr(t, err)

  node := NewNode(ctx, RandID(), node_type, nil, NewGroupExt(nil))

  ctx.Nodes = NodeMap{}
  _, err = ctx.GetNode(node.ID)
  fatalErr(t, err)
}
