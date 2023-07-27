package graphvent

import (
  "testing"
)

func TestLockableLink(t *testing.T) {
  ctx := logTestContext(t, []string{"lockable", "signal"})
  LockableType := NodeType("TEST_LOCKABLE")
  err := ctx.RegisterNodeType(LockableType, []ExtType{LockableExtType})
  fatalErr(t, err)
}

