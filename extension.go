package graphvent

type Change string
type Changes []Change

// Extensions are data attached to nodes that process signals
type Extension interface {
  // Called to process incoming signals, returning changes and messages to send
  Process(*Context, *Node, NodeID, Signal) ([]SendMsg, Changes)

  // Called when the node is loaded into a context(creation or move), so extension data can be initialized
  Load(*Context, *Node) error

  // Called when the node is unloaded from a context(deletion or move), so extension data can be cleaned up
  Unload(*Context, *Node)
}
