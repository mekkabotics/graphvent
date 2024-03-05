package graphvent

type SendMsg struct {
  Dest   NodeID
  Signal Signal
}

type RecvMsg struct {
  Source NodeID
  Signal Signal
}
