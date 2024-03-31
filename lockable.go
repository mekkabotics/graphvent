package graphvent

import (
	"github.com/google/uuid"
)

type ReqState byte
const (
  Unlocked = ReqState(0)
  Unlocking = ReqState(1)
  Locked = ReqState(2)
  Locking = ReqState(3)
  AbortingLock = ReqState(4)
)

var ReqStateStrings = map[ReqState]string {
  Unlocked: "Unlocked",
  Unlocking: "Unlocking",
  Locked: "Locked",
  Locking: "Locking",
  AbortingLock: "AbortingLock",
}

func (state ReqState) String() string {
  str, mapped := ReqStateStrings[state]
  if mapped == false {
    return "UNKNOWN_REQSTATE"
  } else {
    return str
  }
}


type LockableExt struct{
  State ReqState `gv:"state"`
  ReqID *uuid.UUID `gv:"req_id"`
  Owner *NodeID `gv:"owner"`
  PendingOwner *NodeID `gv:"pending_owner"`
  Requirements map[NodeID]ReqState `gv:"requirements" node:"Lockable:"`

  Locked map[NodeID]any
  Unlocked map[NodeID]any

  Waiting WaitMap `gv:"waiting_locks" node:":Lockable"`
}

func (ext *LockableExt) PostDeserialize(ctx *Context) error {
  ext.Locked = map[NodeID]any{}
  ext.Unlocked = map[NodeID]any{}

  for id, state := range(ext.Requirements) {
    if state == Unlocked {
      ext.Unlocked[id] = nil
    } else if state == Locked {
      ext.Locked[id] = nil
    }
  }
  return nil
}

func NewLockableExt(requirements []NodeID) *LockableExt {
  var reqs map[NodeID]ReqState = nil
  var unlocked map[NodeID]any = map[NodeID]any{}

  if len(requirements) != 0 {
    reqs = map[NodeID]ReqState{}
    for _, req := range(requirements) {
      reqs[req] = Unlocked
      unlocked[req] = nil
    }
  }

  return &LockableExt{
    State: Unlocked,
    Owner: nil,
    PendingOwner: nil,
    Requirements: reqs,
    Waiting: WaitMap{},

    Locked: map[NodeID]any{},
    Unlocked: unlocked,
  }
}

func UnlockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  signal := NewUnlockSignal()
  messages := []Message{{node.ID, signal}}
  return signal.ID(), ctx.Send(node, messages)
}

func LockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  signal := NewLockSignal()
  messages := []Message{{node.ID, signal}}
  return signal.ID(), ctx.Send(node, messages)
}

func (ext *LockableExt) Load(ctx *Context, node *Node) error {
  return nil
}

func (ext *LockableExt) Unload(ctx *Context, node *Node) {
  return
}

// Handle link signal by adding/removing the requested NodeID
// returns an error if the node is not unlocked
func (ext *LockableExt) HandleLinkSignal(ctx *Context, node *Node, source NodeID, signal *LinkSignal) ([]Message, Changes) {
  var messages []Message = nil
  var changes Changes = nil

  switch ext.State {
  case Unlocked:
    switch signal.Action {
    case "add":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == true {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_requirement")})
      } else {
        if ext.Requirements == nil {
          ext.Requirements = map[NodeID]ReqState{}
        }
        ext.Requirements[signal.NodeID] = Unlocked
        changes = append(changes, "requirements")
        messages = append(messages, Message{source, NewSuccessSignal(signal.ID())})
      }
    case "remove":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == false {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_requirement")})
      } else {
        delete(ext.Requirements, signal.NodeID)
        changes = append(changes, "requirements")
        messages = append(messages, Message{source, NewSuccessSignal(signal.ID())})
      }
    default:
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "unknown_action")})
    }
  default:
    messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_unlocked: %s", ext.State)})
  }

  return messages, changes
}

// Handle an UnlockSignal by either transitioning to Unlocked state,
// sending unlock signals to requirements, or returning an error signal
func (ext *LockableExt) HandleUnlockSignal(ctx *Context, node *Node, source NodeID, signal *UnlockSignal) ([]Message, Changes) {
  var messages []Message = nil
  var changes Changes = nil

  switch ext.State {
  case Locked:
    if source != *ext.Owner {
      messages = append(messages, Message{source, NewErrorSignal(signal.Id, "not_owner")})
    } else {
      if len(ext.Requirements) == 0 {
        changes = append(changes, "state", "owner", "pending_owner")

        ext.Owner = nil

        ext.PendingOwner = nil

        ext.State = Unlocked

        messages = append(messages, Message{source, NewSuccessSignal(signal.Id)})
      } else {
        changes = append(changes, "state", "waiting", "requirements", "pending_owner")

        ext.PendingOwner = nil

        ext.ReqID = &signal.Id

        ext.State = Unlocking
        for id := range(ext.Requirements) {
          unlock_signal := NewUnlockSignal()

          ext.Waiting[unlock_signal.Id] = id
          ext.Requirements[id] = Unlocking

          messages = append(messages, Message{id, unlock_signal})
        }
      }
    }
  default:
    messages = append(messages, Message{source, NewErrorSignal(signal.Id, "not_locked")})
  }

  return messages, changes
}

// Handle a LockSignal by either transitioning to a locked state,
// sending lock signals to requirements, or returning an error signal
func (ext *LockableExt) HandleLockSignal(ctx *Context, node *Node, source NodeID, signal *LockSignal) ([]Message, Changes) {
  var messages []Message = nil
  var changes Changes = nil

  switch ext.State {
  case Unlocked:
    if len(ext.Requirements) == 0 {
      changes = append(changes, "state", "owner", "pending_owner")

      ext.Owner = &source

      ext.PendingOwner = &source

      ext.State = Locked
      messages = append(messages, Message{source, NewSuccessSignal(signal.Id)})
    } else {
      changes = append(changes, "state", "requirements", "waiting", "pending_owner")

      ext.PendingOwner = &source

      ext.ReqID = &signal.Id

      ext.State = Locking
      for id := range(ext.Requirements) {
        lock_signal := NewLockSignal()

        ext.Waiting[lock_signal.Id] = id
        ext.Requirements[id] = Locking

        messages = append(messages, Message{id, lock_signal})
      }
    }
  default:
    messages = append(messages, Message{source, NewErrorSignal(signal.Id, "not_unlocked: %s", ext.State)})
  }

  return messages, changes
}

// Handle an error signal by aborting the lock, or retrying the unlock
func (ext *LockableExt) HandleErrorSignal(ctx *Context, node *Node, source NodeID, signal *ErrorSignal) ([]Message, Changes) {
  var messages []Message = nil
  var changes Changes = nil

  id, waiting := ext.Waiting[signal.ReqID]
  if waiting == true {
    delete(ext.Waiting, signal.ReqID)
    changes = append(changes, "waiting")

    switch ext.State {
    case Locking:
      changes = append(changes, "state", "requirements")

      ext.Requirements[id] = Unlocked

      unlocked := 0
      for req_id, req_state := range(ext.Requirements) {
        // Unlock locked requirements, and count unlocked requirements
        switch req_state {
        case Locked:
          unlock_signal := NewUnlockSignal()

          ext.Waiting[unlock_signal.Id] = req_id
          ext.Requirements[req_id] = Unlocking

          messages = append(messages, Message{req_id, unlock_signal})
        case Unlocked:
          unlocked += 1
        }
      }

      if unlocked == len(ext.Requirements) {
        changes = append(changes, "owner", "state")
        ext.State = Unlocked
        ext.Owner = nil
      } else {
        changes = append(changes, "state")
        ext.State = AbortingLock
      }

    case Unlocking:
      unlock_signal := NewUnlockSignal()
      ext.Waiting[unlock_signal.Id] = id
      messages = append(messages, Message{id, unlock_signal})

    case AbortingLock:
      req_state := ext.Requirements[id]
      // Mark failed lock as Unlocked, or retry unlock
      switch req_state {
      case Locking:
        ext.Requirements[id] = Unlocked

        // Check if all requirements unlocked now
        unlocked := 0
        for _, req_state := range(ext.Requirements) {
          if req_state == Unlocked {
            unlocked += 1
          }
        }

        if unlocked == len(ext.Requirements) {
          changes = append(changes, "owner", "state")
          ext.State = Unlocked
          ext.Owner = nil
        }
      case Unlocking:
        // Handle error for unlocking requirement while unlocking by retrying unlock
        unlock_signal := NewUnlockSignal()
        ext.Waiting[unlock_signal.Id] = id
        messages = append(messages, Message{id, unlock_signal})
      }
    }
  }

  return messages, changes
}

// Handle a success signal by checking if all requirements have been locked/unlocked
func (ext *LockableExt) HandleSuccessSignal(ctx *Context, node *Node, source NodeID, signal *SuccessSignal) ([]Message, Changes) {
  var messages []Message = nil
  var changes Changes = nil

  id, waiting := ext.Waiting[signal.ReqID]
  if waiting == true {
    delete(ext.Waiting, signal.ReqID)
    changes = append(changes, "waiting")

    switch ext.State {
    case Locking:
      ext.Requirements[id] = Locked
      ext.Locked[id] = nil
      delete(ext.Unlocked, id)

      if len(ext.Locked) == len(ext.Requirements) {
        ctx.Log.Logf("lockable", "%s FULL_LOCK: %d", node.ID, len(ext.Locked))
        changes = append(changes, "state", "owner", "req_id")
        ext.State = Locked

        ext.Owner = ext.PendingOwner

        messages = append(messages, Message{*ext.Owner, NewSuccessSignal(*ext.ReqID)})
        ext.ReqID = nil
      } else {
        ctx.Log.Logf("lockable", "%s PARTIAL_LOCK: %d/%d", node.ID, len(ext.Locked), len(ext.Requirements))
      }
    case AbortingLock:
      req_state := ext.Requirements[id]
      switch req_state {
      case Locking:
        ext.Requirements[id] = Unlocking
        unlock_signal := NewUnlockSignal()
        ext.Waiting[unlock_signal.Id] = id
        messages = append(messages, Message{id, unlock_signal})
      case Unlocking:
        ext.Requirements[id] = Unlocked
        ext.Unlocked[id] = nil
        delete(ext.Locked, id)

        unlocked := 0
        for _, req_state := range(ext.Requirements) {
          switch req_state {
          case Unlocked:
            unlocked += 1
          }
        }

        if unlocked == len(ext.Requirements) {
          changes = append(changes, "state", "pending_owner", "req_id")

          messages = append(messages, Message{*ext.PendingOwner, NewErrorSignal(*ext.ReqID, "not_unlocked: %s", ext.State)})
          ext.State = Unlocked
          ext.ReqID = nil
          ext.PendingOwner = nil
        }
      }


    case Unlocking:
      ext.Requirements[id] = Unlocked
      ext.Unlocked[id] = Unlocked
      delete(ext.Locked, id)

      if len(ext.Unlocked) == len(ext.Requirements) {
        changes = append(changes, "state", "owner", "req_id")

        messages = append(messages, Message{*ext.Owner, NewSuccessSignal(*ext.ReqID)})
        ext.State = Unlocked
        ext.ReqID = nil
        ext.Owner = nil
      }
    }
  }

  return messages, changes
}

func (ext *LockableExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) ([]Message, Changes) {
  var messages []Message = nil
  var changes Changes = nil

  switch sig := signal.(type) {
  case *StatusSignal:
    // Forward StatusSignals up to the owner(unless that would be a cycle)
    if ext.Owner != nil {
      if *ext.Owner != node.ID {
        messages = append(messages, Message{*ext.Owner, signal})
      }
    }
  case *LinkSignal:
    messages, changes = ext.HandleLinkSignal(ctx, node, source, sig)
  case *LockSignal:
    messages, changes = ext.HandleLockSignal(ctx, node, source, sig)
  case *UnlockSignal:
    messages, changes = ext.HandleUnlockSignal(ctx, node, source, sig)
  case *ErrorSignal:
    messages, changes = ext.HandleErrorSignal(ctx, node, source, sig)
  case *SuccessSignal:
    messages, changes = ext.HandleSuccessSignal(ctx, node, source, sig)
  }

  return messages, changes
}

