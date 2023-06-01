package main

import (
  "errors"
  "log"
  "sync"
  "github.com/google/uuid"
)

// Generate a random graphql id
func randid() string{
  uuid_str := uuid.New().String()
  return uuid_str
}

// GraphNode is the interface common to both DAG nodes and Event tree nodes
type GraphNode interface {
  Name() string
  Description() string
  ID() string
  UpdateListeners(info string) error
  UpdateChannel() chan string
  Update(reason string) error
}

// BaseNode is the most basic implementation of the GraphNode interface
// It is used to implement functions common to Events and Resources
type BaseNode struct {
  name string
  description string
  id string
  listeners []chan string
  listeners_lock sync.Mutex
}

func (node * BaseNode) Name() string {
  return node.name
}

func (node * BaseNode) Description() string {
  return node.description
}

func (node * BaseNode) ID() string {
  return node.id
}

// Create a new listener channel for the node, add it to the nodes listener list, and return the new channel
const listener_buffer = 10
func (node * BaseNode) UpdateChannel() chan string{
  new_listener := make(chan string, listener_buffer)
  node.listeners_lock.Lock()
  node.listeners = append(node.listeners, new_listener)
  node.listeners_lock.Unlock()
  return new_listener
}

// Send the update to listener channels
func (node * BaseNode) UpdateListeners(info string) error {
  closed_listeners := []int{}
  listeners_closed := false

  // Send each listener nil to signal it to check for new content
  // if the first attempt to send it fails close the listener
  node.listeners_lock.Lock()
  for i, listener := range node.listeners {
    select {
      case listener <- info:
      default:
        close(listener)
        closed_listeners = append(closed_listeners, i)
        listeners_closed = true
    }
  }

  // If any listeners have been closed, loop over the listeners
  // Add listeners to the "remaining" list if i insn't in closed_listeners
  if listeners_closed == true {
    remaining_listeners := []chan string{}
    for i, listener := range node.listeners {
      listener_closed := false
      for _, index := range closed_listeners {
        if index == i {
          listener_closed = true
          break
        }
      }
      if listener_closed == false {
        remaining_listeners = append(remaining_listeners, listener)
      }
    }

    node.listeners = remaining_listeners
  }
  node.listeners_lock.Unlock()

  return nil
}

// Basic implementation must be overwritten to do anything useful
func (node * BaseNode) Update(reason string) error {
  log.Printf("UPDATE: BaseNode %s: %s", node.Name(), reason)
  return errors.New("Cannot Update a BaseNode")
}
