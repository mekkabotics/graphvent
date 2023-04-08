package main

import (
  "testing"
  "time"
)

type graph_tester testing.T
const listner_timeout = 100 * time.Millisecond

func (t * graph_tester) CheckForNil(listener chan error) {
  timeout := time.After(listner_timeout)
  select {
    case msg := <-listener:
      if msg == nil {
        return
      } else {
        t.Fatal("non-nil message on channel")
      }
    case <-timeout:
      t.Fatal("timeout waiting for message on channel")
  }
}

func (t * graph_tester) CheckForNonNil(listener chan error) {
  timeout := time.After(listner_timeout)
  select {
    case msg := <- listener:
      if msg != nil {
        return
      } else {
        t.Fatal("nil message on channel")
      }
    case <-timeout:
      t.Fatal("timeout waiting for message on channel")
  }
}

func (t * graph_tester) CheckForNone(listener chan error) {
  timeout := time.After(listner_timeout)
  select {
    case <- listener:
      t.Fatal("message on channel")
    case <-timeout:
  }
}

func TestNewResourceAdd(t *testing.T) {
  name := "Test Resource"
  description := "A resource for testing"
  children := []Resource{}

  root_event := NewEvent("", "", []Resource{})
  test_resource := NewResource(name, description, children)
  event_manager := NewEventManager(root_event, []Resource{test_resource})
  res := event_manager.FindResource(test_resource.ID())

  if res == nil {
    t.Fatal("Failed to find Resource in EventManager after adding")
  }

  if res.Name() != name || res.Description() != description {
    t.Fatal("Name/description of returned resource did not match added resource")
  }
}

func TestDoubleResourceAdd(t * testing.T) {
  root_event := NewEvent("", "", []Resource{})
  test_resource := NewResource("", "", []Resource{})
  event_manager := NewEventManager(root_event, []Resource{test_resource})
  err := event_manager.AddResource(test_resource)

  if err == nil {
    t.Fatal("Second AddResource returned nil")
  }
}

func TestMissingResourceAdd(t * testing.T) {
  root_event := NewEvent("", "", []Resource{})
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{r1})

  event_manager := NewEventManager(root_event, []Resource{})
  err := event_manager.AddResource(r2)
  if err == nil {
    t.Fatal("AddResource with missing child returned nil")
  }
}

func TestTieredResource(t * testing.T) {
  root_event := NewEvent("", "", []Resource{})
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{r1})

  event_manager := NewEventManager(root_event, []Resource{r1, r2})
  if event_manager == nil {
    t.Fatal("Failed to create event manager with tiered resources")
  }
}

func TestResourceUpdate(t * testing.T) {
  root_event := NewEvent("", "", []Resource{})
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{})
  r3 := NewResource("r3", "", []Resource{r1, r2})
  r4 := NewResource("r4", "", []Resource{r3})

  event_manager := NewEventManager(root_event, []Resource{r1, r2, r3, r4})
  if event_manager == nil {
    t.Fatal("Failed to add initial tiered resources for test")
  }

  r1_l := r1.UpdateChannel()
  r2_l := r2.UpdateChannel()
  r3_l := r3.UpdateChannel()
  r4_l := r4.UpdateChannel()

  // Calling Update() on the parent with no other parents should only notify node listeners
  r3.Update()
  (*graph_tester)(t).CheckForNone(r1_l)
  (*graph_tester)(t).CheckForNone(r2_l)
  (*graph_tester)(t).CheckForNil(r3_l)
  (*graph_tester)(t).CheckForNil(r4_l)

  // Calling Update() on a child should notify listeners of the parent and child, but not siblings
  r2.Update()
  (*graph_tester)(t).CheckForNone(r1_l)
  (*graph_tester)(t).CheckForNil(r2_l)
  (*graph_tester)(t).CheckForNil(r3_l)
  (*graph_tester)(t).CheckForNil(r4_l)

  // Calling Update() on a child should notify listeners of the parent and child, but not siblings
  r1.Update()
  (*graph_tester)(t).CheckForNil(r1_l)
  (*graph_tester)(t).CheckForNone(r2_l)
  (*graph_tester)(t).CheckForNil(r3_l)
  (*graph_tester)(t).CheckForNil(r4_l)
}

func TestAddEvent(t * testing.T) {
  root_event := NewEvent("", "", []Resource{})
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{r1})

  name := "Test Event"
  description := "A test event"
  resources := []Resource{r2}
  new_event := NewEvent(name, description, resources)

  event_manager := NewEventManager(root_event, []Resource{r1})
  err := event_manager.AddResource(r2)
  if err != nil {
    t.Fatal("Failed to add r2 to event_manager")
  }

  err = event_manager.AddEvent(root_event, new_event)
  if err != nil {
    t.Fatalf("Failed to add new_event to root_event: %s", err)
  }

  res := event_manager.FindEvent(new_event.ID())
  if res == nil {
    t.Fatalf("Failed to find new_event in event_manager: %s", err)
  }

  if res.Name() != name || res.Description() != description {
    t.Fatal("Event found in event_manager didn't match added")
  }

  res_required := res.RequiredResources()
  if len(res_required) < 1 {
    t.Fatal("Event found in event_manager didn't match added")
  } else if res_required[0].ID() != r2.ID() {
    t.Fatal("Event found in event_manager didn't match added")
  }
}

func TestLockResource(t * testing.T) {
  root_event := NewEvent("", "", []Resource{})
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{})
  r3 := NewResource("r3", "", []Resource{r1, r2})
  r4 := NewResource("r3", "", []Resource{r1, r2})

  event_manager := NewEventManager(root_event, []Resource{r1, r2, r3, r4})

  if event_manager == nil {
    t.Fatal("Failed to add initial tiered resources for test")
  }

  r1_l := r1.UpdateChannel()
  rel := root_event.UpdateChannel()

  err := r3.Lock(root_event)
  if err != nil {
    t.Fatal("Failed to lock r3")
  }
  (*graph_tester)(t).CheckForNil(r1_l)
  (*graph_tester)(t).CheckForNil(rel)

  err = r3.Lock(root_event)
  if err == nil {
    t.Fatal("Locked r3 after locking r3")
  }

  err = r4.Lock(root_event)
  if err == nil {
    t.Fatal("Locked r4 after locking r3")
  }

  err = r1.Lock(root_event)
  if err == nil {
    t.Fatal("Locked r1 after locking r3")
  }

  err = r3.Unlock()
  if err != nil {
    t.Fatal("Failed to unlock r3")
  }
  (*graph_tester)(t).CheckForNil(r1_l)
  (*graph_tester)(t).CheckForNil(rel)

  err = r4.Lock(root_event)
  if err != nil {
    t.Fatal("Failed to lock r4 after unlocking r3")
  }
  (*graph_tester)(t).CheckForNil(r1_l)
  (*graph_tester)(t).CheckForNil(rel)

  err = r4.Unlock()
  if err != nil {
    t.Fatal("Failed to unlock r4")
  }
  (*graph_tester)(t).CheckForNil(r1_l)
  (*graph_tester)(t).CheckForNil(rel)
}
