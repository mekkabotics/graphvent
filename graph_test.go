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

  test_resource := NewResource(name, description, children)
  event_manager := NewEventManager()
  event_manager.AddResource(test_resource)
  res := event_manager.FindResource(test_resource.ID())

  if res == nil {
    t.Fatal("Failed to find Resource in EventManager after adding")
  }

  if res.Name() != name || res.Description() != description {
    t.Fatal("Name/description of returned resource did not match added resource")
  }
}

func TestDoubleResourceAdd(t * testing.T) {
  test_resource := NewResource("", "", []Resource{})
  event_manager := NewEventManager()
  err_1 := event_manager.AddResource(test_resource)
  err_2 := event_manager.AddResource(test_resource)

  if err_1 != nil {
    t.Fatalf("First AddResource returned error %s", err_1)
  }

  if err_2 == nil {
    t.Fatal("Second AddResource returned nil")
  }
}

func TestMissingResourceAdd(t * testing.T) {
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{r1})

  event_manager := NewEventManager()
  err := event_manager.AddResource(r2)
  if err == nil {
    t.Fatal("AddResource with missing child returned nil")
  }
}

func TestTieredResourceAdd(t * testing.T) {
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{r1})

  event_manager := NewEventManager()
  err_1 := event_manager.AddResource(r1)
  err_2 := event_manager.AddResource(r2)
  if err_1 != nil || err_2 != nil {
    t.Fatal("Failed adding tiered resource")
  }
}

func TestResourceUpdate(t * testing.T) {
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{})
  r3 := NewResource("r3", "", []Resource{r1, r2})
  r4 := NewResource("r4", "", []Resource{r3})

  event_manager := NewEventManager()
  err_1 := event_manager.AddResource(r1)
  err_2 := event_manager.AddResource(r2)
  err_3 := event_manager.AddResource(r3)
  err_4 := event_manager.AddResource(r4)

  if err_1 != nil || err_2 != nil || err_3 != nil || err_4 != nil {
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

func TestLockResource(t * testing.T) {
  r1 := NewResource("r1", "", []Resource{})
  r2 := NewResource("r2", "", []Resource{})
  r3 := NewResource("r3", "", []Resource{r1, r2})
  r4 := NewResource("r3", "", []Resource{r1, r2})

  event_manager := NewEventManager()
  err_1 := event_manager.AddResource(r1)
  err_2 := event_manager.AddResource(r2)
  err_3 := event_manager.AddResource(r3)
  err_4 := event_manager.AddResource(r4)

  if err_1 != nil || err_2 != nil || err_3 != nil || err_4 != nil {
    t.Fatal("Failed to add initial tiered resources for test")
  }

  r1_l := r1.UpdateChannel()

  // Lock r3(so also r1&r2)
  err := r3.Lock()
  if err != nil {
    t.Fatal("Failed to lock r3")
  }
  (*graph_tester)(t).CheckForNil(r1_l)

  err = r3.Lock()
  if err == nil {
    t.Fatal("Locked r3 after locking r3")
  }

  err = r4.Lock()
  if err == nil {
    t.Fatal("Locked r4 after locking r3")
  }

  err = r1.Lock()
  if err == nil {
    t.Fatal("Locked r1 after locking r3")
  }

  err = r3.Unlock()
  if err != nil {
    t.Fatal("Failed to unlock r3")
  }
  (*graph_tester)(t).CheckForNil(r1_l)

  err = r4.Lock()
  if err != nil {
    t.Fatal("Failed to lock r4 after unlocking r3")
  }
  (*graph_tester)(t).CheckForNil(r1_l)

  err = r4.Unlock()
  if err != nil {
    t.Fatal("Failed to unlock r4")
  }
  (*graph_tester)(t).CheckForNil(r1_l)
}
