package main

import (
  "testing"
  "time"
  "fmt"
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

  err = event_manager.AddEvent(root_event, new_event, nil)
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
  test_event := NewEvent("", "", []Resource{})
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

  err = r3.Unlock(test_event)
  if err == nil {
    t.Fatal("Unlocked r3 with event that didn't lock it")
  }

  err = r3.Unlock(root_event)
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

  err = r4.Unlock(root_event)
  if err != nil {
    t.Fatal("Failed to unlock r4")
  }
  (*graph_tester)(t).CheckForNil(r1_l)
  (*graph_tester)(t).CheckForNil(rel)
}

func TestAddToEventQueue(t * testing.T) {
  queue := NewEventQueue("q", "", []Resource{})
  event_1 := NewEvent("1", "", []Resource{})
  event_2 := NewEvent("2", "", []Resource{})

  err := queue.AddChild(event_1, nil)
  if err == nil {
    t.Fatal("suceeded in added nil info to queue")
  }

  err = queue.AddChild(event_1, &EventQueueInfo{priority: 0})
  if err != nil {
    t.Fatal("failed to add valid event + info to queue")
  }

  err = queue.AddChild(event_2, &EventQueueInfo{priority: 1})
  if err != nil {
    t.Fatal("failed to add valid event + info to queue")
  }
}

func TestStartBaseEvent(t * testing.T) {
  event_1 := NewEvent("1", "", []Resource{})
  r := event_1.DoneResource()
  manager := NewEventManager(event_1, []Resource{})

  e_l := event_1.UpdateChannel()
  r_l := r.UpdateChannel()
  (*graph_tester)(t).CheckForNone(e_l)
  (*graph_tester)(t).CheckForNone(r_l)

  if r.Owner() != event_1 {
    t.Fatal("r is not owned by event_1")
  }

  err := manager.Run()
  if err != nil {
    t.Fatal(err)
  }
  // Check that the update channels for the event and resource have updates
  (*graph_tester)(t).CheckForNil(e_l)
  (*graph_tester)(t).CheckForNil(r_l)

  if r.Owner() != nil {
    t.Fatal("r still owned after event completed")
  }
}

func TestAbortEventQueue(t * testing.T) {
  root_event := NewEventQueue("", "", []Resource{})
  r := root_event.DoneResource()
  manager := NewEventManager(root_event, []Resource{})

  r1 := NewResource("r1", "", []Resource{})
  err := manager.AddResource(r1)
  if err != nil {
    t.Fatal(err)
  }
  r1.Lock(root_event)
  e1 := NewEvent("1", "", []Resource{r1})
  e1_info := NewEventQueueInfo(1)
  // Add an event so that the queue doesn't auto complete
  err = manager.AddEvent(root_event, e1, e1_info)
  if err != nil {
    t.Fatal(err)
  }

  // Now that an event manager is constructed with a queue and 3 basic events
  // start the queue and check that all the events are executed
  go func() {
    time.Sleep(time.Second)
    root_event.Abort()
  }()

  err = manager.Run()
  if err == nil {
    t.Fatal("event manager completed without error")
  }

  if r.Owner() == nil {
    t.Fatal("root event was finished after starting")
  }
}

func TestStartEventQueue(t * testing.T) {
  root_event := NewEventQueue("", "", []Resource{})
  r := root_event.DoneResource()
  rel := root_event.UpdateChannel();
  res_1 := NewResource("test_resource", "", []Resource{})
  res_2 := NewResource("test_resource", "", []Resource{})
  manager := NewEventManager(root_event, []Resource{res_1, res_2})


  e1:= NewEvent("1", "", []Resource{res_1, res_2})
  e1_r := e1.DoneResource()
  e1_info := NewEventQueueInfo(1)
  err := manager.AddEvent(root_event, e1, e1_info)
  if err != nil {
    t.Fatal("Failed to add e1 to manager")
  }
  (*graph_tester)(t).CheckForNil(rel)

  e2 := NewEvent("1", "", []Resource{res_1})
  e2_r := e2.DoneResource()
  e2_info := NewEventQueueInfo(2)
  err = manager.AddEvent(root_event, e2, e2_info)
  if err != nil {
    t.Fatal("Failed to add e2 to manager")
  }
  (*graph_tester)(t).CheckForNil(rel)

  e3 := NewEvent("1", "", []Resource{res_2})
  e3_r := e3.DoneResource()
  e3_info := NewEventQueueInfo(3)
  err = manager.AddEvent(root_event, e3, e3_info)
  if err != nil {
    t.Fatal("Failed to add e3 to manager")
  }
  (*graph_tester)(t).CheckForNil(rel)

  e1_l := e1.UpdateChannel();
  e2_l := e2.UpdateChannel();
  e3_l := e3.UpdateChannel();

  // Abort the event after 5 seconds just in case
  go func() {
    time.Sleep(5 * time.Second)
    root_event.Abort()
  }()

  // Now that an event manager is constructed with a queue and 3 basic events
  // start the queue and check that all the events are executed
  err = manager.Run()
  if err != nil {
    t.Fatal(err)
  }

  if r.Owner() != nil {
    fmt.Printf("root_event.DoneResource(): %p", root_event.DoneResource())
    t.Fatal("root event was not finished after starting")
  }

  if e1_r.Owner() != nil {
    t.Fatal("e1 was not completed")
  }
  (*graph_tester)(t).CheckForNil(e1_l)

  if e2_r.Owner() != nil {
    t.Fatal("e2 was not completed")
  }
  (*graph_tester)(t).CheckForNil(e2_l)

  if e3_r.Owner() != nil {
    t.Fatal("e3 was not completed")
  }
  (*graph_tester)(t).CheckForNil(e3_l)
}
