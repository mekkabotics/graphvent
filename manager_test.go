package graphvent

import (
  "testing"
  "time"
  "fmt"
  "os"
  "runtime/pprof"
)

type GraphTester testing.T
const listner_timeout = 50 * time.Millisecond

func (t * GraphTester) WaitForValue(listener chan GraphSignal, signal_type string, source GraphNode, timeout time.Duration, str string) GraphSignal {
  timeout_channel := time.After(timeout)
  for true {
    select {
    case signal := <- listener:
      if signal.Type() == signal_type {
        fmt.Printf("SIGNAL_TYPE_FOUND: %s - %s %+v\n", signal.Type(), signal.Source(), listener)
        if signal.Source() == source.ID() {
          return signal
        }
      }
    case <-timeout_channel:
      pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
      t.Fatal(str)
      return nil
    }
  }
  return nil
}

func (t * GraphTester) CheckForValue(listener chan GraphSignal, str string) GraphSignal {
  timeout := time.After(listner_timeout)
  select {
    case signal := <- listener:
      return signal
    case <-timeout:
      pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
      t.Fatal(str)
      return nil
  }
}

func (t * GraphTester) CheckForNone(listener chan GraphSignal, str string) {
  timeout := time.After(listner_timeout)
  select {
  case sig := <- listener:
    pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
    t.Fatal(fmt.Sprintf("%s : %+v", str, sig))
  case <-timeout:
  }
}

func TestNewEventWithResource(t *testing.T) {
  name := "Test Resource"
  description := "A resource for testing"
  children := []Resource{}

  test_resource, _ := NewResource(name, description, children)
  root_event, err := NewEvent("root_event", "", []Resource{test_resource})
  if err != nil {
    t.Fatal(err)
  }

  res := FindRequiredResource(root_event, test_resource.ID())
  if res == nil {
    t.Fatal("Failed to find Resource in EventManager after adding")
  }

  if res.Name() != name || res.Description() != description {
    t.Fatal("Name/description of returned resource did not match added resource")
  }
}

func TestDoubleResourceAdd(t * testing.T) {
  test_resource, _ := NewResource("", "", []Resource{})
  _, err := NewEvent("", "", []Resource{test_resource, test_resource})

  if err == nil {
    t.Fatal("NewEvent didn't return an error")
  }
}

func TestTieredResource(t * testing.T) {
  r1, _ := NewResource("r1", "", []Resource{})
  r2, err := NewResource("r2", "", []Resource{r1})
  if err != nil {
    t.Fatal(err)
  }
  _, err = NewEvent("", "", []Resource{r2})

  if err != nil {
    t.Fatal("Failed to create event with tiered resources")
  }
}

func TestResourceUpdate(t * testing.T) {
  r1, err := NewResource("r1", "", []Resource{})
  if err != nil {
    t.Fatal(err)
  }
  r2, err := NewResource("r2", "", []Resource{})
  if err != nil {
    t.Fatal(err)
  }
  r3, err := NewResource("r3", "", []Resource{r1, r2})
  if err != nil {
    t.Fatal(err)
  }
  r4, err := NewResource("r4", "", []Resource{r3})
  if err != nil {
    t.Fatal(err)
  }

  _, err = NewEvent("", "", []Resource{r3, r4})
  if err != nil {
    t.Fatal("Failed to add initial tiered resources for test")
  }

  r1_l := r1.UpdateChannel()
  r2_l := r2.UpdateChannel()
  r3_l := r3.UpdateChannel()
  r4_l := r4.UpdateChannel()

  // Calling Update() on the parent with no other parents should only notify node listeners
  SendUpdate(r3, NewSignal(nil, "test"))
  (*GraphTester)(t).CheckForNone(r1_l, "Update on r1 after updating r3")
  (*GraphTester)(t).CheckForNone(r2_l, "Update on r2 after updating r3")
  (*GraphTester)(t).CheckForValue(r3_l, "No update on r3 after updating r3")
  (*GraphTester)(t).CheckForValue(r4_l, "No update on r4 after updating r3")

  // Calling Update() on a child should notify listeners of the parent and child, but not siblings
  SendUpdate(r2, NewSignal(nil, "test"))
  (*GraphTester)(t).CheckForNone(r1_l, "Update on r1 after updating r2")
  (*GraphTester)(t).CheckForValue(r2_l, "No update on r2 after updating r2")
  (*GraphTester)(t).CheckForValue(r3_l, "No update on r3 after updating r2")
  (*GraphTester)(t).CheckForValue(r4_l, "No update on r4 after updating r2")

  // Calling Update() on a child should notify listeners of the parent and child, but not siblings
  SendUpdate(r1, NewSignal(nil, "test"))
  (*GraphTester)(t).CheckForValue(r1_l, "No update on r1 after updating r1")
  (*GraphTester)(t).CheckForNone(r2_l, "Update on r2 after updating r1")
  (*GraphTester)(t).CheckForValue(r3_l, "No update on r3 after updating r1")
  (*GraphTester)(t).CheckForValue(r4_l, "No update on r4 after updating r1")
}

func TestAddEvent(t * testing.T) {
  r1, _ := NewResource("r1", "", []Resource{})
  r2, _ := NewResource("r2", "", []Resource{r1})
  root_event, _ := NewEvent("", "", []Resource{r2})

  name := "Test Event"
  description := "A test event"
  resources := []Resource{r2}
  new_event, _ := NewEvent(name, description, resources)

  err := LinkEvent(root_event, new_event, nil)
  if err != nil {
    t.Fatalf("Failed to add new_event to root_event: %s", err)
  }

  res := FindChild(root_event, new_event.ID())
  if res == nil {
    t.Fatalf("Failed to find new_event in event_manager: %s", err)
  }

  if res.Name() != name || res.Description() != description {
    t.Fatal("Event found in event_manager didn't match added")
  }

  res_required := res.Resources()
  if len(res_required) < 1 {
    t.Fatal("Event found in event_manager didn't match added")
  } else if res_required[0].ID() != r2.ID() {
    t.Fatal("Event found in event_manager didn't match added")
  }
}

func TestLockResource(t * testing.T) {
  r1, err := NewResource("r1", "", []Resource{})
  if err != nil {
    t.Fatal(err)
  }
  r2, err := NewResource("r2", "", []Resource{})
  if err != nil {
    t.Fatal(err)
  }
  r3, err := NewResource("r3", "", []Resource{r1, r2})
  if err != nil {
    t.Fatal(err)
  }
  r4, err := NewResource("r4", "", []Resource{r1, r2})
  if err != nil {
    t.Fatal(err)
  }
  root_event, err := NewEvent("", "", []Resource{})
  if err != nil {
    t.Fatal(err)
  }
  test_event, err := NewEvent("", "", []Resource{})
  if err != nil {
    t.Fatal(err)
  }

  r1_l := r1.UpdateChannel()
  rel := root_event.UpdateChannel()

  err = LockResource(r3, root_event)
  if err != nil {
    t.Fatal("Failed to lock r3")
  }
  SendUpdate(r3, NewDownSignal(r3, "locked"))

  (*GraphTester)(t).WaitForValue(r1_l, "locked", r3, time.Second, "Wasn't notified of r1 lock on r1 after r3 lock")
  (*GraphTester)(t).WaitForValue(rel, "locked", r3, time.Second, "Wasn't notified of r1 lock on rel after r3 lock")

  err = LockResource(r3, root_event)
  if err == nil {
    t.Fatal("Locked r3 after locking r3")
  }

  err = LockResource(r4, root_event)
  if err == nil {
    t.Fatal("Locked r4 after locking r3")
  }

  err = LockResource(r1, root_event)
  if err == nil {
    t.Fatal("Locked r1 after locking r3")
  }

  err = UnlockResource(r3, test_event)
  if err == nil {
    t.Fatal("Unlocked r3 with event that didn't lock it")
  }

  err = UnlockResource(r3, root_event)
  if err != nil {
    t.Fatal("Failed to unlock r3")
  }
  SendUpdate(r3, NewDownSignal(r3, "unlocked"))
  (*GraphTester)(t).WaitForValue(r1_l, "unlocked", r3, time.Second * 2, "Wasn't notified of r1 unlock on r1 after r3 unlock")

  err = LockResource(r4, root_event)
  if err != nil {
    t.Fatal("Failed to lock r4 after unlocking r3")
  }
  SendUpdate(r4, NewDownSignal(r4, "locked"))
  (*GraphTester)(t).WaitForValue(r1_l, "locked", r4, time.Second * 2, "Wasn't notified of r1 lock on r1 after r4 lock")
  (*GraphTester)(t).WaitForValue(rel, "locked", r4, time.Second * 2, "Wasn't notified of r1 lock on r1 after r4 lock")

  err = UnlockResource(r4, root_event)
  if err != nil {
    t.Fatal("Failed to unlock r4")
  }
  SendUpdate(r4, NewDownSignal(r4, "unlocked"))
  (*GraphTester)(t).WaitForValue(r1_l, "unlocked", r4, time.Second * 2, "Wasn't notified of r1 unlock on r1 after r4 lock")
}

func TestAddToEventQueue(t * testing.T) {
  queue, _ := NewEventQueue("q", "", []Resource{})
  event_1, _ := NewEvent("1", "", []Resource{})
  event_2, _ := NewEvent("2", "", []Resource{})

  err := LinkEvent(queue, event_1, nil)
  if err == nil {
    t.Fatal("suceeded in added nil info to queue")
  }

  err = LinkEvent(queue, event_1, &EventQueueInfo{priority: 0})
  if err != nil {
    t.Fatal("failed to add valid event + info to queue")
  }

  err = LinkEvent(queue, event_2, &EventQueueInfo{priority: 1})
  if err != nil {
    t.Fatal("failed to add valid event + info to queue")
  }
}

func TestStartBaseEvent(t * testing.T) {
  event_1, _ := NewEvent("TestStartBaseEvent event_1", "", []Resource{})
  r := event_1.DoneResource()

  e_l := event_1.UpdateChannel()
  r_l := r.UpdateChannel()
  (*GraphTester)(t).CheckForNone(e_l, "Update on event_1 before starting")
  (*GraphTester)(t).CheckForNone(r_l, "Update on r_1 before starting")

  if r.Owner().ID() != event_1.ID() {
    t.Fatal("r is not owned by event_1")
  }

  err := LockResources(event_1)
  if err != nil {
    t.Fatal(err)
  }

  err = RunEvent(event_1)
  if err != nil {
    t.Fatal(err)
  }
  // Check that the update channels for the event and resource have updates
  (*GraphTester)(t).WaitForValue(e_l, "event_start", event_1, 1*time.Second, "No event_start on e_l")
  (*GraphTester)(t).WaitForValue(e_l, "event_done", event_1, 1*time.Second, "No event_start on e_l")
  (*GraphTester)(t).WaitForValue(r_l, "unlocked", event_1, 1*time.Second, "No unlocked on r_l")

  if r.Owner() != nil {
    t.Fatal("r still owned after event completed")
  }
}

func TestAbortEventQueue(t * testing.T) {
  r1, _ := NewResource("r1", "", []Resource{})
  root_event, _ := NewEventQueue("root_event", "", []Resource{})
  r := root_event.DoneResource()

  LockResource(r1, root_event)

  // Now that the event is constructed with a queue and 3 basic events
  // start the queue and check that all the events are executed
  go func() {
    time.Sleep(100 * time.Millisecond)
    abort_signal := NewDownSignal(nil, "abort")
    SendUpdate(root_event, abort_signal)
  }()

  err := LockResources(root_event)
  if err != nil {
    t.Fatal(err)
  }
  err = RunEvent(root_event)
  if err == nil {
    t.Fatal("root_event completed without error")
  }

  if r.Owner() == nil {
    t.Fatal("root event was finished after starting")
  }
}

func TestDelegateLock(t * testing.T) {
  Log.Init([]string{})
  test_resource, _ := NewResource("test_resource", "", []Resource{})
  root_event, _ := NewEventQueue("root_event", "", []Resource{test_resource})
  test_event, _ := NewEvent("test_event", "", []Resource{test_resource})
  err := LinkEvent(root_event, test_event, NewEventQueueInfo(1))
  if err != nil {
    t.Fatal(err)
  }

  err = LockResources(root_event)
  if err != nil {
    t.Fatal(err)
  }

  test_listener := test_event.UpdateChannel()

  go func() {
    (*GraphTester)(t).WaitForValue(test_listener, "event_done", test_event, 250 * time.Millisecond, "No event_done for test_event")
    abort_signal := NewDownSignal(nil, "cancel")
    SendUpdate(root_event, abort_signal)
  }()

  err = RunEvent(root_event)
  if err != nil {
    t.Fatal(err)
  }
}

func TestStartWithoutLocking(t * testing.T) {
  test_resource, _ := NewResource("test_resource", "", []Resource{})
  root_event, _ := NewEvent("root_event", "", []Resource{test_resource})

  err := RunEvent(root_event)
  if err == nil {
    t.Fatal("Event ran without error without locking resources")
  }
}

func TestStartEventQueue(t * testing.T) {
  Log.Init([]string{"event"})
  root_event, _ := NewEventQueue("root_event", "", []Resource{})
  r := root_event.DoneResource()
  rel := root_event.UpdateChannel();
  res_1, _ := NewResource("test_resource_1", "", []Resource{})
  res_2, _ := NewResource("test_resource_2", "", []Resource{})


  e1, _ := NewEvent("e1", "", []Resource{res_1, res_2})
  e1_l := e1.UpdateChannel()
  e1_r := e1.DoneResource()
  e1_info := NewEventQueueInfo(1)
  err := LinkEvent(root_event, e1, e1_info)
  if err != nil {
    t.Fatal("Failed to add e1 to root_event")
  }
  (*GraphTester)(t).WaitForValue(rel, "child_added", root_event, time.Second, "No update on root_event after adding e1")

  e2, _ := NewEvent("e2", "", []Resource{res_1})
  e2_l := e2.UpdateChannel()
  e2_r := e2.DoneResource()
  e2_info := NewEventQueueInfo(2)
  err = LinkEvent(root_event, e2, e2_info)
  if err != nil {
    t.Fatal("Failed to add e2 to root_event")
  }
  (*GraphTester)(t).WaitForValue(rel, "child_added", root_event, time.Second, "No update on root_event after adding e2")

  e3, _ := NewEvent("e3", "", []Resource{res_2})
  e3_l := e3.UpdateChannel()
  e3_r := e3.DoneResource()
  e3_info := NewEventQueueInfo(3)
  err = LinkEvent(root_event, e3, e3_info)
  if err != nil {
    t.Fatal("Failed to add e3 to root_event")
  }
  (*GraphTester)(t).WaitForValue(rel, "child_added", root_event, time.Second, "No update on root_event after adding e3")

  // Abort the event after 5 seconds just in case
  go func() {
    time.Sleep(5 * time.Second)
    if r.Owner() != nil {
      abort_signal := NewDownSignal(nil, "abort")
      SendUpdate(root_event, abort_signal)
    }
  }()

  // Now that a root_event is constructed with a queue and 3 basic events
  // start the queue and check that all the events are executed
  go func() {
    (*GraphTester)(t).WaitForValue(e1_l, "event_done", e1, time.Second, "No event_done for e3")
    (*GraphTester)(t).WaitForValue(e2_l, "event_done", e2, time.Second, "No event_done for e3")
    (*GraphTester)(t).WaitForValue(e3_l, "event_done", e3, time.Second, "No event_done for e3")
    signal := NewDownSignal(nil, "cancel")
    SendUpdate(root_event, signal)
  }()

  err = LockResources(root_event)
  if err != nil {
    t.Fatal(err)
  }

  err = RunEvent(root_event)
  if err != nil {
    t.Fatal(err)
  }

  if r.Owner() != nil {
    fmt.Printf("root_event.DoneResource(): %p", root_event.DoneResource())
    t.Fatal("root event was not finished after starting")
  }

  if e1_r.Owner() != nil {
    t.Fatal(fmt.Sprintf("e1 was not completed: %s", e1_r.Owner()))
  }

  if e2_r.Owner() != nil {
    t.Fatal(fmt.Sprintf("e2 was not completed"))
  }

  if e3_r.Owner() != nil {
    t.Fatal("e3 was not completed")
  }
}
