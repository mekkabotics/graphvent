package graphvent

import (
  "crypto/ed25519"
  "testing"
  "time"
  "crypto/rand"
)

func TestEvent(t *testing.T) {
  ctx := logTestContext(t, []string{"event", "listener", "listener_debug"})
  err := RegisterExtension[TestEventExt](ctx, nil)
  fatalErr(t, err)


  event_public, event_private, err := ed25519.GenerateKey(rand.Reader)
  event_listener := NewListenerExt(100)
  event, err := NewNode(ctx, event_private, "Base", 100, nil, NewEventExt(KeyID(event_public), "Test Event"), &TestEventExt{time.Second}, event_listener)
  fatalErr(t, err)

  response, signals := testSend(t, ctx, NewEventControlSignal("ready?"), event, event)
  switch resp := response.(type) {
  case *SuccessSignal:
  case *ErrorSignal:
    t.Fatalf("Error response %+v", resp.Error)
  default:
    t.Fatalf("Unexpected response %+v", resp)
  }

  var state_signal *EventStateSignal = nil
  for _, signal := range(signals) {
    event_state, is_event_state := signal.(*EventStateSignal)
    if is_event_state == true && event_state.Source == event.ID && event_state.State == "ready" {
      state_signal = event_state
      break
    }
  }

  if state_signal == nil {
    state_signal, err = WaitForSignal(event_listener.Chan, 10*time.Millisecond, func(sig *EventStateSignal) bool {
      return sig.Source == event.ID && sig.State == "ready"
    })
    fatalErr(t, err)
  }

  response, signals = testSend(t, ctx, NewEventControlSignal("start"), event, event)
  switch resp := response.(type) {
  case *SuccessSignal:
  case *ErrorSignal:
    t.Fatalf("Error response %+v", resp.Error)
  default:
    t.Fatalf("Unexpected response %+v", resp)
  }

  state_signal = nil
  for _, signal := range(signals) {
    event_state, is_event_state := signal.(*EventStateSignal)
    if is_event_state == true && event_state.Source == event.ID && event_state.State == "running" {
      state_signal = event_state
      break
    }
  }

  if state_signal == nil {
    state_signal, err = WaitForSignal(event_listener.Chan, 10*time.Millisecond, func(sig *EventStateSignal) bool {
      return sig.Source == event.ID && sig.State == "running"
    })
    fatalErr(t, err)
  }

  _, err = WaitForSignal(event_listener.Chan, time.Second * 2, func(sig *EventStateSignal) bool {
    return sig.Source == event.ID && sig.State == "done"
  })
  fatalErr(t, err)

  response, signals = testSend(t, ctx, NewEventControlSignal("start"), event, event)
  switch resp := response.(type) {
  case *SuccessSignal:
    t.Fatalf("Success response starting finished TestEventExt")
  case *ErrorSignal:
  default:
    t.Fatalf("Unexpected response %+v", resp)
  }
}
