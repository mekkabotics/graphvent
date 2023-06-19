package main

import (
  "fmt"
  "time"
  "errors"
)

type Member struct {
  BaseResource
}

func NewMember(name string) * Member {
  member := &Member{
    BaseResource: NewBaseResource(name, "A Team Member", []Resource{}),
  }

  return member
}

type Team struct {
  BaseResource
  Org string
  Team string
}

func (team * Team) Members() []*Member {
  ret := make([]*Member, len(team.children))
  for idx, member := range(team.children) {
    ret[idx] = member.(*Member)
  }

  return ret
}

func NewTeam(org string, team string, members []*Member) * Team {
  name := fmt.Sprintf("%s%s", org, team)
  description := fmt.Sprintf("Team %s", name)
  resource := &Team{
    BaseResource: NewBaseResource(name, description, make([]Resource, len(members))),
    Org: org,
    Team: team,
  }

  for idx, member := range(members) {
    resource.children[idx] = member
  }

  return resource
}

type Alliance struct {
  BaseResource
  teams []*Team
}

func NewAlliance(team0 * Team, team1 * Team) * Alliance {
  name := fmt.Sprintf("Alliance %s/%s", team0.Name(), team1.Name())
  description := ""

  resource := &Alliance{
    BaseResource: NewBaseResource(name, description, []Resource{team0, team1}),
    teams: []*Team{team0, team1},
  }
  return resource
}

type Arena interface {
  Resource
}

type VirtualArena struct {
  BaseResource
  connected bool
}

func NewVirtualArena(name string) * VirtualArena {
  arena := &VirtualArena{
    BaseResource: NewBaseResource(name, "A virtual vex arena", []Resource{}),
    connected: false,
  }

  return arena
}

func (arena * VirtualArena) lock(node GraphNode) error {
  if arena.connected == false {
    log.Logf("vex", "ARENA NOT CONNECTED: %s", arena.Name())
    error_str := fmt.Sprintf("%s is not connected, cannot lock", arena.Name())
    return errors.New(error_str)
  }
  return nil
}

func (arena * VirtualArena) update(signal GraphSignal) {
  arena.signal <- signal
  arena.BaseResource.update(signal)
}

func (arena * VirtualArena) Init(abort chan error) bool {
  log.Logf("vex", "Initializing %s", arena.Name())
  go func(arena * VirtualArena, abort chan error) {
    signal := NewSignal(arena, "resource_connected")
    arena.connected = true
    SendUpdate(arena, signal)
    log.Logf("vex", "VIRTUAL_ARENA goroutine starting: %s", arena.Name())
    for true {
      select {
      case <- abort:
        log.Logf("vex", "Virtual arena %s aborting", arena.Name())
        break
      case update := <- arena.signal:
        log.Logf("vex", "VIRTUAL_ARENA_ACTION: %s : %+v", arena.Name(), update)
      }
    }
  }(arena, abort)
  return true
}

type VexEvent struct {
  BaseEvent
}

func NewVexEvent(name string, description string) * VexEvent {
  event := &VexEvent{
    BaseEvent: NewBaseEvent(name, description, []Resource{}),
  }

  event.actions["wait"] = EventWait(event)
  event.handlers["abort"] = EventAbort(event)
  event.actions["start"] = func() (string, error) {
    log.Logf("vex", "STARTING_VEX_TOURNAMENT %s", event.Name())
    return "wait", nil
  }

  return event
}

const start_slack = 250 * time.Millisecond
const TEMP_AUTON_TIME = 5 * time.Second
const TEMP_DRIVE_TIME = 5 * time.Second

type Match struct {
  BaseEvent
  arena Arena
  state string
  control string
  control_start time.Time
  control_duration time.Duration
  alliances []*Alliance
}

func NewMatch(alliance0 * Alliance, alliance1 * Alliance, arena Arena) * Match {
  name := fmt.Sprintf("Match: %s vs. %s on %s", alliance0.Name(), alliance1.Name(), arena.Name())
  description := "A vex match"

  match := &Match{
    BaseEvent: NewBaseEvent(name, description, []Resource{alliance0, alliance1, arena}),
    state: "init",
    control: "init",
    control_start: time.UnixMilli(0),
    control_duration: 0,
    arena: arena,
    alliances: []*Alliance{alliance0, alliance1},
  }

  match.actions["wait"] = EventWait(match)
  match.handlers["abort"] = EventAbort(match)

  match.actions["start"] = func() (string, error) {
    log.Logf("vex", "STARTING_MATCH %s", match.Name())
    match.control = "none"
    match.state = "scheduled"
    match.control_start = time.Now()
    return "wait", nil
  }

  match.handlers["queue_autonomous"] = func(signal GraphSignal) (string, error) {
    if match.state != "scheduled" {
      log.Logf("vex", "BAD_STATE: %s: %s - %s", signal.Type(), match.state, match.Name())
      return "wait", nil
    }
    log.Logf("vex", "AUTONOMOUS_QUEUED: %s", match.Name())
    match.control = "none"
    match.state = "autonomous_queued"
    match.control_start = time.Now().Add(start_slack)
    new_signal := NewSignal(match, "autonomous_queued")
    go SendUpdate(match, new_signal)
    return "wait", nil
  }

  match.handlers["start_autonomous"] = func(signal GraphSignal) (string, error) {
    if match.state != "autonomous_queued" {
      log.Logf("vex", "BAD_STATE: %s: %s - %s", signal.Type(), match.state, match.Name())
      return "wait", nil
    }
    log.Logf("vex", "AUTONOMOUS_RUNNING: %s", match.Name())
    match.control = "program"
    match.state = "autonomous_running"
    match.control_start = time.Now() // TODO
    match.control_duration = TEMP_AUTON_TIME

    go SendUpdate(match, NewSignal(match, "autonomous_running"))

    end_time := match.control_start.Add(TEMP_AUTON_TIME)
    match.SetTimeout(end_time, "autonomous_done")
    log.Logf("vex", "AUTONOMOUS@%s: %s UNTIL %s", time.Now(), match.control_start, end_time)
    return "wait", nil
  }

  match.handlers["autonomous_done"] = func(signal GraphSignal) (string, error) {
    if match.state != "autonomous_running" {
      log.Logf("vex", "BAD_STATE: %s: %s - %s", signal.Type(), match.state, match.Name())
      return "wait", nil
    }
    log.Logf("vex", "AUTONOMOUS_DONE: %s", match.Name())
    match.control = "none"
    match.state = "autonomous_done"

    return "wait", nil
  }

  match.handlers["queue_driver"] = func(signal GraphSignal) (string, error) {
    if match.state != "autonomous_done"{
      log.Logf("vex", "BAD_STATE: %s: %s - %s", signal.Type(), match.state, match.Name())
      return "wait", nil
    }
    match.control = "none"
    match.state = "driver_queued"
    match.control_start = time.Now().Add(start_slack)
    new_signal := NewSignal(match, "driver_queued")
    go SendUpdate(match, new_signal)
    return "wait", nil
  }

  match.handlers["start_driver"] = func(signal GraphSignal) (string, error) {
    if match.state != "driver_queued" {
      log.Logf("vex", "BAD_STATE: %s: %s - %s", signal.Type(), match.state, match.Name())
      return "wait", nil
    }
    match.control = "driver"
    match.state = "driver_running"
    match.control_start = time.Now() // TODO
    match.control_duration = TEMP_DRIVE_TIME

    go SendUpdate(match, NewSignal(match, "driver_running"))

    end_time := match.control_start.Add(TEMP_DRIVE_TIME)
    match.SetTimeout(end_time, "driver_done")

    return "wait", nil
  }

  match.handlers["driver_done"] = func(signal GraphSignal) (string, error) {
    if match.state != "driver_running" {
      log.Logf("vex", "BAD_STATE: %s: %s - %s", signal.Type(), match.state, match.Name())
      return "wait", nil
    }
    match.control = "none"
    match.state = "driver_done"

    return "", nil
  }

  match.actions["driver_done"] = func() (string, error) {
    SendUpdate(match, NewSignal(match, "driver_done"))
    return "wait", nil
  }
  match.actions["autonomous_done"] = func() (string, error) {
    SendUpdate(match, NewSignal(match, "autonomous_done"))
    return "wait", nil
  }

  return match
}
