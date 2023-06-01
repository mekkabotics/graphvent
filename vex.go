package main

import (
  "fmt"
  "log"
  "time"
  "errors"
)

type Member struct {
  BaseResource
}

func NewMember(name string) * Member {
  member := &Member{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: "A Team Member",
        id: randid(),
        listeners: []chan string{},
      },
      parents: []Resource{},
      children: []Resource{},
    },
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
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: description,
        id: randid(),
        listeners: []chan string{},
      },
      parents: []Resource{},
      children: make([]Resource, len(members)),
    },
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
}

func NewAlliance(team0 * Team, team1 * Team) * Alliance {
  name := fmt.Sprintf("Alliance %s/%s", team0.Name(), team1.Name())
  description := ""

  resource := &Alliance{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: description,
        id: randid(),
        listeners: []chan string{},
      },
      parents: []Resource{},
      children: []Resource{team0, team1},
    },
  }
  return resource
}

type Match struct {
  BaseEvent
  state string
  control string
  control_start time.Time
}

type Arena struct {
  BaseResource
  connected bool
}

func NewVirtualArena(name string) * Arena {
  arena := &Arena{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: "A virtual vex arena",
        id: randid(),
        listeners: []chan string{},
      },
      parents: []Resource{},
      children: []Resource{},
    },
    connected: false,
  }

  return arena
}

func (arena * Arena) Lock(event Event) error {
  if arena.connected == false {
    log.Printf("ARENA NOT CONNECTED: %s", arena.Name())
    error_str := fmt.Sprintf("%s is not connected, cannot lock", arena.Name())
    return errors.New(error_str)
  }
  return arena.lock(event)
}

func (arena * Arena) Connect(abort chan error) bool {
  log.Printf("Connecting %s", arena.Name())
  go func(arena * Arena, abort chan error) {
    update_channel := arena.UpdateChannel()
    owner := arena.Owner()
    var owner_channel chan string = nil
    if owner != nil {
      owner_channel = owner.UpdateChannel()
    }
    arena.connected = true
    update_str := fmt.Sprintf("VIRTUAL_ARENA connected: %s", arena.Name())
    arena.Update(update_str)
    log.Printf("VIRTUAL_ARENA goroutine starting: %s", arena.Name())
    for true {
      select {
      case <- abort:
        log.Printf("Virtual arena %s aborting", arena.Name())
        break
      case update, ok := <- update_channel:
        if !ok {
          panic("own update_channel closed")
        }
        log.Printf("%s update: %s", arena.Name(), update)
        new_owner := arena.Owner()
        if new_owner != owner {
          log.Printf("NEW_OWNER for %s", arena.Name())
          if new_owner != nil {
            log.Printf("new: %s", new_owner.Name())
          } else {
            log.Printf("new: nil")
          }

          if owner != nil {
            log.Printf("old: %s", owner.Name())
          } else {
            log.Printf("old: nil")
          }

          owner = new_owner
          if owner != nil {
            owner_channel = owner.UpdateChannel()
          } else {
            owner_channel = nil
          }
        }
      case update, ok := <- owner_channel:
        if !ok {
          panic("owner update channel closed")
        }
        log.Printf("%s owner update: %s", arena.Name(), update)
        log.Printf("owner: %s", owner.Name())
      }
    }
  }(arena, abort)
  return true
}

const start_slack = 3000 * time.Millisecond

func NewMatch(alliance0 * Alliance, alliance1 * Alliance, arena * Arena) * Match {
  name := fmt.Sprintf("Match: %s vs. %s on %s", alliance0.Name(), alliance1.Name(), arena.Name())
  description := "A vex match"

  match := &Match{
    BaseEvent: NewBaseEvent(name, description, []Resource{alliance0, alliance1, arena}),
    state: "init",
    control: "init",
    control_start: time.UnixMilli(0),
  }
  match.LockDone()

  match.actions["start"] = func() (string, error) {
    log.Printf("STARTING_MATCH %s", match.Name())
    match.control = "none"
    match.state = "scheduled"
    return "wait", nil
  }

  match.actions["queue_autonomous"] = func() (string, error) {
    match.control = "none"
    match.state = "autonomous_queued"
    match.control_start = time.Now().Add(start_slack)
    return "wait", nil
  }

  match.actions["start_autonomous"] = func() (string, error) {
    match.control = "autonomous"
    match.state = "autonomous_running"
    return "wait", nil
  }

  return match
}
