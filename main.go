package main

import (
  "time"
  "runtime/pprof"
  "os"
  "os/signal"
  "syscall"
  "fmt"
)

func fake_team(org string, id string, names []string) (*Team, []*Member) {
  members := []*Member{}
  for _, name := range(names) {
    members = append(members, NewMember(name))
  }
  team := NewTeam(org, id, members)
  return team, members
}

func fake_data() (* EventManager) {
  resources := []Resource{}

  teams_div1 := []*Team{}
  t1,  m1  := fake_team("6659", "A", []string{"jimmy"})
  t2,  m2  := fake_team("6659", "B", []string{"timmy"})
  t3,  m3  := fake_team("6659", "C", []string{"grace"})
  t4,  m4  := fake_team("6659", "D", []string{"jeremy"})
  t5,  m5  := fake_team("210",  "W", []string{"bobby"})
  t6,  m6  := fake_team("210",  "X", []string{"toby"})
  t7,  m7  := fake_team("210",  "Y", []string{"jennifer"})
  t8,  m8  := fake_team("210",  "Z", []string{"emily"})
  teams_div2 := []*Team{}
  t9,  m9  := fake_team("666",  "A", []string{"jimmy"})
  t10, m10 := fake_team("666",  "B", []string{"timmy"})
  t11, m11 := fake_team("666",  "C", []string{"grace"})
  t12, m12 := fake_team("666",  "D", []string{"jeremy"})
  t13, m13 := fake_team("315",  "W", []string{"bobby"})
  t14, m14 := fake_team("315",  "X", []string{"toby"})
  t15, m15 := fake_team("315",  "Y", []string{"jennifer"})
  t16, m16 := fake_team("315",  "Z", []string{"emily"})

  teams_div1 = append(teams_div1, t1)
  teams_div1 = append(teams_div1, t2)
  teams_div1 = append(teams_div1, t3)
  teams_div1 = append(teams_div1, t4)
  teams_div1 = append(teams_div1, t5)
  teams_div1 = append(teams_div1, t6)
  teams_div1 = append(teams_div1, t7)
  teams_div1 = append(teams_div1, t8)
  teams_div2 = append(teams_div2, t9)
  teams_div2 = append(teams_div2, t10)
  teams_div2 = append(teams_div2, t11)
  teams_div2 = append(teams_div2, t12)
  teams_div2 = append(teams_div2, t13)
  teams_div2 = append(teams_div2, t14)
  teams_div2 = append(teams_div2, t15)
  teams_div2 = append(teams_div2, t16)

  resources = append(resources, m1[0])
  resources = append(resources, m2[0])
  resources = append(resources, m3[0])
  resources = append(resources, m4[0])
  resources = append(resources, m5[0])
  resources = append(resources, m6[0])
  resources = append(resources, m7[0])
  resources = append(resources, m8[0])
  resources = append(resources, m9[0])
  resources = append(resources, m10[0])
  resources = append(resources, m11[0])
  resources = append(resources, m12[0])
  resources = append(resources, m13[0])
  resources = append(resources, m14[0])
  resources = append(resources, m15[0])
  resources = append(resources, m16[0])

  arenas_div1 := []Arena{}
  arenas_div1 = append(arenas_div1, NewVirtualArena("Arena 1"))
  arenas_div1 = append(arenas_div1, NewVirtualArena("Arena 2"))
  arenas_div2 := []Arena{}
  arenas_div2 = append(arenas_div2, NewVirtualArena("Arena 3"))
  arenas_div2 = append(arenas_div2, NewVirtualArena("Arena 4"))

  for _, arena := range arenas_div1 {
    resources = append(resources, arena)
  }

  for _, arena := range arenas_div2 {
    resources = append(resources, arena)
  }

  for _, team := range teams_div1 {
    resources = append(resources, team)
  }

  for _, team := range teams_div2 {
    resources = append(resources, team)
  }

  alliances_div1 := []*Alliance{}
  alliances_div1 = append(alliances_div1, NewAlliance(t1, t2))
  alliances_div1 = append(alliances_div1, NewAlliance(t3, t4))
  alliances_div1 = append(alliances_div1, NewAlliance(t5, t6))
  alliances_div1 = append(alliances_div1, NewAlliance(t7, t8))
  alliances_div2 := []*Alliance{}
  alliances_div2 = append(alliances_div2, NewAlliance(t9,  t10))
  alliances_div2 = append(alliances_div2, NewAlliance(t11, t12))
  alliances_div2 = append(alliances_div2, NewAlliance(t13, t14))
  alliances_div2 = append(alliances_div2, NewAlliance(t15, t16))

  for _, alliance := range alliances_div1 {
    resources = append(resources, alliance)
  }

  for _, alliance := range alliances_div2 {
    resources = append(resources, alliance)
  }


  gql_server := NewGQLServer(":8080", GQLVexTypes(), GQLVexQueries(), GQLVexMutations(), GQLVexSubscriptions())
  resources = append(resources, gql_server)

  root_event := NewEventQueue("root_event", "", []Resource{gql_server})

  div_1 := NewEventQueue("Division 1", "", []Resource{})
  err := AddChild(root_event, div_1, NewEventQueueInfo(1))
  if err != nil {
    panic(fmt.Sprintf("Failed to add div_1: %s", err))
  }

  div_2 := NewEventQueue("Division 2", "", []Resource{})
  err = AddChild(root_event, div_2, NewEventQueueInfo(1))
  if err != nil {
    panic(fmt.Sprintf("Failed to add div_2: %s", err))
  }

  event_manager := NewEventManager(root_event, resources)

  for i, alliance := range(alliances_div1) {
    for j, alliance2 := range(alliances_div1) {
      if j != i {
        if alliance.Children()[0] == alliance2.Children()[0] || alliance.Children()[0] == alliance2.Children()[1] || alliance.Children()[1] == alliance2.Children()[0] || alliance.Children()[1] == alliance2.Children()[1] {
        } else {
          for arena_idx := 0; arena_idx < len(arenas_div1); arena_idx++ {
            match := NewMatch(alliance, alliance2, arenas_div1[arena_idx])
            log.Logf("test", "Adding %s", match.Name())
            err := event_manager.AddEvent(div_1, match, NewEventQueueInfo(i))
            if err != nil {
              log.Logf("test", "Error adding %s: %s", match.Name(), err)
            }
          }
        }
      }
    }
  }

  for i, alliance := range(alliances_div2) {
    for j, alliance2 := range(alliances_div2) {
      if j != i {
        if alliance.Children()[0] == alliance2.Children()[0] || alliance.Children()[0] == alliance2.Children()[1] || alliance.Children()[1] == alliance2.Children()[0] || alliance.Children()[1] == alliance2.Children()[1] {
        } else {
          for arena_idx := 0; arena_idx < len(arenas_div2); arena_idx++ {
            match := NewMatch(alliance, alliance2, arenas_div2[arena_idx])
            log.Logf("test", "Adding %s", match.Name())
            err := event_manager.AddEvent(div_2, match, NewEventQueueInfo(i))
            if err != nil {
              log.Logf("test", "Error adding %s: %s", match.Name(), err)
            }
          }
        }
      }
    }
  }

  return event_manager
}

func main() {
  event_manager := fake_data()

  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

  go func() {
    cpufile, err := os.OpenFile("graphvent.cpu", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
      panic("Failed to open cpu profile file")
    }
    memfile, err := os.OpenFile("graphvent.mem", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
      panic("Failed to open mem profile file")
    }

    pprof.StartCPUProfile(cpufile)
    time.Sleep(3000 * time.Millisecond)
    pprof.WriteHeapProfile(memfile)
  }()

  go func() {
    for true {
      select {
      case <-sigs:
        pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
        signal := NewSignal(nil, "abort")
        signal.description = event_manager.root_event.ID()
        SendUpdate(event_manager.root_event, signal)
        break
      }
    }
  }()

  log.Logf("test", "Starting event_manager")
  err := event_manager.Run()
  if err != nil {
    log.Logf("test", "Error running event_manager: %s", err)
  } else {
    log.Logf("test", "Finished event_manager")
  }
  pprof.StopCPUProfile()
}
