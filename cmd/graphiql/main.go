package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
	gv "github.com/mekkanized/graphvent"
)

func check(err error) {
  if err != nil {
    panic(err)
  }
}

func main() {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  check(err)

  ctx, err := gv.NewContext(db, gv.NewConsoleLogger([]string{"test", "signal"}))
  check(err)

  gql_ext, err := gv.NewGQLExt(ctx, ":8080", nil, nil)
  check(err)

  listener_ext := gv.NewListenerExt(1000)

  n1, err := gv.NewNode(ctx, nil, "Lockable", 1000, gv.NewLockableExt(nil))
  check(err)

  n2, err := gv.NewNode(ctx, nil, "Lockable", 1000, gv.NewLockableExt([]gv.NodeID{n1.ID}))
  check(err)

  _, err = gv.NewNode(ctx, nil, "Lockable", 1000, gql_ext, listener_ext, gv.NewLockableExt([]gv.NodeID{n2.ID}))
  check(err)

  for true {
    select {
    case message := <- listener_ext.Chan:
      fmt.Printf("Listener Message: %+v\n", message)
    }
  }
}
