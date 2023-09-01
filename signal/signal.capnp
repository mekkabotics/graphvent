using Go = import "/go.capnp";
@0x83a311a90429ef94;
$Go.package("signal");
$Go.import("github.com/mekkanized/graphvent/signal");

struct SignalHeader {
  direction @0 :UInt8;
  id @1 :Data;
  reqID @2 :Data;
}

struct ErrorSignal {
  header @0 :SignalHeader;
  error @1 :Text;
}

struct LinkSignal {
  header @0 :SignalHeader;
  action @1 :Text;
  id @2 :Data;
}

struct LockSignal {
  header @0 :SignalHeader;
  state @1 :Text;
}

struct ReadSignal {
  header @0 :SignalHeader;
  extensions @1 :List(Extension);
  struct Extension {
    type @0 :UInt64;
    fields @1 :List(Text);
  }
}
