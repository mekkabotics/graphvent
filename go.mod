module github.com/mekkanized/graphvent

go 1.21.0

replace github.com/mekkanized/graphvent/signal v0.0.0 => ./signal

require (
	capnproto.org/go/capnp/v3 v3.0.0-alpha-29
	github.com/dgraph-io/badger/v3 v3.2103.5
	github.com/gobwas/ws v1.2.1
	github.com/google/uuid v1.3.0
	github.com/graphql-go/graphql v0.8.1
	github.com/mekkanized/graphvent/signal v0.0.0
	github.com/rs/zerolog v1.29.1
	golang.org/x/net v0.7.0
)

require (
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.opencensus.io v0.22.5 // indirect
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9 // indirect
	golang.org/x/sys v0.6.0 // indirect
	zenhack.net/go/util v0.0.0-20230414204917-531d38494cf5 // indirect
)
