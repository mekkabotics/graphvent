.PHONY: schema test

test: schema
	clear && go test

schema: signal/signal.capnp.go

%.capnp.go: %.capnp
	capnp compile -I ./go-capnp/std -ogo $<
