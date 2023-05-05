.DEFAULT_GOAL := all

.PHONY: all fmt build test

all: fmt

fmt:
	(cargo +nightly fmt)

build:
	(cargo build)

test:
	(cargo test)
