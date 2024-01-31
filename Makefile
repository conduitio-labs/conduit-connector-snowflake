GOLINT := golangci-lint
GOTEST_FLAGS := -v

.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-snowflake.version=${VERSION}'" -o conduit-connector-snowflake cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml

mockgen:
	mockgen -package mock -source source/interface.go -destination source/mock/source.go
	mockgen -package mock -source source/iterator/interface.go -destination source/iterator/mock/iterator.go
