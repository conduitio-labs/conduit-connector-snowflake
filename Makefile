GOLINT := golangci-lint

.PHONY: build test

build:
	go build -o conduit-connector-snowflake cmd/snowflake/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml
