VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-snowflake.version=${VERSION}'" -o conduit-connector-snowflake cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fmt
fmt: ## Format Go files using gofumpt and gci.
	gofumpt -l -w .
	gci write --skip-generated  .

.PHONY: generate
generate:
	go generate ./...

.PHONY: mockgen
mockgen:
	mockgen -package mock -source source/interface.go -destination source/mock/source.go
	mockgen -package mock -source source/iterator/interface.go -destination source/iterator/mock/iterator.go
	mockgen -package mock -source destination/writer/writer.go -destination destination/writer/mock/writer.go

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
