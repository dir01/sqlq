build:
	go build ./...
.PHONY: build

test:
	go test -timeout 1m -race -v ./...
.PHONY: test

test-short:
	go test -timeout 10s -race -short -failfast -v ./...
.PHONY: test-short
