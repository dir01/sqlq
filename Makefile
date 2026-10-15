build:
	go build ./...
.PHONY: build

test:
	go test -timeout 3m -race -v ./...
.PHONY: test

# The linter has a module file of its own, so that its dependencies stay out of the library's.
lint:
	go tool -modfile=tools/go.mod golangci-lint run
.PHONY: lint

test-short:
	go test -timeout 1m -race -short -failfast -v ./...
.PHONY: test-short

test-chaos:
	$(MAKE) -C tests/chaos test
.PHONY: test-chaos

test-simulation-sqlite:
	$(MAKE) -C tests/chaos test-simulation-sqlite
.PHONY: test-simulation-sqlite

test-simulation-psql:
	$(MAKE) -C tests/chaos test-simulation-psql
.PHONY: test-simulation-psql

test-chaos-checks:
	$(MAKE) -C tests/chaos checks
.PHONY: test-chaos-checks

test-checks:
	go vet ./...
	GOEXPERIMENT=cgocheck2 go test -timeout 2m -short -race -gcflags=all=-d=checkptr=2 -count=1 ./...
	$(MAKE) test-chaos-checks
.PHONY: test-checks

chaos-vm-build:
	$(MAKE) -C tests/chaos vm-build
.PHONY: chaos-vm-build

chaos-vm:
	$(MAKE) -C tests/chaos vm-run
.PHONY: chaos-vm

start-jaeger:
	docker run -d --rm --name jaeger \
		-p 5778:5778 \
		-p 16686:16686 \
		-p 4317:4317 \
		-p 4318:4318 \
		-p 14250:14250 \
		-p 14268:14268 \
		-p 9411:9411 \
		jaegertracing/jaeger:2.0.0 \
		--set receivers.otlp.protocols.http.endpoint=0.0.0.0:4318 \
		--set receivers.otlp.protocols.grpc.endpoint=0.0.0.0:4317
.PHONY: start-jaeger
