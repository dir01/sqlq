build:
	go build ./...
.PHONY: build

test:
	go test -timeout 3m -race -v ./...
.PHONY: test

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run
.PHONY: lint

test-short:
	go test -timeout 1m -race -short -failfast -v ./...
.PHONY: test-short

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

