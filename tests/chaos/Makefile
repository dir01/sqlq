.PHONY: test checks test-simulation-sqlite test-simulation-psql vm-build vm-run

SIMULATION_ARGS ?=
SIMULATION_OUTPUT ?= $(CURDIR)/simulation-artifacts
SIMULATION_TIMEOUT ?= 5m

test-simulation-sqlite:
	mkdir -p "$(SIMULATION_OUTPUT)"
	go -C ../.. test -timeout 3m -race -count=1 -v -run '^TestReadinessSQLite$$' .
	go test -timeout $(SIMULATION_TIMEOUT) -race -count=1 -v -artifacts -outputdir "$(SIMULATION_OUTPUT)" ./internal/chaos -run '^TestSimulationSQLite$$' -args $(SIMULATION_ARGS)

test-simulation-psql:
	mkdir -p "$(SIMULATION_OUTPUT)"
	go test -timeout $(SIMULATION_TIMEOUT) -race -count=1 -v -artifacts -outputdir "$(SIMULATION_OUTPUT)" ./internal/chaos -run '^TestSimulationPSQL$$' -args $(SIMULATION_ARGS)

test:
	go test -timeout 2m -short -race -count=1 ./...

checks:
	go vet ./...
	GOEXPERIMENT=cgocheck2 go test -timeout 2m -short -race -gcflags=all=-d=checkptr=2 -count=1 ./...

vm-build:
	sh vm/build.sh

vm-run:
	go run ./cmd/sqlq-chaos-vm
