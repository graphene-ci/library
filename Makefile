.DEFAULT_GOAL := help
BIN := $(CURDIR)/bin
export GOTOOLCHAIN := go1.26.5
export GOWORK := $(CURDIR)/go.work
GOLANGCI_LINT_VERSION := v2.12.2
MODULES := docker k8s file git host

.PHONY: configure test lint it-host help
configure: go.work $(BIN)/golangci-lint ## Prepare local SDK workspace and pinned tools

# Rebuilt when the module list changes, so an existing checkout picks up a
# new module.
go.work: Makefile
	rm -f go.work go.work.sum
	GOWORK=off go work init ../pipeline $(addprefix ./,$(MODULES))

$(BIN)/golangci-lint:
	mkdir -p $(BIN)
	GOWORK=off GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

test: go.work ## Test all library modules against the sibling SDK checkout
	go test $(addsuffix /...,$(addprefix ./,$(MODULES)))

lint: configure ## Lint library modules
	$(BIN)/golangci-lint run $(addsuffix /...,$(addprefix ./,$(MODULES)))

HOST_IT_IMAGE := jrei/systemd-ubuntu:24.04
HOST_IT_BIN := $(CURDIR)/bin/host-it.test

it-host: go.work ## Run the host integration tests as root in a disposable systemd container
	CGO_ENABLED=0 go test -c -tags integration -o $(HOST_IT_BIN) ./host/
	@set -eu; \
	cid=$$(docker run -d --rm --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
		--tmpfs /run --tmpfs /run/lock --security-opt label=disable $(HOST_IT_IMAGE)); \
	trap 'docker rm -f '"$$cid"' >/dev/null' EXIT; \
	until [ "$$(docker exec $$cid systemctl is-system-running 2>/dev/null || true)" = running ]; do sleep 1; done; \
	docker cp $(HOST_IT_BIN) $$cid:/host-it.test; \
	docker exec -e GRAPHENE_HOST_IT=1 $$cid /host-it.test -test.v -test.run TestReal

help: ## Show targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
