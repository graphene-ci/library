.DEFAULT_GOAL := help
BIN := $(CURDIR)/bin
export GOTOOLCHAIN := go1.26.5
export GOWORK := $(CURDIR)/go.work
GOLANGCI_LINT_VERSION := v2.12.2
MODULES := docker k8s file git

.PHONY: configure test lint help
configure: go.work $(BIN)/golangci-lint ## Prepare local SDK workspace and pinned tools

go.work:
	GOWORK=off go work init ../pipeline $(addprefix ./,$(MODULES))

$(BIN)/golangci-lint:
	mkdir -p $(BIN)
	GOWORK=off GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

test: go.work ## Test all library modules against the sibling SDK checkout
	go test $(addsuffix /...,$(addprefix ./,$(MODULES)))

lint: configure ## Lint library modules
	$(BIN)/golangci-lint run $(addsuffix /...,$(addprefix ./,$(MODULES)))

help: ## Show targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
