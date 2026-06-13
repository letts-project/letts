.PHONY: all build dugdale letts test vet fmt check clean linux deb bump release version help

GO ?= go
BIN_DIR ?= bin

all: check build ## Run checks then build both binaries (default)

build: dugdale letts ## Build dugdale + letts into bin/

dugdale: ## Build the dugdale daemon into bin/
	$(GO) build -o $(BIN_DIR)/dugdale ./cmd/dugdale

letts: ## Build the letts CLI into bin/
	$(GO) build -o $(BIN_DIR)/letts ./cmd/letts

test: ## Run the test suite (race detector)
	$(GO) test -race ./...

vet: ## Run go vet
	$(GO) vet ./...

fmt: ## Format the tree with gofmt
	gofmt -l -w .

check: fmt vet test ## fmt + vet + test

clean: ## Remove built binaries (bin/)
	rm -rf $(BIN_DIR)

linux: ## Cross-compile linux/amd64 binaries into dist/ (no package)
	./scripts/build/build.sh

deb: ## Build linux binaries + package a .deb into dist/
	./scripts/build/package.sh

bump: ## Increment build number in VERSION, commit and tag it
	./scripts/build/bump.sh

release: ## Push current branch + version tag to origin (triggers CI release)
	./scripts/build/release.sh

version: ## Print the current version string (0.0.<N>)
	@./scripts/build/version.sh

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-9s\033[0m %s\n", $$1, $$2}'
