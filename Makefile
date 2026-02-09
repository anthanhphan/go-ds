SHELL := /bin/sh

# Variables
PKGS := $(shell go list ./... | grep -vE '/example')
REPORT_DIR := code_report
COVER_OUT := $(REPORT_DIR)/coverage.out
COVER_HTML := $(REPORT_DIR)/coverage.html
SECURITY_JSON := $(REPORT_DIR)/security-report.json
BIN_DIR := bin

# Tools
GOLANGCI_LINT ?= golangci-lint
GOSEC ?= gosec
GOVULNCHECK ?= govulncheck
STATICCHECK ?= staticcheck
INEFFASSIGN ?= ineffassign
MISSPELL ?= misspell
GOCYCLO ?= gocyclo

GO ?= go

COMPOSE_SIMPLE := docker-compose.simple.yaml
COMPOSE_COMPLETE := docker-compose.complete.yaml

.DEFAULT_GOAL := help

.PHONY: all help build test tidy clean \
	fmt imports vet lint staticcheck ineffassign misspell cyclo security_scan vul \
	test_race test_coverage bench \
	install_tools \
	up-simple down-simple logs-simple ps-simple restart-simple \
	up-complete down-complete logs-complete ps-complete restart-complete

all: tidy fmt imports lint vet staticcheck ineffassign misspell cyclo test_race test_coverage security_scan vul

help: ## Show this help
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ------------------------------
# Tool Installation
# ------------------------------

install_tools: ## Install Go quality tools
	@echo "Installing Go quality tools..."
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/gordonklaus/ineffassign@latest
	go install github.com/client9/misspell/cmd/misspell@latest
	go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	@echo "All tools installed successfully."

# ------------------------------
# Basic Checks & Formatting
# ------------------------------

fmt: ## Format Go code
	@echo "Running go fmt..."
	@$(GO) fmt ./...

imports: ## Organize imports
	@echo "Organizing imports..."
	@goimports -w .

tidy: ## Tidy go modules
	@echo "Running go mod tidy..."
	@$(GO) mod tidy
	@$(GO) mod verify

vet: ## Run go vet
	@echo "Running go vet..."
	@$(GO) vet ./...

lint: ## Run golangci-lint
	@echo "Running golangci-lint..."
	@$(GOLANGCI_LINT) run ./...

staticcheck: ## Run staticcheck
	@echo "Running staticcheck..."
	@$(STATICCHECK) -go 1.25.2 ./...

ineffassign: ## Check for ineffectual assignments
	@echo "Checking for ineffectual assignments..."
	@$(INEFFASSIGN) ./...

misspell: ## Check for spelling errors
	@echo "Checking for spelling errors..."
	@$(MISSPELL) ./...

cyclo: ## Check cyclomatic complexity
	@echo "Checking cyclomatic complexity..."
	@$(GOCYCLO) -over 35 -ignore "_test" .

# ------------------------------
# Tests & Coverage
# ------------------------------

test: ## Run unit tests
	@$(GO) test ./...

test_race: ## Run tests with race detector
	@echo "Running tests with race detector..."
	@$(GO) clean -testcache
	@$(GO) test -race -count=2 $(PKGS)

test_coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	@mkdir -p $(REPORT_DIR)
	@$(GO) clean -testcache
	@$(GO) test $(PKGS) -coverprofile=$(COVER_OUT)
	@$(GO) tool cover -html=$(COVER_OUT) -o $(COVER_HTML)
	@echo "Coverage report generated at: $(COVER_HTML)"

bench: ## Run benchmarks
	@echo "Running benchmarks and saving to report..."
	@mkdir -p $(REPORT_DIR)
	@$(GO) test -bench=. -benchmem -run=^$$ $(PKGS) | tee $(REPORT_DIR)/benchmark.txt
	@echo "Benchmark results saved at: $(REPORT_DIR)/benchmark.txt"

# ------------------------------
# Security & Vulnerability
# ------------------------------

security_scan: ## Run gosec security scan
	@echo "Running gosec security scan..."
	@mkdir -p $(REPORT_DIR)
	@$(GOSEC) -fmt json -out $(SECURITY_JSON) ./...
	@echo "Security report generated at: $(SECURITY_JSON)"

vul: ## Run govulncheck
	@echo "Running govulncheck..."
	@$(GOVULNCHECK) ./...

# ------------------------------
# Build
# ------------------------------

build: ## Build all binaries
	@mkdir -p $(BIN_DIR)
	@$(GO) build -o $(BIN_DIR)/api ./cmd/gateway
	@$(GO) build -o $(BIN_DIR)/storage ./cmd/storage

clean: ## Remove build artifacts
	@rm -rf $(BIN_DIR) $(REPORT_DIR)

# ------------------------------
# Docker Compose (Simple: 3 nodes, 1 shard)
# ------------------------------

up-simple: ## Start simple 3-node stack
	@docker compose -f $(COMPOSE_SIMPLE) up --build -d

down-simple: ## Stop simple stack
	@docker compose -f $(COMPOSE_SIMPLE) down

logs-simple: ## Tail logs for simple stack
	@docker compose -f $(COMPOSE_SIMPLE) logs -f

ps-simple: ## Show status of simple stack
	@docker compose -f $(COMPOSE_SIMPLE) ps

restart-simple: down-simple up-simple ## Restart simple stack

# ------------------------------
# Docker Compose (Complete: 6 nodes, multiple shards)
# ------------------------------

up-complete: ## Start complete 6-node stack
	@docker compose -f $(COMPOSE_COMPLETE) up --build -d

down-complete: ## Stop complete stack
	@docker compose -f $(COMPOSE_COMPLETE) down

logs-complete: ## Tail logs for complete stack
	@docker compose -f $(COMPOSE_COMPLETE) logs -f

ps-complete: ## Show status of complete stack
	@docker compose -f $(COMPOSE_COMPLETE) ps

restart-complete: down-complete up-complete ## Restart complete stack

# ------------------------------
# Mocks
# ------------------------------

mock: ## Generate mocks
	@echo "Generating mocks..."
	@$(GO) generate ./...
	@echo "Mocks generated."
