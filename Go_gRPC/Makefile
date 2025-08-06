.PHONY: build test clean run docker-build docker-run proto deps lint format help

# Variables
BINARY_NAME=ad-event-processor
DOCKER_IMAGE=ad-event-processor:latest
PROTO_PATH=proto
GO_FILES=$(shell find . -name "*.go" -type f)

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# Dependencies
deps: ## Install Go dependencies
	go mod download
	go mod tidy

# Protocol Buffers
proto: ## Generate Go code from protobuf files
	@echo "Generating protobuf files..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_PATH)/*.proto

# Build
build: proto ## Build the application
	@echo "Building $(BINARY_NAME)..."
	go build -o bin/$(BINARY_NAME) cmd/server/main.go

build-client: proto ## Build the client application
	@echo "Building client..."
	go build -o bin/client cmd/client/main.go

# Test
test: ## Run tests
	go test -v ./...

test-coverage: ## Run tests with coverage
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Linting and formatting
lint: ## Run golangci-lint
	golangci-lint run

format: ## Format Go code
	go fmt ./...
	goimports -w .

# Run
run: build ## Run the server
	./bin/$(BINARY_NAME)

run-client: build-client ## Run the client
	./bin/client

# Docker
docker-build: ## Build Docker image
	docker build -t $(DOCKER_IMAGE) .

docker-run: docker-build ## Run application in Docker
	docker-compose up

docker-down: ## Stop Docker containers
	docker-compose down

# Development
dev: ## Run in development mode with air (live reload)
	air

# Clean
clean: ## Clean build artifacts
	@echo "Cleaning..."
	rm -rf bin/
	rm -f coverage.out coverage.html
	go clean

# Database setup
setup-db: ## Setup local databases (Redis & MongoDB)
	docker-compose up -d redis mongodb

# Generate mock files for testing
generate-mocks: ## Generate mock files
	go generate ./...

# Install development tools
install-tools: ## Install development tools
	go install github.com/cosmtrek/air@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# All-in-one development setup
setup: deps install-tools proto ## Setup development environment

# Release
release: clean test build ## Build release version
	@echo "Release built successfully"