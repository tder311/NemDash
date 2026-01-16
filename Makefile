# NEM Dashboard Makefile
# Cross-platform development commands

# Configuration
BACKEND_DIR := nem-dashboard-backend
FRONTEND_DIR := nem-dashboard-frontend
PYTHON := python3
NPM := npm

# Default target
.DEFAULT_GOAL := help

# Phony targets (not files)
.PHONY: help install install-backend install-frontend \
        run-backend run-frontend dev \
        check check-python check-node check-deps \
        setup-env build clean test \
        health import-generators

##@ General

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Setup

install: install-backend install-frontend ## Install all dependencies

install-backend: ## Install Python backend dependencies
	@echo "Installing backend dependencies..."
	cd $(BACKEND_DIR) && pip install -r requirements.txt

install-frontend: ## Install Node.js frontend dependencies
	@echo "Installing frontend dependencies..."
	cd $(FRONTEND_DIR) && $(NPM) install

setup-env: ## Create .env from .env.example if it doesn't exist
	@if [ ! -f $(BACKEND_DIR)/.env ]; then \
		echo "Creating $(BACKEND_DIR)/.env from .env.example..."; \
		cp $(BACKEND_DIR)/.env.example $(BACKEND_DIR)/.env; \
		echo ".env file created successfully"; \
	else \
		echo "$(BACKEND_DIR)/.env already exists"; \
	fi

##@ Development

run-backend: setup-env ## Start the backend server (blocking)
	@echo "Starting backend server..."
	cd $(BACKEND_DIR) && $(PYTHON) run.py

run-frontend: ## Start the frontend development server (blocking)
	@echo "Starting frontend server..."
	cd $(FRONTEND_DIR) && $(NPM) start

dev: ## Start both servers in separate macOS Terminal windows
	@./scripts/dev.sh

##@ Verification

check: check-python check-node ## Check all dependencies

check-python: ## Verify Python installation and version
	@echo "Checking Python..."
	@$(PYTHON) --version || (echo "Error: Python 3 not found" && exit 1)
	@$(PYTHON) -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)" || \
		(echo "Error: Python 3.8+ required" && exit 1)
	@echo "Python OK"

check-node: ## Verify Node.js and npm installation
	@echo "Checking Node.js..."
	@node --version || (echo "Error: Node.js not found" && exit 1)
	@$(NPM) --version || (echo "Error: npm not found" && exit 1)
	@echo "Node.js OK"

check-deps: check ## Check if dependencies are installed
	@echo "Checking backend dependencies..."
	@cd $(BACKEND_DIR) && $(PYTHON) -c "import fastapi, uvicorn, pandas" 2>/dev/null || \
		(echo "Backend dependencies missing. Run 'make install-backend'" && exit 1)
	@echo "Backend dependencies OK"
	@echo "Checking frontend dependencies..."
	@test -d $(FRONTEND_DIR)/node_modules || \
		(echo "Frontend dependencies missing. Run 'make install-frontend'" && exit 1)
	@echo "Frontend dependencies OK"

##@ Build & Test

build: ## Build frontend for production
	@echo "Building frontend for production..."
	cd $(FRONTEND_DIR) && $(NPM) run build

test: ## Run frontend tests
	@echo "Running frontend tests..."
	cd $(FRONTEND_DIR) && $(NPM) test -- --watchAll=false

##@ Cleanup

clean: ## Remove build artifacts and caches
	@echo "Cleaning build artifacts..."
	rm -rf $(FRONTEND_DIR)/build
	rm -rf $(FRONTEND_DIR)/node_modules/.cache
	find $(BACKEND_DIR) -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Clean complete"

##@ Database

import-generators: ## Import generator data from CSV
	@echo "Importing generator data..."
	cd $(BACKEND_DIR) && $(PYTHON) import_geninfo_csv.py

##@ Health Checks

health: ## Check if servers are running
	@echo "Checking backend health..."
	@curl -s http://localhost:8000/health > /dev/null && echo "Backend: OK" || echo "Backend: Not running"
	@echo "Checking frontend..."
	@curl -s http://localhost:3000 > /dev/null && echo "Frontend: OK" || echo "Frontend: Not running"
