#!/bin/bash
# NEM Dashboard Development Launcher
# Runs the backend and frontend together in the current terminal (cmux-friendly):
# combined, prefixed logs in one pane; Ctrl+C stops both.

# Get the project root directory (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
BACKEND_DIR="$PROJECT_ROOT/nem-dashboard-backend"
FRONTEND_DIR="$PROJECT_ROOT/nem-dashboard-frontend"

echo "NEM Dashboard Development Environment"
echo "======================================"
echo "Project root: $PROJECT_ROOT"
echo ""

# Function to kill process on a port
kill_port() {
    local port=$1
    local pid=$(lsof -ti :$port 2>/dev/null)
    if [ -n "$pid" ]; then
        echo "Killing existing process on port $port (PID: $pid)..."
        kill $pid 2>/dev/null
        sleep 1
        # Force kill if still running
        if lsof -ti :$port > /dev/null 2>&1; then
            kill -9 $(lsof -ti :$port) 2>/dev/null
        fi
    fi
}

# Kill any existing instances
echo "Checking for existing instances..."
kill_port 8000  # Backend
kill_port 3000  # Frontend
echo ""

# Setup .env if needed
if [ ! -f "$BACKEND_DIR/.env" ]; then
    echo "Creating .env from .env.example..."
    cp "$BACKEND_DIR/.env.example" "$BACKEND_DIR/.env"
fi

# Check if Docker is available and start PostgreSQL
echo "Checking PostgreSQL..."
if command -v docker &> /dev/null; then
    # Check if PostgreSQL container is running
    if ! docker ps --format '{{.Names}}' | grep -q "nem_dashboard_postgres"; then
        echo "Starting PostgreSQL with docker-compose..."
        cd "$BACKEND_DIR"
        if docker compose version &> /dev/null 2>&1; then
            docker compose up -d
        else
            docker-compose up -d
        fi

        # Wait for PostgreSQL to be ready
        echo "Waiting for PostgreSQL to be ready..."
        for i in {1..30}; do
            if docker compose exec -T postgres pg_isready -U postgres &> /dev/null 2>&1 || \
               docker-compose exec -T postgres pg_isready -U postgres &> /dev/null 2>&1; then
                echo "PostgreSQL is ready!"
                break
            fi
            if [ $i -eq 30 ]; then
                echo "Warning: PostgreSQL may not be ready yet"
            fi
            sleep 1
        done
        cd "$PROJECT_ROOT"
    else
        echo "PostgreSQL is already running"
    fi
else
    echo "Warning: Docker not found. Make sure PostgreSQL is running manually."
    echo "The backend requires DATABASE_URL to be set to a running PostgreSQL instance."
fi
echo ""

# Run both servers in THIS terminal with combined, prefixed logs.
# Ctrl+C (or any exit) stops both via the process-group kill in the trap.
cleanup() {
    trap - INT TERM EXIT
    echo ""
    echo "Stopping servers..."
    kill 0 2>/dev/null   # signal the whole process group (covers npm/uvicorn children)
    kill_port 8000       # backstop: free the ports regardless of process-tree state
    kill_port 3000
}
trap cleanup INT TERM EXIT

echo "Starting backend + frontend in this terminal (Ctrl+C stops both)..."
echo "  Backend:  http://localhost:8000 (health check: /health)"
echo "  Frontend: http://localhost:3000"
echo ""

# Portable line prefixer (BSD sed has no -u; this works on macOS and Linux).
# Pipeline subshells inherit shell functions, so it's usable on both sides.
prefix() { while IFS= read -r line; do printf '%s%s\n' "$1" "$line"; done; }

# Backend (uvicorn via run.py). 'exec' so the subshell becomes the server
# process, keeping the tree tidy for the process-group kill.
( cd "$BACKEND_DIR" && exec python3 run.py ) 2>&1 | prefix "[backend]  " &

# Give the backend a moment before the frontend starts hitting it.
sleep 2

# Frontend (CRA). BROWSER=none stops it auto-opening a browser tab.
( cd "$FRONTEND_DIR" && BROWSER=none exec npm start ) 2>&1 | prefix "[frontend] " &

# Wait for either to exit; Ctrl+C triggers the trap and tears both down.
wait
