#!/bin/bash
# NEM Dashboard Development Launcher
# Opens backend and frontend in separate macOS Terminal windows

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

# Check if we're on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "Error: This script is designed for macOS."
    echo "On other platforms, run these commands in separate terminals:"
    echo "  Terminal 1: cd $BACKEND_DIR && python3 run.py"
    echo "  Terminal 2: cd $FRONTEND_DIR && npm start"
    exit 1
fi

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

# Create temporary launcher scripts
BACKEND_LAUNCHER=$(mktemp /tmp/nem-backend.XXXXXX.sh)
FRONTEND_LAUNCHER=$(mktemp /tmp/nem-frontend.XXXXXX.sh)

# Make them executable
chmod +x "$BACKEND_LAUNCHER" "$FRONTEND_LAUNCHER"

# Write backend launcher
cat > "$BACKEND_LAUNCHER" << EOF
#!/bin/bash
echo "NEM Dashboard Backend"
echo "==================="
echo ""
cd "$BACKEND_DIR"
python3 run.py
# Keep window open on error
read -p "Press Enter to close..."
EOF

# Write frontend launcher
cat > "$FRONTEND_LAUNCHER" << EOF
#!/bin/bash
echo "NEM Dashboard Frontend"
echo "====================="
echo ""
cd "$FRONTEND_DIR"
npm start
# Keep window open on error
read -p "Press Enter to close..."
EOF

echo "Starting backend server in new Terminal window..."
open -a Terminal "$BACKEND_LAUNCHER"

# Give backend a moment to start initializing
sleep 2

echo "Starting frontend server in new Terminal window..."
open -a Terminal "$FRONTEND_LAUNCHER"

echo ""
echo "Development servers starting..."
echo "  Backend:  http://localhost:8000 (health check: /health)"
echo "  Frontend: http://localhost:3000"
echo ""
echo "To stop: Close the Terminal windows or use Ctrl+C in each"
echo ""

# Clean up temp files after a delay (windows have already started)
(sleep 5 && rm -f "$BACKEND_LAUNCHER" "$FRONTEND_LAUNCHER") &
