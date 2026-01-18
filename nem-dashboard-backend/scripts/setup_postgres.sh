#!/bin/bash
# Setup script for PostgreSQL development environment
#
# Usage:
#   ./scripts/setup_postgres.sh          # Start PostgreSQL and migrate data
#   ./scripts/setup_postgres.sh --reset  # Reset database and re-migrate

set -e

cd "$(dirname "$0")/.."

echo "=================================="
echo "NEM Dashboard PostgreSQL Setup"
echo "=================================="
echo

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH"
    echo "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check for docker-compose
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "ERROR: docker-compose is not available"
    exit 1
fi

# Use 'docker compose' if available, otherwise 'docker-compose'
if docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# Handle --reset flag
if [ "$1" == "--reset" ]; then
    echo "Resetting PostgreSQL database..."
    $COMPOSE_CMD down -v 2>/dev/null || true
fi

# Start PostgreSQL
echo "Starting PostgreSQL container..."
$COMPOSE_CMD up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if $COMPOSE_CMD exec -T postgres pg_isready -U postgres &> /dev/null; then
        echo "PostgreSQL is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: PostgreSQL did not become ready in time"
        exit 1
    fi
    sleep 1
done

echo

# Check if SQLite database exists
SQLITE_PATH="./data/nem_dispatch.db"
if [ -f "$SQLITE_PATH" ]; then
    echo "Found existing SQLite database: $SQLITE_PATH"
    echo

    # Check if PostgreSQL already has data
    PG_COUNT=$($COMPOSE_CMD exec -T postgres psql -U postgres -d nem_dashboard -t -c "SELECT COUNT(*) FROM dispatch_data" 2>/dev/null || echo "0")
    PG_COUNT=$(echo $PG_COUNT | tr -d ' ')

    if [ "$PG_COUNT" != "0" ] && [ "$1" != "--reset" ]; then
        echo "PostgreSQL already has data ($PG_COUNT dispatch records)"
        echo "Use --reset flag to re-migrate from SQLite"
    else
        echo "Running migration from SQLite to PostgreSQL..."
        echo
        python scripts/migrate_to_postgres.py
    fi
else
    echo "No existing SQLite database found at $SQLITE_PATH"
    echo "A fresh database will be created when you start the server."
fi

echo
echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo
echo "To use PostgreSQL, add to your .env file:"
echo "  DATABASE_URL=postgresql://postgres:localdev@localhost:5432/nem_dashboard"
echo
echo "Then start the backend:"
echo "  python run.py"
echo
