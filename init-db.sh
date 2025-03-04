#!/bin/bash
set -e

# Load environment variables from .env if present
source .env 2>/dev/null || true

# Set default values if env vars are not set
DB_HOST=${DB_HOST:-db}
DB_PORT=${DB_PORT:-5432}
DB_USER=${DB_USER:-user}
DB_PASSWORD=${DB_PASSWORD:-password}
DB_NAME=${DB_NAME:-matching_db}

echo "Initializing database schema for $DB_NAME..."

# Apply schema.sql to create tables
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f schema.sql

echo "Database schema initialized successfully!" 