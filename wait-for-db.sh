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

# Maximum number of attempts
MAX_ATTEMPTS=30
ATTEMPT=0

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."

# Wait for PostgreSQL to be ready
until PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c '\q' > /dev/null 2>&1; do
  ATTEMPT=$((ATTEMPT+1))
  
  if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
    echo "PostgreSQL is not available after $MAX_ATTEMPTS attempts. Exiting."
    exit 1
  fi
  
  echo "PostgreSQL is unavailable - retrying in 5s (attempt $ATTEMPT/$MAX_ATTEMPTS)"
  sleep 5
done

echo "PostgreSQL is up and running!"

# Check if the specific database exists
ATTEMPT=0
echo "Checking if database '$DB_NAME' exists..."

until PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; do
  ATTEMPT=$((ATTEMPT+1))
  
  if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
    echo "Database $DB_NAME does not exist after $MAX_ATTEMPTS attempts. Attempting to create it..."
    
    # Try to create the database
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c "CREATE DATABASE $DB_NAME;" > /dev/null 2>&1
    
    # Check if creation was successful
    if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
      echo "Successfully created database $DB_NAME."
    else
      echo "Failed to create database $DB_NAME. Exiting."
      exit 1
    fi
    
    break
  fi
  
  echo "Database $DB_NAME does not exist - retrying in 5s (attempt $ATTEMPT/$MAX_ATTEMPTS)"
  sleep 5
done

echo "Database $DB_NAME is ready!"

# Initialize the database schema if needed
echo "Checking if tables exist in database..."
TABLE_COUNT=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" | xargs)

if [ "$TABLE_COUNT" -eq "0" ]; then
  echo "No tables found, initializing database schema..."
  PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f schema.sql
  echo "Schema initialized successfully!"
else
  echo "Tables already exist, skipping schema initialization."
fi

echo "Starting application..."

# Execute the command passed to the script
exec "$@" 