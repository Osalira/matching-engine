# Build stage
FROM golang:1.17-alpine AS builder

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o matching-engine .

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata postgresql-client bash dos2unix

# Create non-root user
RUN adduser -D -g '' appuser

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/matching-engine .

# Copy schema file for initialization
COPY schema.sql ./

# Create logs directory
RUN mkdir -p logs && chown -R appuser:appuser /app

# Use non-root user
USER appuser

# Expose the port the app runs on
EXPOSE 8080

# Command to run the application with integrated wait-for-db logic
CMD bash -c '\
    echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..." && \
    ATTEMPT=0 && MAX_ATTEMPTS=30 && \
    until PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c "\q" > /dev/null 2>&1; do \
      ATTEMPT=$((ATTEMPT+1)) && \
      if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then \
        echo "PostgreSQL is not available after $MAX_ATTEMPTS attempts. Exiting." && \
        exit 1; \
      fi && \
      echo "PostgreSQL is unavailable - retrying in 5s (attempt $ATTEMPT/$MAX_ATTEMPTS)" && \
      sleep 5; \
    done && \
    echo "PostgreSQL is up and running!" && \
    echo "Checking if database $DB_NAME exists..." && \
    ATTEMPT=0 && \
    until PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; do \
      ATTEMPT=$((ATTEMPT+1)) && \
      if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then \
        echo "Database $DB_NAME does not exist after $MAX_ATTEMPTS attempts. Attempting to create it..." && \
        PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c "CREATE DATABASE $DB_NAME;" > /dev/null 2>&1 && \
        if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then \
          echo "Successfully created database $DB_NAME."; \
        else \
          echo "Failed to create database $DB_NAME. Exiting." && \
          exit 1; \
        fi && \
        break; \
      fi && \
      echo "Database $DB_NAME does not exist - retrying in 5s (attempt $ATTEMPT/$MAX_ATTEMPTS)" && \
      sleep 5; \
    done && \
    echo "Database $DB_NAME is ready!" && \
    echo "Checking if tables exist in database..." && \
    TABLE_COUNT=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '\''public'\'';" | xargs) && \
    if [ "$TABLE_COUNT" -eq "0" ]; then \
      echo "No tables found, initializing database schema..." && \
      PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f schema.sql && \
      echo "Schema initialized successfully!"; \
    else \
      echo "Tables already exist, skipping schema initialization."; \
    fi && \
    echo "Starting application..." && \
    ./matching-engine' 