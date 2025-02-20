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
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/matching-engine .

# Create logs directory
RUN mkdir -p logs && chown -R appuser:appuser /app

# Use non-root user
USER appuser

# Expose the port the app runs on
EXPOSE 8080

# Command to run the application
CMD ["./matching-engine"] 