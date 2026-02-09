FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go.mod and go.sum
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries
RUN go build -o /app/api ./cmd/gateway
RUN go build -o /app/storage ./cmd/storage

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/api /app/api
COPY --from=builder /app/storage /app/storage

# Create data directory
RUN mkdir -p /data

# Default command (can be overridden in docker-compose)
CMD ["./api"]
