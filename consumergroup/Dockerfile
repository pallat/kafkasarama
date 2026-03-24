# Stage 1: Build the Go binary
FROM golang:1.22-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files from the root of the project
# (Build should be executed from the repository root)
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the consumergroup source code and its dependencies
COPY consumergroup/ ./consumergroup/

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./consumergroup/main.go

# Stage 2: Create a minimal production image
FROM alpine:latest

# Install tini and CA certificates
RUN apk --no-cache add ca-certificates tini

# Set the working directory
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /main .

# Default environment variables (override at runtime)
ENV BROKERS="localhost:9092"
ENV GROUP="consumer-group"
ENV TOPICS="test-topic"
ENV PORT="8080"

# Expose the health check port
EXPOSE 8080

# Health check using wget
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:${PORT}/readyz || exit 1


# Use tini as the init process to handle signals properly
ENTRYPOINT ["/sbin/tini", "--"]

# Run the binary
CMD ["./main"]

