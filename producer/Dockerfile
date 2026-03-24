# Stage 1: Build the Go binary
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./producer/main.go

# Stage 2: Create a minimal production image
FROM alpine:latest

RUN apk --no-cache add ca-certificates tini

WORKDIR /root/

COPY --from=builder /main .

ENV BROKER="localhost:9092"
ENV TOPIC="test-topic"
ENV PORT="8080"

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:${PORT}/readyz || exit 1

ENTRYPOINT ["/sbin/tini", "--"]

CMD ["./main"]
