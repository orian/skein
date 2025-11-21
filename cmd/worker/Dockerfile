# Use a debian-based image for the builder stage for glibc compatibility
FROM golang:1.25-bookworm AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files and download dependencies
COPY ../../go.mod ../../go.sum ./
RUN go mod download

# Copy the source code
COPY ../../ ./

# Build the worker binary
RUN go build -o /app/worker ./cmd/worker

# ---

# Use a minimal debian-based image for the final stage for libc compatibility
FROM debian:bookworm-slim

# Set the working directory
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/worker .

# Command to run the application
CMD ["./worker"]
