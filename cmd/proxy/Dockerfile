# Use the official Golang image to build the application
FROM golang:1.25-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files and download dependencies
COPY ../../go.mod ../../go.sum ./
RUN go mod download

# Copy the source code
COPY ../../ ./

# Build the proxy binary
RUN go build -o /app/proxy ./cmd/proxy

# ---

# Use a minimal image for the final stage
FROM alpine:latest

# Install curl for healthchecks
RUN apk add --no-cache curl

# Set the working directory
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/proxy .

# Expose port 8080
EXPOSE 8080

# Command to run the application
CMD ["./proxy"]
