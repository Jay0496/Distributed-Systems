FROM golang:1.22-alpine

WORKDIR /app

# Install build essentials for SQLite (cgo)
RUN apk add --no-cache gcc musl-dev protobuf-dev

# Copy dependency files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

EXPOSE 50051