# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
FROM golang:1.22-alpine AS builder

# gcc/musl-dev are required by mattn/go-sqlite3 (CGO).
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -trimpath -o /out/server .
RUN CGO_ENABLED=0 go build -trimpath -o /out/healthcheck ./cmd/healthcheck

# ---- runtime stage ---------------------------------------------------------
FROM alpine:3.19

RUN apk add --no-cache ca-certificates libgcc

WORKDIR /app

COPY --from=builder /out/server /app/server
COPY --from=builder /out/healthcheck /app/healthcheck

EXPOSE 50051 60051

ENTRYPOINT ["/app/server"]
