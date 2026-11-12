# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
FROM golang:1.22-alpine AS builder

# gcc/musl-dev are required by mattn/go-sqlite3 (CGO).
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# The server links SQLite through CGO. The CLI tools are pure Go so they can be
# built with CGO disabled and start without the SQLite shared library.
RUN CGO_ENABLED=1 go build -trimpath -o /out/server ./cmd/server
RUN CGO_ENABLED=0 go build -trimpath -o /out/gateway ./cmd/gateway
RUN CGO_ENABLED=0 go build -trimpath -o /out/healthcheck ./cmd/healthcheck
RUN CGO_ENABLED=0 go build -trimpath -o /out/bench ./cmd/bench

# ---- runtime stage ---------------------------------------------------------
FROM alpine:3.19

# libgcc is needed by the CGO server binary. The sqlite CLI lets operators
# inspect /data/node.db from inside a node container without extra tooling.
RUN apk add --no-cache ca-certificates libgcc sqlite

WORKDIR /app

COPY --from=builder /out/ /app/

EXPOSE 50051 60051

ENTRYPOINT ["/app/server"]
