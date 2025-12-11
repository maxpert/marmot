# Marmot v2.0 Docker Image
# Multi-stage build

FROM golang:1.24-bookworm AS builder

RUN apt-get update && apt-get install -y gcc libsqlite3-dev

WORKDIR /build
COPY . .

# Go 1.25.1 required - use GOTOOLCHAIN to fetch it
ENV GOTOOLCHAIN=auto
RUN CGO_ENABLED=1 go build -tags sqlite_preupdate_hook -o marmot-v2 .

# Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/marmot-v2 /app/marmot-v2

RUN mkdir -p /data

# 3307 = MySQL protocol, 8081 = gRPC
EXPOSE 3307 8081

ENTRYPOINT ["/app/marmot-v2"]
CMD ["-config", "/app/config.toml"]
