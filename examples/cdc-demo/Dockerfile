# Marmot with CDC Publisher
FROM golang:1.24-bookworm AS builder

RUN apt-get update && apt-get install -y gcc libsqlite3-dev

WORKDIR /build
COPY . .

ENV GOTOOLCHAIN=auto
RUN CGO_ENABLED=1 go build -tags "sqlite_preupdate_hook sqlite_fts5 sqlite_json sqlite_math_functions sqlite_foreign_keys sqlite_stat4 sqlite_vacuum_incr" -o marmot .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/marmot /app/marmot

RUN mkdir -p /data

EXPOSE 3306 8080

ENTRYPOINT ["/app/marmot"]
CMD ["-config", "/app/config.toml"]
