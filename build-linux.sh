#!/bin/sh

# Build Marmot v2.0 for Linux (static binary)
# Requires musl cross-compiler: brew install FiloSottile/musl-cross/musl-cross

CC=x86_64-linux-musl-gcc \
CXX=x86_64-linux-musl-g++ \
GOARCH=amd64 GOOS=linux CGO_ENABLED=1 \
go build -tags sqlite_preupdate_hook -ldflags "-linkmode external -extldflags -static" -o dist/linux/amd64/marmot-v2
