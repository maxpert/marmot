#!/bin/sh

export CC=x86_64-linux-musl-gcc
export CXX=x86_64-linux-musl-g++

CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o build/marmot-linux-amd64 ./marmot.go
CGO_ENABLED=1 GOOS=linux GOARCH=386 go build -o build/marmot-linux-386 ./marmot.go