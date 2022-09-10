#!/bin/sh

docker run --rm -v "$PWD":/usr/src/myapp -w /usr/src/myapp -e CGO_ENABLED=1 -e GOARCH=amd64 golang:1.18 go build -v -o build/marmot-linux-amd64 marmot.go
