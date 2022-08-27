#!/bin/sh

GOOS=linux GOARCH=amd64 go build -o build/marmot-linux-amd64 ./marmot.go
GOOS=linux GOARCH=386 go build -o build/marmot-linux-386 ./marmot.go