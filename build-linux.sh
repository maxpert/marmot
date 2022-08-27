#!/bin/sh

GOOS=linux GOARCH=amd64 go build -o marmot-linux-amd64 ./marmot.go
GOOS=linux GOARCH=386 go build -o marmot-linux-386 ./marmot.go