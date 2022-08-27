# Marmot
A distributed SQLite ephemeral cache replicator.  

## Running

Generate certificate:
```shell
openssl req -x509 -nodes -days 36500 -newkey rsa:2048 -subj "/CN=Marmot/C=US/L=San Fransisco" -addext "subjectAltName=DNS.1:localhost IP.1:127.0.0.1" -keyout server.key -out server.crt
```

Build
```shell
go build -o build/marmot ./marmot.go
```

Make sure you have 2 SQLite DBs with exact same schemas (ideally empty):

```shell
build/marmot -bind 0.0.0.0:8161 -peers 127.0.0.1:8160 -db-path /tmp/cache.db -verbose
build/marmot -bind 0.0.0.0:8160 -peers 127.0.0.1:8161 -db-path /tmp/cache-2.db -verbose
```

## Production status

Highly experimental and still WIP.