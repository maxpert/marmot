FROM golang:1.20-alpine

RUN apk add --no-cache \
  git \
  gcc \
  g++ \
  musl-dev

#ENV CC=x86_64-linux-musl-gcc
#ENV CXX=x86_64-linux-musl-g++
ARG MACHINE_ID="nothing"
ARG UID=1000
ARG GID=1000
ARG GROUP_NAME=marmot
ARG USER_NAME=marmot

ENV GOARCH=amd64
ENV GOOS="linux"
ENV CGO_ENABLED=1

WORKDIR /src

RUN git clone --depth=1 https://github.com/maxpert/marmot
WORKDIR /src/marmot

RUN go build -ldflags \
  "-linkmode external -extldflags -static" \
  -o dist/linux/amd64/marmot && \
  cp dist/linux/amd64/marmot /usr/local/bin && \
  chmod +x /usr/local/bin/marmot

RUN echo $MACHINE_ID > /etc/machine-id

RUN addgroup -g $GID $GROUP_NAME && \
    adduser --shell /sbin/nologin --disabled-password \
    --no-create-home --uid $UID --ingroup $GROUP_NAME $USER_NAME

USER marmot
WORKDIR /app
VOLUME /app

CMD ["marmot"]