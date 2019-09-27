FROM golang:1.12 AS builder
COPY . /go/src/github.com/src-d/ghsync
WORKDIR /go/src/github.com/src-d/ghsync
RUN make build

FROM alpine

RUN apk update && \
  apk add ca-certificates && \
  rm -rf /var/cache/apk/*

COPY --from=builder /go/src/github.com/src-d/ghsync/build/bin/ghsync /bin/ghsync

ENTRYPOINT ["/bin/ghsync"]
