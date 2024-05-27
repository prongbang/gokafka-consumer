# build stage
FROM golang:1.22.2-alpine AS builder

ENV GO111MODULE=on

# install git.
RUN apk update && apk add --no-cache git

RUN mkdir -p /go/src/github.com/prongbang/gokafka-consumer
WORKDIR /go/src/github.com/prongbang/gokafka-consumer
COPY . .

# Using go mod with go 1.11
RUN go mod tidy

# With go ≥ 1.10
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o /go/bin/app cmd/app/main.go

# small image
FROM alpine:3.7

WORKDIR /app
COPY --from=builder /go/bin/app .

ENV TZ=Asia/Bangkok
RUN echo "Asia/Bangkok" > /etc/timezone

# run binary.
ENTRYPOINT ["/app/app", "-env", "production"]